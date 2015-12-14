
# Copyright 2013 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import collections
import threading

from oslo_log import log as logging
import six

from oslo_messaging._drivers import common

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Pool(object):

    """A thread-safe object pool.

    Modelled after the eventlet.pools.Pool interface, but designed to be safe
    when using native threads without the GIL.

    Resizing is not supported.
    """

    def __init__(self, max_size=4):
        super(Pool, self).__init__()

        self._max_size = max_size
        self._current_size = 0
        self._cond = threading.Condition()

        self._items = collections.deque()

    def put(self, item):
        """Return an item to the pool."""
        with self._cond:
            self._items.appendleft(item)
            self._cond.notify()

    def get(self):
        """Return an item from the pool, when one is available.

        This may cause the calling thread to block.
        """
        with self._cond:
            while True:
                try:
                    return self._items.popleft()
                except IndexError:
                    pass

                if self._current_size < self._max_size:
                    self._current_size += 1
                    break

                # FIXME(markmc): timeout needed to allow keyboard interrupt
                # http://bugs.python.org/issue8844
                self._cond.wait(timeout=1)

        # We've grabbed a slot and dropped the lock, now do the creation
        try:
            return self.create()
        except Exception:
            with self._cond:
                self._current_size -= 1
            raise

    def iter_free(self):
        """Iterate over free items."""
        with self._cond:
            while True:
                try:
                    yield self._items.popleft()
                except IndexError:
                    break

    @abc.abstractmethod
    def create(self):
        """Construct a new item."""


class ConnectionPool(Pool):
    """Class that implements a Pool of Connections."""
    def __init__(self, conf, rpc_conn_pool_size, url, connection_cls):
        self.connection_cls = connection_cls
        self.conf = conf
        self.url = url
        super(ConnectionPool, self).__init__(rpc_conn_pool_size)
        self.reply_proxy = None

    # TODO(comstud): Timeout connections not used in a while
    def create(self, purpose=None):
        if purpose is None:
            purpose = common.PURPOSE_SEND
        LOG.debug('Pool creating new connection')
        return self.connection_cls(self.conf, self.url, purpose)

    def empty(self):
        for item in self.iter_free():
            item.close()
