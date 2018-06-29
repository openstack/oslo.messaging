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
import sys
import threading

from oslo_log import log as logging
from oslo_utils import timeutils
import six

from oslo_messaging._drivers import common

LOG = logging.getLogger(__name__)

# TODO(harlowja): remove this when we no longer have to support 2.7
if sys.version_info[0:2] < (3, 2):
    def wait_condition(cond):
        # FIXME(markmc): timeout needed to allow keyboard interrupt
        # http://bugs.python.org/issue8844
        cond.wait(timeout=1)
else:
    def wait_condition(cond):
        cond.wait()


@six.add_metaclass(abc.ABCMeta)
class Pool(object):
    """A thread-safe object pool.

    Modelled after the eventlet.pools.Pool interface, but designed to be safe
    when using native threads without the GIL.

    Resizing is not supported.

    """

    def __init__(self, max_size=4, min_size=2, ttl=1200, on_expire=None):
        super(Pool, self).__init__()
        self._min_size = min_size
        self._max_size = max_size
        self._item_ttl = ttl
        self._current_size = 0
        self._cond = threading.Condition()
        self._items = collections.deque()
        self._on_expire = on_expire

    def expire(self):
        """Remove expired items from left (the oldest item) to
        right (the newest item).
        """
        with self._cond:
            while len(self._items) > self._min_size:
                try:
                    ttl_watch, item = self._items.popleft()
                    if ttl_watch.expired():
                        self._on_expire and self._on_expire(item)
                        self._current_size -= 1
                    else:
                        self._items.appendleft((ttl_watch, item))
                        return
                except IndexError:
                    break

    def put(self, item):
        """Return an item to the pool."""
        with self._cond:
            ttl_watch = timeutils.StopWatch(duration=self._item_ttl)
            ttl_watch.start()
            self._items.append((ttl_watch, item))
            self._cond.notify()

    def get(self):
        """Return an item from the pool, when one is available.

        This may cause the calling thread to block.
        """
        with self._cond:
            while True:
                try:
                    ttl_watch, item = self._items.pop()
                    self.expire()
                    return item
                except IndexError:
                    pass

                if self._current_size < self._max_size:
                    self._current_size += 1
                    break

                wait_condition(self._cond)

        # We've grabbed a slot and dropped the lock, now do the creation
        try:
            return self.create()
        except Exception:
            with self._cond:
                self._current_size -= 1
            raise

    def iter_free(self):
        """Iterate over free items."""
        while True:
            try:
                _, item = self._items.pop()
                yield item
            except IndexError:
                return

    @abc.abstractmethod
    def create(self):
        """Construct a new item."""


class ConnectionPool(Pool):
    """Class that implements a Pool of Connections."""

    def __init__(self, conf, max_size, min_size, ttl, url, connection_cls):
        self.connection_cls = connection_cls
        self.conf = conf
        self.url = url
        super(ConnectionPool, self).__init__(max_size, min_size, ttl,
                                             self._on_expire)

    def _on_expire(self, connection):
        connection.close()
        LOG.debug("Idle connection has expired and been closed."
                  " Pool size: %d" % len(self._items))

    def create(self, purpose=common.PURPOSE_SEND):
        LOG.debug('Pool creating new connection')
        return self.connection_cls(self.conf, self.url, purpose)

    def empty(self):
        for item in self.iter_free():
            item.close()
