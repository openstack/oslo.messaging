#    Copyright 2016 Mirantis, Inc.
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

import logging
import threading
import time

from oslo_messaging._drivers.zmq_driver import zmq_async

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class TTLCache(object):

    _UNDEFINED = object()

    def __init__(self, ttl=None):
        self._lock = threading.Lock()
        self._cache = {}
        self._executor = None

        if not (ttl is None or isinstance(ttl, (int, float))):
            raise ValueError('ttl must be None or a number')

        # no (i.e. infinite) ttl
        if ttl is None or ttl <= 0:
            ttl = float('inf')
        else:
            self._executor = zmq_async.get_executor(self._update_cache)

        self._ttl = ttl

        if self._executor:
            self._executor.execute()

    @staticmethod
    def _is_expired(expiration_time, current_time):
        return expiration_time <= current_time

    def add(self, key, value=None):
        with self._lock:
            expiration_time = time.time() + self._ttl
            self._cache[key] = (value, expiration_time)

    def get(self, key, default=None):
        with self._lock:
            data = self._cache.get(key)
            if data is None:
                return default
            value, expiration_time = data
            if self._is_expired(expiration_time, time.time()):
                del self._cache[key]
                return default
            return value

    def __contains__(self, key):
        return self.get(key, self._UNDEFINED) is not self._UNDEFINED

    def _update_cache(self):
        with self._lock:
            current_time = time.time()
            old_size = len(self._cache)
            self._cache = \
                {key: (value, expiration_time) for
                 key, (value, expiration_time) in self._cache.items()
                 if not self._is_expired(expiration_time, current_time)}
            new_size = len(self._cache)
            LOG.debug('Updated cache: current size %(new_size)s '
                      '(%(size_difference)s records removed)',
                      {'new_size': new_size,
                       'size_difference': old_size - new_size})
        time.sleep(self._ttl)

    def cleanup(self):
        if self._executor:
            self._executor.stop()
