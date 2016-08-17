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

import threading
import time

import six

from oslo_messaging._drivers.zmq_driver import zmq_async

zmq = zmq_async.import_zmq()


class TTLCache(object):

    def __init__(self, ttl=None):
        self._lock = threading.Lock()
        self._expiration_times = {}
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

    def add(self, item):
        with self._lock:
            self._expiration_times[item] = time.time() + self._ttl

    def discard(self, item):
        with self._lock:
            self._expiration_times.pop(item, None)

    def __contains__(self, item):
        with self._lock:
            expiration_time = self._expiration_times.get(item)
            if expiration_time is None:
                return False
            if self._is_expired(expiration_time, time.time()):
                self._expiration_times.pop(item)
                return False
            return True

    def _update_cache(self):
        with self._lock:
            current_time = time.time()
            self._expiration_times = \
                {item: expiration_time for
                 item, expiration_time in six.iteritems(self._expiration_times)
                 if not self._is_expired(expiration_time, current_time)}
        time.sleep(self._ttl)

    def cleanup(self):
        if self._executor:
            self._executor.stop()
