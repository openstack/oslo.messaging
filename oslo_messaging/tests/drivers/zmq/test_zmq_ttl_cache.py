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

import time

from oslo_messaging._drivers.zmq_driver.server import zmq_ttl_cache
from oslo_messaging.tests import utils as test_utils


class TestZmqTTLCache(test_utils.BaseTestCase):

    def setUp(self):
        super(TestZmqTTLCache, self).setUp()

        def call_count_decorator(unbound_method):
            def wrapper(self, *args, **kwargs):
                wrapper.call_count += 1
                return unbound_method(self, *args, **kwargs)
            wrapper.call_count = 0
            return wrapper

        zmq_ttl_cache.TTLCache._update_cache = \
            call_count_decorator(zmq_ttl_cache.TTLCache._update_cache)

        self.cache = zmq_ttl_cache.TTLCache(ttl=1)

        self.addCleanup(lambda: self.cache.cleanup())

    def _test_add_get(self):
        self.cache.add('x', 'a')

        self.assertEqual(self.cache.get('x'), 'a')
        self.assertEqual(self.cache.get('x', 'b'), 'a')
        self.assertIsNone(self.cache.get('y'))
        self.assertEqual(self.cache.get('y', 'b'), 'b')

        time.sleep(1)

        self.assertIsNone(self.cache.get('x'))
        self.assertEqual(self.cache.get('x', 'b'), 'b')

    def test_add_get_with_executor(self):
        self._test_add_get()

    def test_add_get_without_executor(self):
        self.cache._executor.stop()
        self._test_add_get()

    def _test_in_operator(self):
        self.cache.add(1)

        self.assertIn(1, self.cache)

        time.sleep(0.5)

        self.cache.add(2)

        self.assertIn(1, self.cache)
        self.assertIn(2, self.cache)

        time.sleep(0.75)

        self.cache.add(3)

        self.assertNotIn(1, self.cache)
        self.assertIn(2, self.cache)
        self.assertIn(3, self.cache)

        time.sleep(0.5)

        self.assertNotIn(2, self.cache)
        self.assertIn(3, self.cache)

    def test_in_operator_with_executor(self):
        self._test_in_operator()

    def test_in_operator_without_executor(self):
        self.cache._executor.stop()
        self._test_in_operator()

    def _is_expired(self, key):
        with self.cache._lock:
            _, expiration_time = self.cache._cache[key]
            return self.cache._is_expired(expiration_time, time.time())

    def test_executor(self):
        self.cache.add(1)

        self.assertEqual([1], sorted(self.cache._cache.keys()))
        self.assertFalse(self._is_expired(1))

        time.sleep(0.75)

        self.assertEqual(1, self.cache._update_cache.call_count)

        self.cache.add(2)

        self.assertEqual([1, 2], sorted(self.cache._cache.keys()))
        self.assertFalse(self._is_expired(1))
        self.assertFalse(self._is_expired(2))

        time.sleep(0.75)

        self.assertEqual(2, self.cache._update_cache.call_count)

        self.cache.add(3)

        if 1 in self.cache:
            self.assertEqual([1, 2, 3], sorted(self.cache._cache.keys()))
            self.assertTrue(self._is_expired(1))
        else:
            self.assertEqual([2, 3], sorted(self.cache._cache.keys()))
        self.assertFalse(self._is_expired(2))
        self.assertFalse(self._is_expired(3))

        time.sleep(0.75)

        self.assertEqual(3, self.cache._update_cache.call_count)

        self.assertEqual([3], sorted(self.cache._cache.keys()))
        self.assertFalse(self._is_expired(3))
