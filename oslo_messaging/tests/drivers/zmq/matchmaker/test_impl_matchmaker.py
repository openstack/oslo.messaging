# Copyright 2014 Canonical, Ltd.
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

from stevedore import driver
import testscenarios
import testtools

import retrying

import oslo_messaging
from oslo_messaging.tests import utils as test_utils
from oslo_utils import importutils

redis = importutils.try_import('redis')


def redis_available():
    '''Helper to see if local redis server is running'''
    if not redis:
        return False
    try:
        c = redis.StrictRedis(socket_timeout=1)
        c.ping()
        return True
    except redis.exceptions.ConnectionError:
        return False


load_tests = testscenarios.load_tests_apply_scenarios


@testtools.skipIf(not redis_available(), "redis unavailable")
class TestImplMatchmaker(test_utils.BaseTestCase):

    scenarios = [
        ("dummy", {"rpc_zmq_matchmaker": "dummy"}),
        ("redis", {"rpc_zmq_matchmaker": "redis"}),
    ]

    def setUp(self):
        super(TestImplMatchmaker, self).setUp()

        self.test_matcher = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.rpc_zmq_matchmaker,
        ).driver(self.conf)

        if self.rpc_zmq_matchmaker == "redis":
            self.addCleanup(self.test_matcher._redis.flushdb)

        self.target = oslo_messaging.Target(topic="test_topic")
        self.host1 = b"test_host1"
        self.host2 = b"test_host2"

    def test_register(self):
        self.test_matcher.register(self.target, self.host1, "test")

        self.assertEqual(self.test_matcher.get_hosts(self.target, "test"),
                         [self.host1])

    def test_register_two_hosts(self):
        self.test_matcher.register(self.target, self.host1, "test")
        self.test_matcher.register(self.target, self.host2, "test")

        self.assertItemsEqual(self.test_matcher.get_hosts(self.target, "test"),
                              [self.host1, self.host2])

    def test_register_unsibscribe(self):
        self.test_matcher.register(self.target, self.host1, "test")
        self.test_matcher.register(self.target, self.host2, "test")

        self.test_matcher.unregister(self.target, self.host2, "test")

        self.assertItemsEqual(self.test_matcher.get_hosts(self.target, "test"),
                              [self.host1])

    def test_register_two_same_hosts(self):
        self.test_matcher.register(self.target, self.host1, "test")
        self.test_matcher.register(self.target, self.host1, "test")

        self.assertEqual(self.test_matcher.get_hosts(self.target, "test"),
                         [self.host1])

    def test_get_hosts_wrong_topic(self):
        target = oslo_messaging.Target(topic="no_such_topic")
        hosts = []
        try:
            hosts = self.test_matcher.get_hosts(target, "test")
        except retrying.RetryError:
            pass
        self.assertEqual(hosts, [])
