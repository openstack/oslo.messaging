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

from oslo_messaging.tests import utils as test_utils


load_tests = testscenarios.load_tests_apply_scenarios


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

        self.topic = "test_topic"
        self.host1 = b"test_host1"
        self.host2 = b"test_host2"

    def test_register(self):
        self.test_matcher.register(self.topic, self.host1)

        self.assertEqual(self.test_matcher.get_hosts(self.topic), [self.host1])
        self.assertEqual(self.test_matcher.get_single_host(self.topic),
                         self.host1)

    def test_register_two_hosts(self):
        self.test_matcher.register(self.topic, self.host1)
        self.test_matcher.register(self.topic, self.host2)

        self.assertEqual(self.test_matcher.get_hosts(self.topic),
                         [self.host1, self.host2])
        self.assertIn(self.test_matcher.get_single_host(self.topic),
                      [self.host1, self.host2])

    def test_register_two_same_hosts(self):
        self.test_matcher.register(self.topic, self.host1)
        self.test_matcher.register(self.topic, self.host1)

        self.assertEqual(self.test_matcher.get_hosts(self.topic), [self.host1])
        self.assertEqual(self.test_matcher.get_single_host(self.topic),
                         self.host1)

    def test_get_hosts_wrong_topic(self):
        self.assertEqual(self.test_matcher.get_hosts("no_such_topic"), [])

    def test_get_single_host_wrong_topic(self):
        self.assertEqual(self.test_matcher.get_single_host("no_such_topic"),
                         "localhost")
