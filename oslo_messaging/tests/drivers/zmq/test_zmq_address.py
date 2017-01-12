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

import testscenarios
import testtools

import oslo_messaging
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging.tests import utils as test_utils


zmq = zmq_async.import_zmq()

load_tests = testscenarios.load_tests_apply_scenarios


class TestZmqAddress(test_utils.BaseTestCase):

    scenarios = [
        ('router', {'listener_type': zmq_names.socket_type_str(zmq.ROUTER)}),
        ('dealer', {'listener_type': zmq_names.socket_type_str(zmq.DEALER)})
    ]

    @testtools.skipIf(zmq is None, "zmq not available")
    def test_target_to_key_topic_only(self):
        target = oslo_messaging.Target(topic='topic')
        key = zmq_address.target_to_key(target, self.listener_type)
        self.assertEqual(self.listener_type + '/topic', key)

    @testtools.skipIf(zmq is None, "zmq not available")
    def test_target_to_key_topic_server_round_robin(self):
        target = oslo_messaging.Target(topic='topic', server='server')
        key = zmq_address.target_to_key(target, self.listener_type)
        self.assertEqual(self.listener_type + '/topic/server', key)

    @testtools.skipIf(zmq is None, "zmq not available")
    def test_target_to_key_topic_fanout(self):
        target = oslo_messaging.Target(topic='topic', fanout=True)
        key = zmq_address.target_to_key(target, self.listener_type)
        self.assertEqual(self.listener_type + '/topic', key)

    @testtools.skipIf(zmq is None, "zmq not available")
    def test_target_to_key_topic_server_fanout(self):
        target = oslo_messaging.Target(topic='topic', server='server',
                                       fanout=True)
        key = zmq_address.target_to_key(target, self.listener_type)
        self.assertEqual(self.listener_type + '/topic', key)

    @testtools.skipIf(zmq is None, "zmq not available")
    def test_target_to_key_topic_server_fanout_no_prefix(self):
        target = oslo_messaging.Target(topic='topic', server='server',
                                       fanout=True)
        key = zmq_address.target_to_key(target)
        self.assertEqual('topic', key)
