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

from oslo_messaging._drivers.zmq_driver.client import zmq_routing_table
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests import utils as test_utils


zmq = zmq_async.import_zmq()


class TestRoutingTable(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRoutingTable, self).setUp()

    def test_get_next_while_origin_changed(self):
        table = zmq_routing_table.RoutingTable(self.conf)
        table.register("topic1.server1", "1")
        table.register("topic1.server1", "2")
        table.register("topic1.server1", "3")

        rr_gen = table.get_hosts_round_robin("topic1.server1")

        result = []
        for i in range(3):
            result.append(next(rr_gen))

        self.assertEqual(3, len(result))
        self.assertIn("1", result)
        self.assertIn("2", result)
        self.assertIn("3", result)

        table.register("topic1.server1", "4")
        table.register("topic1.server1", "5")
        table.register("topic1.server1", "6")

        result = []
        for i in range(6):
            result.append(next(rr_gen))

        self.assertEqual(6, len(result))
        self.assertIn("1", result)
        self.assertIn("2", result)
        self.assertIn("3", result)
        self.assertIn("4", result)
        self.assertIn("5", result)
        self.assertIn("6", result)

    def test_no_targets(self):
        table = zmq_routing_table.RoutingTable(self.conf)
        rr_gen = table.get_hosts_round_robin("topic1.server1")

        result = []
        for t in rr_gen:
            result.append(t)
        self.assertEqual(0, len(result))

    def test_target_unchanged(self):
        table = zmq_routing_table.RoutingTable(self.conf)
        table.register("topic1.server1", "1")

        rr_gen = table.get_hosts_round_robin("topic1.server1")

        result = []
        for i in range(3):
            result.append(next(rr_gen))

        self.assertEqual(["1", "1", "1"], result)
