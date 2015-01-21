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

from oslo_utils import importutils
import testtools

from oslo_messaging.tests import utils as test_utils

# NOTE(jamespage) matchmaker tied directly to eventlet
# which is not yet py3 compatible - skip if import fails
matchmaker_ring = (
    importutils.try_import('oslo_messaging._drivers.matchmaker_ring'))


@testtools.skipIf(not matchmaker_ring, "matchmaker/eventlet unavailable")
class MatchmakerRingTest(test_utils.BaseTestCase):

    def setUp(self):
        super(MatchmakerRingTest, self).setUp()
        self.ring_data = {
            "conductor": ["controller1", "node1", "node2", "node3"],
            "scheduler": ["controller1", "node1", "node2", "node3"],
            "network": ["controller1", "node1", "node2", "node3"],
            "cert": ["controller1"],
            "console": ["controller1"],
            "consoleauth": ["controller1"]}
        self.matcher = matchmaker_ring.MatchMakerRing(self.ring_data)

    def test_direct(self):
        self.assertEqual(
            self.matcher.queues('cert.controller1'),
            [('cert.controller1', 'controller1')])
        self.assertEqual(
            self.matcher.queues('conductor.node1'),
            [('conductor.node1', 'node1')])

    def test_fanout(self):
        self.assertEqual(
            self.matcher.queues('fanout~conductor'),
            [('fanout~conductor.controller1', 'controller1'),
             ('fanout~conductor.node1', 'node1'),
             ('fanout~conductor.node2', 'node2'),
             ('fanout~conductor.node3', 'node3')])

    def test_bare_topic(self):
        # Round robins through the hosts on the topic
        self.assertEqual(
            self.matcher.queues('scheduler'),
            [('scheduler.controller1', 'controller1')])
        self.assertEqual(
            self.matcher.queues('scheduler'),
            [('scheduler.node1', 'node1')])
        self.assertEqual(
            self.matcher.queues('scheduler'),
            [('scheduler.node2', 'node2')])
        self.assertEqual(
            self.matcher.queues('scheduler'),
            [('scheduler.node3', 'node3')])
        # Cycles loop
        self.assertEqual(
            self.matcher.queues('scheduler'),
            [('scheduler.controller1', 'controller1')])
