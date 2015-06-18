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

import testtools

from oslo_messaging.tests import utils as test_utils
from oslo_utils import importutils

# NOTE(jamespage) matchmaker tied directly to eventlet
# which is not yet py3 compatible - skip if import fails
matchmaker = (
    importutils.try_import('oslo.messaging._drivers.matchmaker'))


@testtools.skipIf(not matchmaker, "matchmaker/eventlet unavailable")
class MatchmakerTest(test_utils.BaseTestCase):

    def test_fanout_binding(self):
        matcher = matchmaker.MatchMakerBase()
        matcher.add_binding(
            matchmaker.FanoutBinding(), matchmaker.DirectExchange())
        self.assertEqual(matcher.queues('hello.world'), [])
        self.assertEqual(
            matcher.queues('fanout~fantasy.unicorn'),
            [('fanout~fantasy.unicorn', 'unicorn')])
        self.assertEqual(
            matcher.queues('fanout~fantasy.pony'),
            [('fanout~fantasy.pony', 'pony')])

    def test_topic_binding(self):
        matcher = matchmaker.MatchMakerBase()
        matcher.add_binding(
            matchmaker.TopicBinding(), matchmaker.StubExchange())
        self.assertEqual(
            matcher.queues('hello-world'), [('hello-world', None)])

    def test_direct_binding(self):
        matcher = matchmaker.MatchMakerBase()
        matcher.add_binding(
            matchmaker.DirectBinding(), matchmaker.StubExchange())
        self.assertEqual(
            matcher.queues('hello.server'), [('hello.server', None)])
        self.assertEqual(matcher.queues('hello-world'), [])

    def test_localhost_match(self):
        matcher = matchmaker.MatchMakerLocalhost()
        self.assertEqual(
            matcher.queues('hello.server'), [('hello.server', 'server')])

        # Gets remapped due to localhost exchange
        # all bindings default to first match.
        self.assertEqual(
            matcher.queues('fanout~testing.server'),
            [('fanout~testing.localhost', 'localhost')])

        self.assertEqual(
            matcher.queues('hello-world'),
            [('hello-world.localhost', 'localhost')])
