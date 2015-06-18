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

redis = importutils.try_import('redis')
matchmaker_redis = (
    importutils.try_import('oslo.messaging._drivers.matchmaker_redis'))


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


@testtools.skipIf(not matchmaker_redis, "matchmaker/eventlet unavailable")
@testtools.skipIf(not redis_available(), "redis unavailable")
class RedisMatchMakerTest(test_utils.BaseTestCase):

    def setUp(self):
        super(RedisMatchMakerTest, self).setUp()
        self.ring_data = {
            "conductor": ["controller1", "node1", "node2", "node3"],
            "scheduler": ["controller1", "node1", "node2", "node3"],
            "network": ["controller1", "node1", "node2", "node3"],
            "cert": ["controller1"],
            "console": ["controller1"],
            "l3_agent.node1": ["node1"],
            "consoleauth": ["controller1"]}
        self.matcher = matchmaker_redis.MatchMakerRedis()
        self.populate()

    def tearDown(self):
        super(RedisMatchMakerTest, self).tearDown()
        c = redis.StrictRedis()
        c.flushdb()

    def populate(self):
        for k, hosts in self.ring_data.items():
            for h in hosts:
                self.matcher.register(k, h)

    def test_direct(self):
        self.assertEqual(
            self.matcher.queues('cert.controller1'),
            [('cert.controller1', 'controller1')])

    def test_register(self):
        self.matcher.register('cert', 'keymaster')
        self.assertEqual(
            sorted(self.matcher.redis.smembers('cert')),
            ['cert.controller1', 'cert.keymaster'])
        self.matcher.register('l3_agent.node1', 'node1')
        self.assertEqual(
            sorted(self.matcher.redis.smembers('l3_agent.node1')),
            ['l3_agent.node1.node1'])

    def test_unregister(self):
        self.matcher.unregister('conductor', 'controller1')
        self.assertEqual(
            sorted(self.matcher.redis.smembers('conductor')),
            ['conductor.node1', 'conductor.node2', 'conductor.node3'])
