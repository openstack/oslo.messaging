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

from six.moves import mock
import testtools

import oslo_messaging
from oslo_messaging._drivers import common
from oslo_messaging._drivers.zmq_driver.matchmaker.zmq_matchmaker_base \
    import MatchmakerDummy
from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_redis
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests import utils as test_utils

zmq = zmq_async.import_zmq()

redis = zmq_matchmaker_redis.redis
sentinel = zmq_matchmaker_redis.redis_sentinel


class TestZmqTransportUrl(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqTransportUrl, self).setUp()

    def setup_url(self, url):
        transport = oslo_messaging.get_transport(self.conf, url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        return driver, url

    def mock_redis(self):
        if redis is None:
            self.skipTest("redis not available")
        else:
            redis_patcher = mock.patch.object(redis, 'StrictRedis')
            self.addCleanup(redis_patcher.stop)
            return redis_patcher.start()

    def mock_sentinel(self):
        if sentinel is None:
            self.skipTest("sentinel not available")
        else:
            sentinel_patcher = mock.patch.object(sentinel, 'Sentinel')
            self.addCleanup(sentinel_patcher.stop)
            return sentinel_patcher.start()

    def test_empty_url(self):
        self.mock_redis()
        driver, url = self.setup_url("zmq:///")
        self.assertIs(zmq_matchmaker_redis.MatchmakerRedis,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq', driver.matchmaker.url.transport)

    def test_error_url(self):
        self.assertRaises(common.RPCException, self.setup_url, "zmq+error:///")

    def test_dummy_url(self):
        driver, url = self.setup_url("zmq+dummy:///")
        self.assertIs(MatchmakerDummy,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+dummy', driver.matchmaker.url.transport)

    def test_redis_url(self):
        self.mock_redis()
        driver, url = self.setup_url("zmq+redis:///")
        self.assertIs(zmq_matchmaker_redis.MatchmakerRedis,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)

    def test_sentinel_url(self):
        self.mock_sentinel()
        driver, url = self.setup_url("zmq+sentinel:///")
        self.assertIs(zmq_matchmaker_redis.MatchmakerSentinel,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+sentinel', driver.matchmaker.url.transport)

    def test_host_with_credentials_url(self):
        self.mock_redis()
        driver, url = self.setup_url("zmq://:password@host:60000/")
        self.assertIs(zmq_matchmaker_redis.MatchmakerRedis,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq', driver.matchmaker.url.transport)
        self.assertEqual(
            [{"host": "host", "port": 60000, "password": "password"}],
            driver.matchmaker._redis_hosts
        )

    def test_redis_multiple_hosts_url(self):
        self.mock_redis()
        driver, url = self.setup_url(
            "zmq+redis://host1:60001,host2:60002,host3:60003/"
        )
        self.assertIs(zmq_matchmaker_redis.MatchmakerRedis,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)
        self.assertEqual(
            [{"host": "host1", "port": 60001, "password": None},
             {"host": "host2", "port": 60002, "password": None},
             {"host": "host3", "port": 60003, "password": None}],
            driver.matchmaker._redis_hosts
        )

    def test_sentinel_multiple_hosts_url(self):
        self.mock_sentinel()
        driver, url = self.setup_url(
            "zmq+sentinel://host1:20001,host2:20002,host3:20003/"
        )
        self.assertIs(zmq_matchmaker_redis.MatchmakerSentinel,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+sentinel', driver.matchmaker.url.transport)
        self.assertEqual(
            [("host1", 20001), ("host2", 20002), ("host3", 20003)],
            driver.matchmaker._sentinel_hosts
        )
