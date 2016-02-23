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

import testtools

import oslo_messaging
from oslo_messaging._drivers import common
from oslo_messaging._drivers.zmq_driver.matchmaker.base import DummyMatchMaker
from oslo_messaging._drivers.zmq_driver.matchmaker import matchmaker_redis
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests import utils as test_utils


zmq = zmq_async.import_zmq()


class TestZmqTransportUrl(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqTransportUrl, self).setUp()

    def setup_url(self, url):
        transport = oslo_messaging.get_transport(self.conf, url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        return driver, url

    def test_empty_url(self):
        driver, url = self.setup_url("zmq:///")
        self.assertIs(matchmaker_redis.RedisMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq', driver.matchmaker.url.transport)

    def test_error_name(self):
        self.assertRaises(common.RPCException, self.setup_url, "zmq+error:///")

    def test_dummy_url(self):
        driver, url = self.setup_url("zmq+dummy:///")
        self.assertIs(DummyMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+dummy', driver.matchmaker.url.transport)

    def test_redis_url(self):
        driver, url = self.setup_url("zmq+redis:///")
        self.assertIs(matchmaker_redis.RedisMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)

    def test_redis_url_no_creds(self):
        driver, url = self.setup_url("zmq+redis://host:65123/")
        self.assertIs(matchmaker_redis.RedisMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)
        self.assertEqual("host", driver.matchmaker.standalone_redis["host"])
        self.assertEqual(65123, driver.matchmaker.standalone_redis["port"])

    def test_redis_url_no_port(self):
        driver, url = self.setup_url("zmq+redis://:p12@host:65123/")
        self.assertIs(matchmaker_redis.RedisMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)
        self.assertEqual("host", driver.matchmaker.standalone_redis["host"])
        self.assertEqual(65123, driver.matchmaker.standalone_redis["port"])
        self.assertEqual("p12", driver.matchmaker.standalone_redis["password"])

    def test_sentinel_multiple_hosts_url(self):
        driver, url = self.setup_url(
            "zmq+redis://sentinel1:20001,sentinel2:20001,sentinel3:20001/")
        self.assertIs(matchmaker_redis.RedisMatchMaker,
                      driver.matchmaker.__class__)
        self.assertEqual('zmq+redis', driver.matchmaker.url.transport)
        self.assertEqual(3, len(driver.matchmaker.sentinel_hosts))
        expected = [("sentinel1", 20001), ("sentinel2", 20001),
                    ("sentinel3", 20001)]
        self.assertEqual(expected, driver.matchmaker.sentinel_hosts)
