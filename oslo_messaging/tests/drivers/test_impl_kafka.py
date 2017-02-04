# Copyright (C) 2015 Cisco Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import kafka
import kafka.errors
from six.moves import mock
import testscenarios

import oslo_messaging
from oslo_messaging._drivers import common as common_driver
from oslo_messaging._drivers import impl_kafka as kafka_driver
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class TestKafkaDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestKafkaDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, kafka_driver.KafkaDriver)


class TestKafkaTransportURL(test_utils.BaseTestCase):

    scenarios = [
        ('none', dict(url=None,
                      expected=dict(hostaddrs=['localhost:9092']))),
        ('empty', dict(url='kafka:///',
                       expected=dict(hostaddrs=['localhost:9092']))),
        ('host', dict(url='kafka://127.0.0.1',
                      expected=dict(hostaddrs=['127.0.0.1:9092']))),
        ('port', dict(url='kafka://localhost:1234',
                      expected=dict(hostaddrs=['localhost:1234']))),
        ('two', dict(url='kafka://localhost:1234,localhost2:1234',
                     expected=dict(hostaddrs=['localhost:1234',
                                              'localhost2:1234']))),

    ]

    def setUp(self):
        super(TestKafkaTransportURL, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'

    def test_transport_url(self):
        transport = oslo_messaging.get_transport(self.conf, self.url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver

        conn = driver._get_connection(common_driver.PURPOSE_SEND)
        self.assertEqual(self.expected['hostaddrs'], conn.hostaddrs)


class TestKafkaDriver(test_utils.BaseTestCase):
    """Unit Test cases to test the kafka driver
    """

    def setUp(self):
        super(TestKafkaDriver, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

    def test_send(self):
        target = oslo_messaging.Target(topic="topic_test")
        self.assertRaises(NotImplementedError,
                          self.driver.send, target, {}, {})

    def test_send_notification(self):
        target = oslo_messaging.Target(topic="topic_test")

        with mock.patch("kafka.KafkaProducer") as fake_producer_class:
            fake_producer = fake_producer_class.return_value
            fake_producer.send.side_effect = kafka.errors.NoBrokersAvailable
            self.assertRaises(kafka.errors.NoBrokersAvailable,
                              self.driver.send_notification,
                              target, {}, {"payload": ["test_1"]},
                              None, retry=3)
            self.assertEqual(3, fake_producer.send.call_count)

    def test_listen(self):
        target = oslo_messaging.Target(topic="topic_test")
        self.assertRaises(NotImplementedError, self.driver.listen, target,
                          None, None)

    def test_listen_for_notifications(self):
        targets_and_priorities = [
            (oslo_messaging.Target(topic="topic_test_1"), "sample"),
        ]
        expected_topics = ["topic_test_1.sample"]
        with mock.patch("kafka.KafkaConsumer") as consumer:
            self.driver.listen_for_notifications(
                targets_and_priorities, "kafka_test", 1000, 10)
            consumer.assert_called_once_with(
                *expected_topics, group_id="kafka_test",
                bootstrap_servers=['localhost:9092'],
                max_partition_fetch_bytes=mock.ANY,
                selector=mock.ANY
            )

    def test_cleanup(self):
        listeners = [mock.MagicMock(), mock.MagicMock()]
        self.driver.listeners.extend(listeners)
        self.driver.cleanup()
        for listener in listeners:
            listener.close.assert_called_once_with()


class TestKafkaConnection(test_utils.BaseTestCase):

    def setUp(self):
        super(TestKafkaConnection, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

    def test_notify(self):
        conn = self.driver._get_connection(common_driver.PURPOSE_SEND)

        with mock.patch("kafka.KafkaProducer") as fake_producer_class:
            fake_producer = fake_producer_class.return_value
            conn.notify_send("fake_topic", {"fake_ctxt": "fake_param"},
                             {"fake_text": "fake_message_1"}, 10)
            self.assertEqual(1, len(fake_producer.send.mock_calls))
