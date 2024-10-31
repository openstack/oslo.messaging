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

import testscenarios
from unittest import mock

from confluent_kafka import KafkaException

import oslo_messaging
from oslo_messaging._drivers import impl_kafka as kafka_driver
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class TestKafkaDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super().setUp()
        self.messaging_conf.transport_url = 'kafka:/'

    def test_driver_load(self):
        transport = oslo_messaging.get_notification_transport(self.conf)
        self.assertIsInstance(transport._driver, kafka_driver.KafkaDriver)


class TestKafkaTransportURL(test_utils.BaseTestCase):

    scenarios = [
        ('port', dict(url='kafka://localhost:1234',
                      expected=dict(hostaddrs=['localhost:1234'],
                                    username=None,
                                    password=None,
                                    vhost=None))),
        ('vhost', dict(url='kafka://localhost:1234/my_host',
                       expected=dict(hostaddrs=['localhost:1234'],
                                     username=None,
                                     password=None,
                                     vhost='my_host'))),
        ('two', dict(url='kafka://localhost:1234,localhost2:1234',
                     expected=dict(hostaddrs=['localhost:1234',
                                              'localhost2:1234'],
                                   username=None,
                                   password=None,
                                   vhost=None))),
        ('user', dict(url='kafka://stack:stacksecret@localhost:9092/my_host',
                      expected=dict(hostaddrs=['localhost:9092'],
                                    username='stack',
                                    password='stacksecret',
                                    vhost='my_host'))),
        ('user2', dict(url='kafka://stack:stacksecret@localhost:9092,'
                       'stack2:stacksecret2@localhost:1234/my_host',
                       expected=dict(hostaddrs=['localhost:9092',
                                                'localhost:1234'],
                                     username='stack',
                                     password='stacksecret',
                                     vhost='my_host'))),
        ('ipv4', dict(url='kafka://127.0.0.1:1234',
                      expected=dict(hostaddrs=['127.0.0.1:1234'],
                                    username=None,
                                    password=None,
                                    vhost=None))),
        ('ipv6', dict(url='kafka://[::1]:1234',
                      expected=dict(hostaddrs=['[::1]:1234'],
                                    username=None,
                                    password=None,
                                    vhost=None))),
    ]

    def setUp(self):
        super().setUp()
        self.messaging_conf.transport_url = 'kafka:/'

    def test_transport_url(self):
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              self.url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver

        self.assertIsInstance(driver, kafka_driver.KafkaDriver)
        self.assertEqual(self.expected['hostaddrs'], driver.pconn.hostaddrs)
        self.assertEqual(self.expected['username'], driver.pconn.username)
        self.assertEqual(self.expected['password'], driver.pconn.password)
        self.assertEqual(self.expected['vhost'], driver.virtual_host)


class TestKafkaDriver(test_utils.BaseTestCase):
    """Unit Test cases to test the kafka driver
    """

    def setUp(self):
        super().setUp()
        self.messaging_conf.transport_url = 'kafka:/'
        transport = oslo_messaging.get_notification_transport(self.conf)
        self.driver = transport._driver

    def test_send(self):
        target = oslo_messaging.Target(topic="topic_test")
        self.assertRaises(NotImplementedError,
                          self.driver.send, target, {}, {})

    def test_send_notification(self):
        target = oslo_messaging.Target(topic="topic_test")

        with mock.patch("confluent_kafka.Producer") as producer:
            self.driver.send_notification(
                target, {}, {"payload": ["test_1"]},
                None, retry=3)
            producer.assert_called_once_with({
                'bootstrap.servers': '',
                'linger.ms': mock.ANY,
                'batch.num.messages': mock.ANY,
                'compression.codec': 'none',
                'security.protocol': 'PLAINTEXT',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': mock.ANY,
                'sasl.password': mock.ANY,
                'ssl.ca.location': '',
                'ssl.certificate.location': '',
                'ssl.key.location': '',
                'ssl.key.password': '',
            })

    def test_send_notification_retries_on_buffer_error(self):
        target = oslo_messaging.Target(topic="topic_test")

        with mock.patch("confluent_kafka.Producer") as producer:
            fake_producer = mock.MagicMock()
            fake_producer.produce = mock.Mock(
                side_effect=[BufferError, BufferError, None])
            producer.return_value = fake_producer

            self.driver.send_notification(
                target, {}, {"payload": ["test_1"]},
                None, retry=3)

            assert fake_producer.produce.call_count == 3

    def test_send_notification_stops_on_kafka_error(self):
        target = oslo_messaging.Target(topic="topic_test")

        with mock.patch("confluent_kafka.Producer") as producer:
            fake_producer = mock.MagicMock()
            fake_producer.produce = mock.Mock(
                side_effect=[KafkaException, None])
            producer.return_value = fake_producer

            self.driver.send_notification(
                target, {}, {"payload": ["test_1"]},
                None, retry=3)

            assert fake_producer.produce.call_count == 1

    def test_listen(self):
        target = oslo_messaging.Target(topic="topic_test")
        self.assertRaises(NotImplementedError, self.driver.listen, target,
                          None, None)

    def test_listen_for_notifications(self):
        targets_and_priorities = [
            (oslo_messaging.Target(topic="topic_test_1"), "sample"),
        ]
        with mock.patch("confluent_kafka.Consumer") as consumer:
            self.driver.listen_for_notifications(
                targets_and_priorities, "kafka_test", 1000, 10)
            consumer.assert_called_once_with({
                'bootstrap.servers': '',
                'enable.partition.eof': False,
                'group.id': 'kafka_test',
                'enable.auto.commit': mock.ANY,
                'max.partition.fetch.bytes': mock.ANY,
                'security.protocol': 'PLAINTEXT',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': mock.ANY,
                'sasl.password': mock.ANY,
                'ssl.ca.location': '',
                'ssl.certificate.location': '',
                'ssl.key.location': '',
                'ssl.key.password': '',
                'default.topic.config': {'auto.offset.reset': 'latest'}
            })

    def test_cleanup(self):
        listeners = [mock.MagicMock(), mock.MagicMock()]
        self.driver.listeners.extend(listeners)
        self.driver.cleanup()
        for listener in listeners:
            listener.close.assert_called_once_with()


class TestKafkaConnection(test_utils.BaseTestCase):

    def setUp(self):
        super().setUp()
        self.messaging_conf.transport_url = 'kafka:/'
        transport = oslo_messaging.get_notification_transport(self.conf)
        self.driver = transport._driver

    def test_notify(self):

        with mock.patch("confluent_kafka.Producer") as producer:
            self.driver.pconn.notify_send("fake_topic",
                                          {"fake_ctxt": "fake_param"},
                                          {"fake_text": "fake_message_1"},
                                          10)
            assert producer.call_count == 1
