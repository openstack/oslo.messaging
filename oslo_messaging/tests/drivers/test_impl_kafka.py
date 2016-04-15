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
from kafka.common import KafkaError
import mock
from oslo_serialization import jsonutils
import testscenarios
from testtools.testcase import unittest
import time

import oslo_messaging
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers import impl_kafka as kafka_driver
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios

KAFKA_BROKER = 'localhost:9092'
KAFKA_BROKER_URL = 'kafka://localhost:9092'


def _is_kafka_service_running():
    """Checks whether the Kafka service is running or not"""
    kafka_running = True
    try:
        broker = KAFKA_BROKER
        kafka.KafkaClient(broker)
    except KafkaError:
        # Kafka service is not running.
        kafka_running = False
    return kafka_running


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
                      expected=[dict(host='localhost', port=9092)])),
        ('empty', dict(url='kafka:///',
                       expected=[dict(host='localhost', port=9092)])),
        ('host', dict(url='kafka://127.0.0.1',
                      expected=[dict(host='127.0.0.1', port=9092)])),
        ('port', dict(url='kafka://localhost:1234',
                      expected=[dict(host='localhost', port=1234)])),
    ]

    def setUp(self):
        super(TestKafkaTransportURL, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'

    def test_transport_url(self):
        transport = oslo_messaging.get_transport(self.conf, self.url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver

        conn = driver._get_connection(kafka_driver.PURPOSE_SEND)
        self.assertEqual(self.expected[0]['host'], conn.host)
        self.assertEqual(self.expected[0]['port'], conn.port)


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

        with mock.patch.object(
                kafka_driver.Connection, 'notify_send') as fake_send:
            self.driver.send_notification(target, {}, {}, None)
            self.assertEqual(1, len(fake_send.mock_calls))

    def test_listen(self):
        target = oslo_messaging.Target(topic="topic_test")
        self.assertRaises(NotImplementedError, self.driver.listen, target)


class TestKafkaConnection(test_utils.BaseTestCase):

    def setUp(self):
        super(TestKafkaConnection, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_send')
    def test_notify(self, fake_send, fake_ensure_connection):
        conn = self.driver._get_connection(kafka_driver.PURPOSE_SEND)
        conn.notify_send("fake_topic", {"fake_ctxt": "fake_param"},
                         {"fake_text": "fake_message_1"}, 10)
        self.assertEqual(1, len(fake_send.mock_calls))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_send')
    def test_notify_with_retry(self, fake_send, fake_ensure_connection):
        conn = self.driver._get_connection(kafka_driver.PURPOSE_SEND)
        fake_send.side_effect = KafkaError("fake_exception")
        conn.notify_send("fake_topic", {"fake_ctxt": "fake_param"},
                         {"fake_text": "fake_message_2"}, 10)
        self.assertEqual(10, len(fake_send.mock_calls))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_parse_url')
    def test_consume(self, fake_parse_url, fake_ensure_connection):
        fake_message = {
            "context": {"fake": "fake_context_1"},
            "message": {"fake": "fake_message_1"}}

        conn = kafka_driver.Connection(
            self.conf, '', kafka_driver.PURPOSE_LISTEN)

        conn.consumer = mock.MagicMock()
        conn.consumer.fetch_messages = mock.MagicMock(
            return_value=iter([jsonutils.dumps(fake_message)]))

        self.assertEqual(fake_message, jsonutils.loads(conn.consume()[0]))
        self.assertEqual(1, len(conn.consumer.fetch_messages.mock_calls))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_parse_url')
    def test_consume_timeout(self, fake_parse_url, fake_ensure_connection):
        deadline = time.time() + 3
        conn = kafka_driver.Connection(
            self.conf, '', kafka_driver.PURPOSE_LISTEN)

        conn.consumer = mock.MagicMock()
        conn.consumer.fetch_messages = mock.MagicMock(return_value=iter([]))

        self.assertRaises(driver_common.Timeout, conn.consume, timeout=3)
        self.assertEqual(0, int(deadline - time.time()))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_parse_url')
    def test_consume_with_default_timeout(
            self, fake_parse_url, fake_ensure_connection):
        deadline = time.time() + 1
        conn = kafka_driver.Connection(
            self.conf, '', kafka_driver.PURPOSE_LISTEN)

        conn.consumer = mock.MagicMock()
        conn.consumer.fetch_messages = mock.MagicMock(return_value=iter([]))

        self.assertRaises(driver_common.Timeout, conn.consume)
        self.assertEqual(0, int(deadline - time.time()))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, '_parse_url')
    def test_consume_timeout_without_consumers(
            self, fake_parse_url, fake_ensure_connection):
        deadline = time.time() + 3
        conn = kafka_driver.Connection(
            self.conf, '', kafka_driver.PURPOSE_LISTEN)
        conn.consumer = mock.MagicMock(return_value=None)

        self.assertRaises(driver_common.Timeout, conn.consume, timeout=3)
        self.assertEqual(0, int(deadline - time.time()))


class TestKafkaListener(test_utils.BaseTestCase):

    def setUp(self):
        super(TestKafkaListener, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, 'declare_topic_consumer')
    def test_create_listener(self, fake_consumer, fake_ensure_connection):
        fake_target = oslo_messaging.Target(topic='fake_topic')
        fake_targets_and_priorities = [(fake_target, 'info')]
        self.driver.listen_for_notifications(fake_targets_and_priorities, None,
                                             None, None)
        self.assertEqual(1, len(fake_consumer.mock_calls))

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, 'declare_topic_consumer')
    def test_converting_targets_to_topics(self, fake_consumer,
                                          fake_ensure_connection):
        fake_targets_and_priorities = [
            (oslo_messaging.Target(topic="fake_topic",
                                   exchange="test1"), 'info'),
            (oslo_messaging.Target(topic="fake_topic",
                                   exchange="test2"), 'info'),
            (oslo_messaging.Target(topic="fake_topic",
                                   exchange="test1"), 'error'),
            (oslo_messaging.Target(topic="fake_topic",
                                   exchange="test3"), 'error'),
        ]
        self.driver.listen_for_notifications(fake_targets_and_priorities, None,
                                             None, None)
        self.assertEqual(1, len(fake_consumer.mock_calls))
        fake_consumer.assert_called_once_with(set(['fake_topic.error',
                                                   'fake_topic.info']),
                                              None)

    @mock.patch.object(kafka_driver.Connection, '_ensure_connection')
    @mock.patch.object(kafka_driver.Connection, 'declare_topic_consumer')
    def test_stop_listener(self, fake_consumer, fake_client):
        fake_target = oslo_messaging.Target(topic='fake_topic')
        fake_targets_and_priorities = [(fake_target, 'info')]
        listener = self.driver.listen_for_notifications(
            fake_targets_and_priorities, None, None, None)._poll_style_listener
        listener.conn.consume = mock.MagicMock()
        listener.conn.consume.return_value = (
            iter([kafka.common.KafkaMessage(
                topic='fake_topic', partition=0, offset=0,
                key=None, value='{"message": {"fake": "fake_message_1"},'
                                '"context": {"fake": "fake_context_1"}}')]))
        listener.poll()
        self.assertEqual(1, len(listener.conn.consume.mock_calls))
        listener.conn.stop_consuming = mock.MagicMock()
        listener.stop()
        fake_response = listener.poll()
        self.assertEqual(1, len(listener.conn.consume.mock_calls))
        self.assertEqual([], fake_response)


class TestWithRealKafkaBroker(test_utils.BaseTestCase):

    def setUp(self):
        super(TestWithRealKafkaBroker, self).setUp()
        self.messaging_conf.transport_driver = 'kafka'
        transport = oslo_messaging.get_transport(self.conf, KAFKA_BROKER_URL)
        self.driver = transport._driver

    @unittest.skipUnless(
        _is_kafka_service_running(), "Kafka service is not available")
    def test_send_and_receive_message(self):
        target = oslo_messaging.Target(
            topic="fake_topic", exchange='fake_exchange')
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener
        fake_context = {"fake_context_key": "fake_context_value"}
        fake_message = {"fake_message_key": "fake_message_value"}
        self.driver.send_notification(
            target, fake_context, fake_message, None)

        received_message = listener.poll()[0]
        self.assertEqual(fake_context, received_message.ctxt)
        self.assertEqual(fake_message, received_message.message)

    @unittest.skipUnless(
        _is_kafka_service_running(), "Kafka service is not available")
    def test_send_and_receive_message_without_exchange(self):
        target = oslo_messaging.Target(topic="fake_no_exchange_topic")
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener
        fake_context = {"fake_context_key": "fake_context_value"}
        fake_message = {"fake_message_key": "fake_message_value"}
        self.driver.send_notification(
            target, fake_context, fake_message, None)

        received_message = listener.poll()[0]
        self.assertEqual(fake_context, received_message.ctxt)
        self.assertEqual(fake_message, received_message.message)

    @unittest.skipUnless(
        _is_kafka_service_running(), "Kafka service is not available")
    def test_receive_message_from_empty_topic_with_timeout(self):
        target = oslo_messaging.Target(
            topic="fake_empty_topic", exchange='fake_empty_exchange')
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener

        deadline = time.time() + 3
        received_message = listener.poll(batch_timeout=3)
        self.assertEqual(0, int(deadline - time.time()))
        self.assertEqual([], received_message)
