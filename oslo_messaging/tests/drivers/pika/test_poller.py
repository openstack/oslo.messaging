#    Copyright 2015 Mirantis, Inc.
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

import time
import unittest

import mock

from oslo_messaging._drivers.pika_driver import pika_poller


class PikaPollerTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._poller_connection_mock = mock.Mock()
        self._poller_channel_mock = mock.Mock()
        self._poller_connection_mock.channel.return_value = (
            self._poller_channel_mock
        )
        self._pika_engine.create_connection.return_value = (
            self._poller_connection_mock
        )
        self._prefetch_count = 123

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = pika_poller.PikaPoller(
            self._pika_engine, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )
        unused = object()
        method = object()
        properties = object()
        body = object()

        self._poller_connection_mock.process_data_events.side_effect = (
            lambda time_limit: poller._on_message_with_ack_callback(
                unused, method, properties, body
            )
        )

        poller.start()
        res = poller.poll()

        self.assertEqual(len(res), 1)

        self.assertEqual(res[0], incoming_message_class_mock.return_value)
        incoming_message_class_mock.assert_called_once_with(
            self._pika_engine, self._poller_channel_mock, method, properties,
            body
        )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_after_stop(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = pika_poller.PikaPoller(
            self._pika_engine, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )

        n = 10
        params = []

        for i in range(n):
            params.append((object(), object(), object(), object()))

        def f(time_limit):
            if poller._started:
                for k in range(n):
                    poller._on_message_no_ack_callback(
                        *params[k]
                    )

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=1)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], incoming_message_class_mock.return_value)
        self.assertEqual(
            incoming_message_class_mock.call_args_list[0][0],
            (self._pika_engine, None) + params[0][1:]
        )

        poller.stop()

        res2 = poller.poll(prefetch_size=n)

        self.assertEqual(len(res2), n - 1)
        self.assertEqual(incoming_message_class_mock.call_count, n)

        self.assertEqual(
            self._poller_connection_mock.process_data_events.call_count, 2)

        for i in range(n - 1):
            self.assertEqual(res2[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i + 1][0],
                (self._pika_engine, None) + params[i + 1][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_batch(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = pika_poller.PikaPoller(
            self._pika_engine, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )

        n = 10
        params = []

        for i in range(n):
            params.append((object(), object(), object(), object()))

        index = [0]

        def f(time_limit):
            poller._on_message_with_ack_callback(
                *params[index[0]]
            )
            index[0] += 1

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=n)

        self.assertEqual(len(res), n)
        self.assertEqual(incoming_message_class_mock.call_count, n)

        for i in range(n):
            self.assertEqual(res[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i][0],
                (self._pika_engine, self._poller_channel_mock) + params[i][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_batch_with_timeout(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = pika_poller.PikaPoller(
            self._pika_engine, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )

        n = 10
        timeout = 1
        sleep_time = 0.2
        params = []

        success_count = 5

        for i in range(n):
            params.append((object(), object(), object(), object()))

        index = [0]

        def f(time_limit):
            time.sleep(sleep_time)
            poller._on_message_with_ack_callback(
                *params[index[0]]
            )
            index[0] += 1

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=n, timeout=timeout)

        self.assertEqual(len(res), success_count)
        self.assertEqual(incoming_message_class_mock.call_count, success_count)

        for i in range(success_count):
            self.assertEqual(res[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i][0],
                (self._pika_engine, self._poller_channel_mock) + params[i][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)


class RpcServicePikaPollerTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._poller_connection_mock = mock.Mock()
        self._poller_channel_mock = mock.Mock()
        self._poller_connection_mock.channel.return_value = (
            self._poller_channel_mock
        )
        self._pika_engine.create_connection.return_value = (
            self._poller_connection_mock
        )

        self._pika_engine.get_rpc_queue_name.side_effect = (
            lambda topic, server, no_ack: "_".join(
                [topic, str(server), str(no_ack)]
            )
        )

        self._pika_engine.get_rpc_exchange_name.side_effect = (
            lambda exchange, topic, fanout, no_ack: "_".join(
                [exchange, topic, str(fanout), str(no_ack)]
            )
        )

        self._prefetch_count = 123
        self._target = mock.Mock(exchange="exchange", topic="topic",
                                 server="server")
        self._pika_engine.rpc_queue_expiration = 12345

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "RpcPikaIncomingMessage")
    def test_declare_rpc_queue_bindings(self, rpc_pika_incoming_message_mock):
        poller = pika_poller.RpcServicePikaPoller(
            self._pika_engine, self._target, self._prefetch_count,
        )
        self._poller_connection_mock.process_data_events.side_effect = (
            lambda time_limit: poller._on_message_with_ack_callback(
                None, None, None, None
            )
        )

        poller.start()
        res = poller.poll()

        self.assertEqual(len(res), 1)

        self.assertEqual(res[0], rpc_pika_incoming_message_mock.return_value)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            declare_queue_binding_by_channel_mock.call_count, 6
        )

        declare_queue_binding_by_channel_mock.assert_has_calls((
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_False_True",
                exchange_type='direct',
                queue="topic_None_True",
                queue_expiration=12345,
                routing_key="topic_None_True"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_False_True",
                exchange_type='direct',
                queue="topic_server_True",
                queue_expiration=12345,
                routing_key="topic_server_True"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_True_True",
                exchange_type='fanout',
                queue="topic_server_True",
                queue_expiration=12345,
                routing_key=''
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_False_False",
                exchange_type='direct',
                queue="topic_None_False",
                queue_expiration=12345,
                routing_key="topic_None_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_False_False",
                exchange_type='direct',
                queue="topic_server_False",
                queue_expiration=12345,
                routing_key="topic_server_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange_topic_True_False",
                exchange_type='fanout',
                queue="topic_server_False",
                queue_expiration=12345,
                routing_key=''
            ),
        ))


class RpcReplyServicePikaPollerTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._poller_connection_mock = mock.Mock()
        self._poller_channel_mock = mock.Mock()
        self._poller_connection_mock.channel.return_value = (
            self._poller_channel_mock
        )
        self._pika_engine.create_connection.return_value = (
            self._poller_connection_mock
        )

        self._prefetch_count = 123
        self._exchange = "rpc_reply_exchange"
        self._queue = "rpc_reply_queue"

        self._pika_engine.rpc_reply_retry_delay = 12132543456

        self._pika_engine.rpc_queue_expiration = 12345
        self._pika_engine.rpc_reply_retry_attempts = 3

    def test_start(self):
        poller = pika_poller.RpcReplyPikaPoller(
            self._pika_engine, self._exchange, self._queue,
            self._prefetch_count,
        )

        poller.start()

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

    def test_declare_rpc_reply_queue_binding(self):
        poller = pika_poller.RpcReplyPikaPoller(
            self._pika_engine, self._exchange, self._queue,
            self._prefetch_count,
        )

        poller.start()

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            declare_queue_binding_by_channel_mock.call_count, 1
        )

        declare_queue_binding_by_channel_mock.assert_called_once_with(
            channel=self._poller_channel_mock, durable=False,
            exchange='rpc_reply_exchange', exchange_type='direct',
            queue='rpc_reply_queue', queue_expiration=12345,
            routing_key='rpc_reply_queue'
        )


class NotificationPikaPollerTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._poller_connection_mock = mock.Mock()
        self._poller_channel_mock = mock.Mock()
        self._poller_connection_mock.channel.return_value = (
            self._poller_channel_mock
        )
        self._pika_engine.create_connection.return_value = (
            self._poller_connection_mock
        )

        self._prefetch_count = 123
        self._target_and_priorities = (
            (
                mock.Mock(exchange="exchange1", topic="topic1",
                          server="server1"), 1
            ),
            (
                mock.Mock(exchange="exchange1", topic="topic1"), 2
            ),
            (
                mock.Mock(exchange="exchange2", topic="topic2",), 1
            ),
        )
        self._pika_engine.notification_persistence = object()

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "PikaIncomingMessage")
    def test_declare_notification_queue_bindings_default_queue(
            self, pika_incoming_message_mock):
        poller = pika_poller.NotificationPikaPoller(
            self._pika_engine, self._target_and_priorities,
            self._prefetch_count, None
        )
        self._poller_connection_mock.process_data_events.side_effect = (
            lambda time_limit: poller._on_message_with_ack_callback(
                None, None, None, None
            )
        )

        poller.start()
        res = poller.poll()

        self.assertEqual(len(res), 1)

        self.assertEqual(res[0], pika_incoming_message_mock.return_value)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            declare_queue_binding_by_channel_mock.call_count, 3
        )

        declare_queue_binding_by_channel_mock.assert_has_calls((
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange1",
                exchange_type='direct',
                queue="topic1.1",
                queue_expiration=None,
                routing_key="topic1.1"
            ),
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange1",
                exchange_type='direct',
                queue="topic1.2",
                queue_expiration=None,
                routing_key="topic1.2"
            ),
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange2",
                exchange_type='direct',
                queue="topic2.1",
                queue_expiration=None,
                routing_key="topic2.1"
            )
        ))

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "PikaIncomingMessage")
    def test_declare_notification_queue_bindings_custom_queue(
            self, pika_incoming_message_mock):
        poller = pika_poller.NotificationPikaPoller(
            self._pika_engine, self._target_and_priorities,
            self._prefetch_count, "custom_queue_name"
        )
        self._poller_connection_mock.process_data_events.side_effect = (
            lambda time_limit: poller._on_message_with_ack_callback(
                None, None, None, None
            )
        )

        poller.start()
        res = poller.poll()

        self.assertEqual(len(res), 1)

        self.assertEqual(res[0], pika_incoming_message_mock.return_value)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            declare_queue_binding_by_channel_mock.call_count, 3
        )

        declare_queue_binding_by_channel_mock.assert_has_calls((
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange1",
                exchange_type='direct',
                queue="custom_queue_name",
                queue_expiration=None,
                routing_key="topic1.1"
            ),
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange1",
                exchange_type='direct',
                queue="custom_queue_name",
                queue_expiration=None,
                routing_key="topic1.2"
            ),
            mock.call(
                channel=self._poller_channel_mock,
                durable=self._pika_engine.notification_persistence,
                exchange="exchange2",
                exchange_type='direct',
                queue="custom_queue_name",
                queue_expiration=None,
                routing_key="topic2.1"
            )
        ))
