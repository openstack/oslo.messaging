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

import threading
import time
import unittest

from concurrent import futures
from six.moves import mock

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
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

        self._executor = futures.ThreadPoolExecutor(1)

        def timer_task(timeout, callback):
            time.sleep(timeout)
            callback()

        self._poller_connection_mock.add_timeout.side_effect = (
            lambda *args: self._executor.submit(timer_task, *args)
        )

        self._prefetch_count = 123

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_start(self, declare_queue_binding_mock):
        poller = pika_poller.PikaPoller(
            self._pika_engine, 1, None, self._prefetch_count, None
        )

        poller.start(None)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)
        self.assertTrue(declare_queue_binding_mock.called)

    def test_start_when_connection_unavailable(self):
        poller = pika_poller.PikaPoller(
            self._pika_engine, 1, None, self._prefetch_count, None
        )

        self._pika_engine.create_connection.side_effect = (
            pika_drv_exc.EstablishConnectionException
        )

        # start() should not raise socket.timeout exception
        poller.start(None)

        # stop is needed to stop reconnection background job
        poller.stop()

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_message_processing(self, declare_queue_binding_mock):
        res = []

        def on_incoming_callback(incoming):
            res.append(incoming)

        incoming_message_class_mock = mock.Mock()
        poller = pika_poller.PikaPoller(
            self._pika_engine, 1, None, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )
        unused = object()
        method = object()
        properties = object()
        body = object()

        poller.start(on_incoming_callback)
        poller._on_message_with_ack_callback(
            unused, method, properties, body
        )

        self.assertEqual(1, len(res))

        self.assertEqual([incoming_message_class_mock.return_value], res[0])
        incoming_message_class_mock.assert_called_once_with(
            self._pika_engine, self._poller_channel_mock, method, properties,
            body
        )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_message_processing_batch(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()

        n = 10
        params = []

        res = []

        def on_incoming_callback(incoming):
            res.append(incoming)

        poller = pika_poller.PikaPoller(
            self._pika_engine, n, None, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )

        for i in range(n):
            params.append((object(), object(), object(), object()))

        poller.start(on_incoming_callback)

        for i in range(n):
            poller._on_message_with_ack_callback(
                *params[i]
            )

        self.assertEqual(1, len(res))
        self.assertEqual(10, len(res[0]))
        self.assertEqual(n, incoming_message_class_mock.call_count)

        for i in range(n):
            self.assertEqual(incoming_message_class_mock.return_value,
                             res[0][i])
            self.assertEqual(
                (self._pika_engine, self._poller_channel_mock) + params[i][1:],
                incoming_message_class_mock.call_args_list[i][0]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_message_processing_batch_with_timeout(self,
                                                   declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()

        n = 10
        timeout = 1

        res = []
        evt = threading.Event()

        def on_incoming_callback(incoming):
            res.append(incoming)
            evt.set()

        poller = pika_poller.PikaPoller(
            self._pika_engine, n, timeout, self._prefetch_count,
            incoming_message_class=incoming_message_class_mock
        )

        params = []

        success_count = 5

        poller.start(on_incoming_callback)

        for i in range(n):
            params.append((object(), object(), object(), object()))

        for i in range(success_count):
            poller._on_message_with_ack_callback(
                *params[i]
            )

        self.assertTrue(evt.wait(timeout * 2))

        self.assertEqual(1, len(res))
        self.assertEqual(success_count, len(res[0]))
        self.assertEqual(success_count, incoming_message_class_mock.call_count)

        for i in range(success_count):
            self.assertEqual(incoming_message_class_mock.return_value,
                             res[0][i])
            self.assertEqual(
                (self._pika_engine, self._poller_channel_mock) + params[i][1:],
                incoming_message_class_mock.call_args_list[i][0]
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
            lambda topic, server, no_ack, worker=False:
                "_".join([topic, str(server), str(no_ack), str(worker)])
        )

        self._pika_engine.get_rpc_exchange_name.side_effect = (
            lambda exchange: exchange
        )

        self._prefetch_count = 123
        self._target = mock.Mock(exchange="exchange", topic="topic",
                                 server="server")
        self._pika_engine.rpc_queue_expiration = 12345

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "RpcPikaIncomingMessage")
    def test_declare_rpc_queue_bindings(self, rpc_pika_incoming_message_mock):
        poller = pika_poller.RpcServicePikaPoller(
            self._pika_engine, self._target, 1, None,
            self._prefetch_count
        )

        poller.start(None)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            6, declare_queue_binding_by_channel_mock.call_count
        )

        declare_queue_binding_by_channel_mock.assert_has_calls((
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_None_True_False",
                queue_expiration=12345,
                routing_key="topic_None_True_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_server_True_False",
                queue_expiration=12345,
                routing_key="topic_server_True_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_server_True_True",
                queue_expiration=12345,
                routing_key="topic_all_workers_True_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_None_False_False",
                queue_expiration=12345,
                routing_key="topic_None_False_False"
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_server_False_False",
                queue_expiration=12345,
                routing_key='topic_server_False_False'
            ),
            mock.call(
                channel=self._poller_channel_mock, durable=False,
                exchange="exchange",
                exchange_type='direct',
                queue="topic_server_False_True",
                queue_expiration=12345,
                routing_key='topic_all_workers_False_False'
            )
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

    def test_declare_rpc_reply_queue_binding(self):
        poller = pika_poller.RpcReplyPikaPoller(
            self._pika_engine, self._exchange, self._queue, 1, None,
            self._prefetch_count,
        )

        poller.start(None)
        poller.stop()

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            1, declare_queue_binding_by_channel_mock.call_count
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

    def test_declare_notification_queue_bindings_default_queue(self):
        poller = pika_poller.NotificationPikaPoller(
            self._pika_engine, self._target_and_priorities, 1, None,
            self._prefetch_count, None
        )

        poller.start(None)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            3, declare_queue_binding_by_channel_mock.call_count
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

    def test_declare_notification_queue_bindings_custom_queue(self):
        poller = pika_poller.NotificationPikaPoller(
            self._pika_engine, self._target_and_priorities, 1, None,
            self._prefetch_count, "custom_queue_name"
        )

        poller.start(None)

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        declare_queue_binding_by_channel_mock = (
            self._pika_engine.declare_queue_binding_by_channel
        )

        self.assertEqual(
            3, declare_queue_binding_by_channel_mock.call_count
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
