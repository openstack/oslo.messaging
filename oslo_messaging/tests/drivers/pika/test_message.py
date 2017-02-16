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

import functools
import unittest

from concurrent import futures
from oslo_serialization import jsonutils
from oslo_utils import timeutils
import pika
from six.moves import mock

import oslo_messaging
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_message as pika_drv_msg


class PikaIncomingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._channel = mock.Mock()

        self._delivery_tag = 12345

        self._method = pika.spec.Basic.Deliver(delivery_tag=self._delivery_tag)
        self._properties = pika.BasicProperties(
            content_type="application/json",
            headers={"version": "1.0"},
        )
        self._body = (
            b'{"_$_key_context":"context_value",'
            b'"payload_key": "payload_value"}'
        )

    def test_message_body_parsing(self):
        message = pika_drv_msg.PikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))

    def test_message_acknowledge(self):
        message = pika_drv_msg.PikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        message.acknowledge()

        self.assertEqual(1, self._channel.basic_ack.call_count)
        self.assertEqual({"delivery_tag": self._delivery_tag},
                         self._channel.basic_ack.call_args[1])

    def test_message_acknowledge_no_ack(self):
        message = pika_drv_msg.PikaIncomingMessage(
            self._pika_engine, None, self._method, self._properties,
            self._body
        )

        message.acknowledge()

        self.assertEqual(0, self._channel.basic_ack.call_count)

    def test_message_requeue(self):
        message = pika_drv_msg.PikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        message.requeue()

        self.assertEqual(1, self._channel.basic_nack.call_count)
        self.assertEqual({"delivery_tag": self._delivery_tag, 'requeue': True},
                         self._channel.basic_nack.call_args[1])

    def test_message_requeue_no_ack(self):
        message = pika_drv_msg.PikaIncomingMessage(
            self._pika_engine, None, self._method, self._properties,
            self._body
        )

        message.requeue()

        self.assertEqual(0, self._channel.basic_nack.call_count)


class RpcPikaIncomingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._pika_engine.rpc_reply_retry_attempts = 3
        self._pika_engine.rpc_reply_retry_delay = 0.25

        self._channel = mock.Mock()

        self._delivery_tag = 12345

        self._method = pika.spec.Basic.Deliver(delivery_tag=self._delivery_tag)
        self._body = (
            b'{"_$_key_context":"context_value",'
            b'"payload_key":"payload_value"}'
        )
        self._properties = pika.BasicProperties(
            content_type="application/json",
            headers={"version": "1.0"},
        )

    def test_call_message_body_parsing(self):
        self._properties.correlation_id = 123456789
        self._properties.reply_to = "reply_queue"

        message = pika_drv_msg.RpcPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertEqual(123456789, message.msg_id)
        self.assertEqual("reply_queue", message.reply_q)

        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))

    def test_cast_message_body_parsing(self):
        message = pika_drv_msg.RpcPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertIsNone(message.msg_id)
        self.assertIsNone(message.reply_q)

        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))

    @mock.patch(("oslo_messaging._drivers.pika_driver.pika_message."
                 "PikaOutgoingMessage.send"))
    def test_reply_for_cast_message(self, send_reply_mock):
        message = pika_drv_msg.RpcPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertIsNone(message.msg_id)
        self.assertIsNone(message.reply_q)

        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))

        message.reply(reply=object())

        self.assertEqual(0, send_reply_mock.call_count)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "RpcReplyPikaOutgoingMessage")
    @mock.patch("tenacity.retry")
    def test_positive_reply_for_call_message(self,
                                             retry_mock,
                                             outgoing_message_mock):
        self._properties.correlation_id = 123456789
        self._properties.reply_to = "reply_queue"

        message = pika_drv_msg.RpcPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertEqual(123456789, message.msg_id)
        self.assertEqual("reply_queue", message.reply_q)

        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))
        reply = "all_fine"
        message.reply(reply=reply)

        outgoing_message_mock.assert_called_once_with(
            self._pika_engine, 123456789, failure_info=None, reply='all_fine',
            content_type='application/json'
        )
        outgoing_message_mock().send.assert_called_once_with(
            reply_q='reply_queue', stopwatch=mock.ANY, retrier=mock.ANY
        )
        retry_mock.assert_called_once_with(
            stop=mock.ANY, retry=mock.ANY, wait=mock.ANY
        )

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_message."
                "RpcReplyPikaOutgoingMessage")
    @mock.patch("tenacity.retry")
    def test_negative_reply_for_call_message(self,
                                             retry_mock,
                                             outgoing_message_mock):
        self._properties.correlation_id = 123456789
        self._properties.reply_to = "reply_queue"

        message = pika_drv_msg.RpcPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            self._body
        )

        self.assertEqual("context_value",
                         message.ctxt.get("key_context", None))
        self.assertEqual(123456789, message.msg_id)
        self.assertEqual("reply_queue", message.reply_q)

        self.assertEqual("payload_value",
                         message.message.get("payload_key", None))

        failure_info = object()
        message.reply(failure=failure_info)

        outgoing_message_mock.assert_called_once_with(
            self._pika_engine, 123456789,
            failure_info=failure_info,
            reply=None,
            content_type='application/json'
        )
        outgoing_message_mock().send.assert_called_once_with(
            reply_q='reply_queue', stopwatch=mock.ANY, retrier=mock.ANY
        )
        retry_mock.assert_called_once_with(
            stop=mock.ANY, retry=mock.ANY, wait=mock.ANY
        )


class RpcReplyPikaIncomingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._pika_engine.allowed_remote_exmods = [
            pika_drv_cmns.EXCEPTIONS_MODULE, "oslo_messaging.exceptions"
        ]

        self._channel = mock.Mock()

        self._delivery_tag = 12345

        self._method = pika.spec.Basic.Deliver(delivery_tag=self._delivery_tag)

        self._properties = pika.BasicProperties(
            content_type="application/json",
            headers={"version": "1.0"},
            correlation_id=123456789
        )

    def test_positive_reply_message_body_parsing(self):

        body = b'{"s": "all fine"}'

        message = pika_drv_msg.RpcReplyPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            body
        )

        self.assertEqual(123456789, message.msg_id)
        self.assertIsNone(message.failure)
        self.assertEqual("all fine", message.result)

    def test_negative_reply_message_body_parsing(self):

        body = (b'{'
                b'    "e": {'
                b'         "s": "Error message",'
                b'         "t": ["TRACE HERE"],'
                b'         "c": "MessagingException",'
                b'         "m": "oslo_messaging.exceptions"'
                b'     }'
                b'}')

        message = pika_drv_msg.RpcReplyPikaIncomingMessage(
            self._pika_engine, self._channel, self._method, self._properties,
            body
        )

        self.assertEqual(123456789, message.msg_id)
        self.assertIsNone(message.result)
        self.assertEqual(
            'Error message\n'
            'TRACE HERE',
            str(message.failure)
        )
        self.assertIsInstance(message.failure,
                              oslo_messaging.MessagingException)


class PikaOutgoingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.MagicMock()
        self._pika_engine.default_content_type = "application/json"
        self._exchange = "it is exchange"
        self._routing_key = "it is routing key"
        self._expiration = 1
        self._stopwatch = (
            timeutils.StopWatch(duration=self._expiration).start()
        )
        self._mandatory = object()

        self._message = {"msg_type": 1, "msg_str": "hello"}
        self._context = {"request_id": 555, "token": "it is a token"}

    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_send_with_confirmation(self):
        message = pika_drv_msg.PikaOutgoingMessage(
            self._pika_engine, self._message, self._context
        )

        message.send(
            exchange=self._exchange,
            routing_key=self._routing_key,
            confirm=True,
            mandatory=self._mandatory,
            persistent=True,
            stopwatch=self._stopwatch,
            retrier=None
        )

        self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=mock.ANY,
            exchange=self._exchange, mandatory=self._mandatory,
            properties=mock.ANY,
            routing_key=self._routing_key
        )

        body = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["body"]

        self.assertEqual(
            b'{"_$_request_id": 555, "_$_token": "it is a token", '
            b'"msg_str": "hello", "msg_type": 1}',
            body
        )

        props = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(2, props.delivery_mode)
        self.assertTrue(self._expiration * 1000 - float(props.expiration) <
                        100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertTrue(props.message_id)

    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_send_without_confirmation(self):
        message = pika_drv_msg.PikaOutgoingMessage(
            self._pika_engine, self._message, self._context
        )

        message.send(
            exchange=self._exchange,
            routing_key=self._routing_key,
            confirm=False,
            mandatory=self._mandatory,
            persistent=False,
            stopwatch=self._stopwatch,
            retrier=None
        )

        self._pika_engine.connection_without_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=mock.ANY,
            exchange=self._exchange, mandatory=self._mandatory,
            properties=mock.ANY,
            routing_key=self._routing_key
        )

        body = self._pika_engine.connection_without_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["body"]

        self.assertEqual(
            b'{"_$_request_id": 555, "_$_token": "it is a token", '
            b'"msg_str": "hello", "msg_type": 1}',
            body
        )

        props = self._pika_engine.connection_without_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(1, props.delivery_mode)
        self.assertTrue(self._expiration * 1000 - float(props.expiration)
                        < 100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertTrue(props.message_id)


class RpcPikaOutgoingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._exchange = "it is exchange"
        self._routing_key = "it is routing key"

        self._pika_engine = mock.MagicMock()
        self._pika_engine.get_rpc_exchange_name.return_value = self._exchange
        self._pika_engine.get_rpc_queue_name.return_value = self._routing_key
        self._pika_engine.default_content_type = "application/json"

        self._message = {"msg_type": 1, "msg_str": "hello"}
        self._context = {"request_id": 555, "token": "it is a token"}

    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_send_cast_message(self):
        message = pika_drv_msg.RpcPikaOutgoingMessage(
            self._pika_engine, self._message, self._context
        )

        expiration = 1
        stopwatch = timeutils.StopWatch(duration=expiration).start()

        message.send(
            exchange=self._exchange,
            routing_key=self._routing_key,
            reply_listener=None,
            stopwatch=stopwatch,
            retrier=None
        )

        self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=mock.ANY,
            exchange=self._exchange, mandatory=True,
            properties=mock.ANY,
            routing_key=self._routing_key
        )

        body = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["body"]

        self.assertEqual(
            b'{"_$_request_id": 555, "_$_token": "it is a token", '
            b'"msg_str": "hello", "msg_type": 1}',
            body
        )

        props = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(1, props.delivery_mode)
        self.assertTrue(expiration * 1000 - float(props.expiration) < 100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertIsNone(props.correlation_id)
        self.assertIsNone(props.reply_to)
        self.assertTrue(props.message_id)

    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_send_call_message(self):
        message = pika_drv_msg.RpcPikaOutgoingMessage(
            self._pika_engine, self._message, self._context
        )

        expiration = 1
        stopwatch = timeutils.StopWatch(duration=expiration).start()

        result = "it is a result"
        reply_queue_name = "reply_queue_name"

        future = futures.Future()
        future.set_result(result)
        reply_listener = mock.Mock()
        reply_listener.register_reply_waiter.return_value = future
        reply_listener.get_reply_qname.return_value = reply_queue_name

        res = message.send(
            exchange=self._exchange,
            routing_key=self._routing_key,
            reply_listener=reply_listener,
            stopwatch=stopwatch,
            retrier=None
        )

        self.assertEqual(result, res)

        self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=mock.ANY,
            exchange=self._exchange, mandatory=True,
            properties=mock.ANY,
            routing_key=self._routing_key
        )

        body = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["body"]

        self.assertEqual(
            b'{"_$_request_id": 555, "_$_token": "it is a token", '
            b'"msg_str": "hello", "msg_type": 1}',
            body
        )

        props = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(1, props.delivery_mode)
        self.assertTrue(expiration * 1000 - float(props.expiration) < 100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertEqual(message.msg_id, props.correlation_id)
        self.assertEqual(reply_queue_name, props.reply_to)
        self.assertTrue(props.message_id)


class RpcReplyPikaOutgoingMessageTestCase(unittest.TestCase):
    def setUp(self):
        self._reply_q = "reply_queue_name"

        self._expiration = 1
        self._stopwatch = (
            timeutils.StopWatch(duration=self._expiration).start()
        )

        self._pika_engine = mock.MagicMock()

        self._rpc_reply_exchange = "rpc_reply_exchange"
        self._pika_engine.rpc_reply_exchange = self._rpc_reply_exchange
        self._pika_engine.default_content_type = "application/json"

        self._msg_id = 12345567

    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_success_message_send(self):
        message = pika_drv_msg.RpcReplyPikaOutgoingMessage(
            self._pika_engine, self._msg_id, reply="all_fine"
        )

        message.send(self._reply_q, stopwatch=self._stopwatch, retrier=None)

        self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=b'{"s": "all_fine"}',
            exchange=self._rpc_reply_exchange, mandatory=True,
            properties=mock.ANY,
            routing_key=self._reply_q
        )

        props = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(1, props.delivery_mode)
        self.assertTrue(self._expiration * 1000 - float(props.expiration) <
                        100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertEqual(message.msg_id, props.correlation_id)
        self.assertIsNone(props.reply_to)
        self.assertTrue(props.message_id)

    @mock.patch("traceback.format_exception", new=lambda x, y, z: z)
    @mock.patch("oslo_serialization.jsonutils.dump_as_bytes",
                new=functools.partial(jsonutils.dump_as_bytes, sort_keys=True))
    def test_failure_message_send(self):
        failure_info = (oslo_messaging.MessagingException,
                        oslo_messaging.MessagingException("Error message"),
                        ['It is a trace'])

        message = pika_drv_msg.RpcReplyPikaOutgoingMessage(
            self._pika_engine, self._msg_id, failure_info=failure_info
        )

        message.send(self._reply_q, stopwatch=self._stopwatch, retrier=None)

        self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.assert_called_once_with(
            body=mock.ANY,
            exchange=self._rpc_reply_exchange,
            mandatory=True,
            properties=mock.ANY,
            routing_key=self._reply_q
        )

        body = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["body"]
        self.assertEqual(
            b'{"e": {"c": "MessagingException", '
            b'"m": "oslo_messaging.exceptions", "s": "Error message", '
            b'"t": ["It is a trace"]}}',
            body
        )

        props = self._pika_engine.connection_with_confirmation_pool.acquire(
        ).__enter__().channel.publish.call_args[1]["properties"]

        self.assertEqual('application/json', props.content_type)
        self.assertEqual(1, props.delivery_mode)
        self.assertTrue(self._expiration * 1000 - float(props.expiration) <
                        100)
        self.assertEqual({'version': '1.0'}, props.headers)
        self.assertEqual(message.msg_id, props.correlation_id)
        self.assertIsNone(props.reply_to)
        self.assertTrue(props.message_id)
