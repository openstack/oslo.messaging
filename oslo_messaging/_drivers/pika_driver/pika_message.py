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


import socket
import time
import traceback
import uuid

from concurrent import futures
from oslo_log import log as logging
from oslo_utils import importutils
from oslo_utils import timeutils
from pika import exceptions as pika_exceptions
from pika import spec as pika_spec
import pika_pool
import six
import tenacity


import oslo_messaging
from oslo_messaging._drivers import base
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging import _utils as utils
from oslo_messaging import exceptions


LOG = logging.getLogger(__name__)

_VERSION_HEADER = "version"
_VERSION = "1.0"


class RemoteExceptionMixin(object):
    """Used for constructing dynamic exception type during deserialization of
    remote exception. It defines unified '__init__' method signature and
    exception message format
    """
    def __init__(self, module, clazz, message, trace):
        """Store serialized data
        :param module: String, module name for importing original exception
            class of serialized remote exception
        :param clazz: String, original class name of serialized remote
            exception
        :param message: String, original message of serialized remote
            exception
        :param trace: String, original trace of serialized remote exception
        """
        self.module = module
        self.clazz = clazz
        self.message = message
        self.trace = trace

        self._str_msgs = message + "\n" + "\n".join(trace)

    def __str__(self):
        return self._str_msgs


class PikaIncomingMessage(base.IncomingMessage):
    """Driver friendly adapter for received message. Extract message
    information from RabbitMQ message and provide access to it
    """

    def __init__(self, pika_engine, channel, method, properties, body):
        """Parse RabbitMQ message

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param channel: Channel, RabbitMQ channel which was used for
            this message delivery, used for sending ack back.
            If None - ack is not required
        :param method: Method, RabbitMQ message method
        :param properties: Properties, RabbitMQ message properties
        :param body: Bytes, RabbitMQ message body
        """
        headers = getattr(properties, "headers", {})
        version = headers.get(_VERSION_HEADER, None)
        if not utils.version_is_compatible(version, _VERSION):
            raise pika_drv_exc.UnsupportedDriverVersion(
                "Message's version: {} is not compatible with driver version: "
                "{}".format(version, _VERSION))

        self._pika_engine = pika_engine
        self._channel = channel
        self._delivery_tag = method.delivery_tag

        self._version = version

        self._content_type = properties.content_type
        self.unique_id = properties.message_id

        self.expiration_time = (
            None if properties.expiration is None else
            time.time() + float(properties.expiration) / 1000
        )

        try:
            serializer = pika_drv_cmns.MESSAGE_SERIALIZERS[self._content_type]
        except KeyError:
            raise NotImplementedError(
                "Content-type['{}'] is not supported.".format(
                    self._content_type
                )
            )

        message_dict = serializer.load_from_bytes(body)

        context_dict = {}

        for key in list(message_dict.keys()):
            key = six.text_type(key)
            if key.startswith('_$_'):
                value = message_dict.pop(key)
                context_dict[key[3:]] = value

        super(PikaIncomingMessage, self).__init__(context_dict, message_dict)

    def need_ack(self):
        return self._channel is not None

    def acknowledge(self):
        """Ack the message. Should be called by message processing logic when
        it considered as consumed (means that we don't need redelivery of this
        message anymore)
        """
        if self.need_ack():
            self._channel.basic_ack(delivery_tag=self._delivery_tag)

    def requeue(self):
        """Rollback the message. Should be called by message processing logic
        when it can not process the message right now and should be redelivered
        later if it is possible
        """
        if self.need_ack():
            return self._channel.basic_nack(delivery_tag=self._delivery_tag,
                                            requeue=True)


class RpcPikaIncomingMessage(PikaIncomingMessage, base.RpcIncomingMessage):
    """PikaIncomingMessage implementation for RPC messages. It expects
    extra RPC related fields in message body (msg_id and reply_q). Also 'reply'
    method added to allow consumer to send RPC reply back to the RPC client
    """

    def __init__(self, pika_engine, channel, method, properties, body):
        """Defines default values of msg_id and reply_q fields and just call
        super.__init__ method

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param channel: Channel, RabbitMQ channel which was used for
            this message delivery, used for sending ack back.
            If None - ack is not required
        :param method: Method, RabbitMQ message method
        :param properties: Properties, RabbitMQ message properties
        :param body: Bytes, RabbitMQ message body
        """
        super(RpcPikaIncomingMessage, self).__init__(
            pika_engine, channel, method, properties, body
        )
        self.reply_q = properties.reply_to
        self.msg_id = properties.correlation_id

    def reply(self, reply=None, failure=None):
        """Send back reply to the RPC client
        :param reply: Dictionary, reply. In case of exception should be None
        :param failure: Tuple, should be a sys.exc_info() tuple.
            Should be None if RPC request was successfully processed.

        :return RpcReplyPikaIncomingMessage, message with reply
        """

        if self.reply_q is None:
            return

        reply_outgoing_message = RpcReplyPikaOutgoingMessage(
            self._pika_engine, self.msg_id, reply=reply, failure_info=failure,
            content_type=self._content_type,
        )

        def on_exception(ex):
            if isinstance(ex, pika_drv_exc.ConnectionException):
                LOG.warning(
                    "Connectivity related problem during reply sending. %s",
                    ex
                )
                return True
            else:
                return False

        if self._pika_engine.rpc_reply_retry_attempts:
            retrier = tenacity.retry(
                stop=(
                    tenacity.stop_never
                    if self._pika_engine.rpc_reply_retry_attempts == -1 else
                    tenacity.stop_after_attempt(
                        self._pika_engine.rpc_reply_retry_attempts
                    )
                ),
                retry=tenacity.retry_if_exception(on_exception),
                wait=tenacity.wait_fixed(
                    self._pika_engine.rpc_reply_retry_delay
                )
            )
        else:
            retrier = None

        try:
            timeout = (None if self.expiration_time is None else
                       max(self.expiration_time - time.time(), 0))
            with timeutils.StopWatch(duration=timeout) as stopwatch:
                reply_outgoing_message.send(
                    reply_q=self.reply_q,
                    stopwatch=stopwatch,
                    retrier=retrier
                )
            LOG.debug(
                "Message [id:'%s'] replied to '%s'.", self.msg_id, self.reply_q
            )
        except Exception:
            LOG.exception(
                "Message [id:'%s'] wasn't replied to : %s", self.msg_id,
                self.reply_q
            )


class RpcReplyPikaIncomingMessage(PikaIncomingMessage):
    """PikaIncomingMessage implementation for RPC reply messages. It expects
    extra RPC reply related fields in message body (result and failure).
    """
    def __init__(self, pika_engine, channel, method, properties, body):
        """Defines default values of result and failure fields, call
        super.__init__ method and then construct Exception object if failure is
        not None

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param channel: Channel, RabbitMQ channel which was used for
            this message delivery, used for sending ack back.
            If None - ack is not required
        :param method: Method, RabbitMQ message method
        :param properties: Properties, RabbitMQ message properties
        :param body: Bytes, RabbitMQ message body
        """
        super(RpcReplyPikaIncomingMessage, self).__init__(
            pika_engine, channel, method, properties, body
        )

        self.msg_id = properties.correlation_id

        self.result = self.message.get("s", None)
        self.failure = self.message.get("e", None)

        if self.failure is not None:
            trace = self.failure.get('t', [])
            message = self.failure.get('s', "")
            class_name = self.failure.get('c')
            module_name = self.failure.get('m')

            res_exc = None

            if module_name in pika_engine.allowed_remote_exmods:
                try:
                    module = importutils.import_module(module_name)
                    klass = getattr(module, class_name)

                    ex_type = type(
                        klass.__name__,
                        (RemoteExceptionMixin, klass),
                        {}
                    )

                    res_exc = ex_type(module_name, class_name, message, trace)
                except ImportError as e:
                    LOG.warning(
                        "Can not deserialize remote exception [module:%s, "
                        "class:%s]. %s", module_name, class_name, e
                    )

            # if we have not processed failure yet, use RemoteError class
            if res_exc is None:
                res_exc = oslo_messaging.RemoteError(
                    class_name, message, trace
                )
            self.failure = res_exc


class PikaOutgoingMessage(object):
    """Driver friendly adapter for sending message. Construct RabbitMQ message
    and send it
    """

    def __init__(self, pika_engine, message, context, content_type=None):
        """Parse RabbitMQ message

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param message: Dictionary, user's message fields
        :param context: Dictionary, request context's fields
        :param content_type: String, content-type header, defines serialization
            mechanism, if None default content-type from pika_engine is used
        """

        self._pika_engine = pika_engine

        self._content_type = (
            content_type if content_type is not None else
            self._pika_engine.default_content_type
        )

        try:
            self._serializer = pika_drv_cmns.MESSAGE_SERIALIZERS[
                self._content_type
            ]
        except KeyError:
            raise NotImplementedError(
                "Content-type['{}'] is not supported.".format(
                    self._content_type
                )
            )

        self.message = message
        self.context = context

        self.unique_id = uuid.uuid4().hex

    def _prepare_message_to_send(self):
        """Combine user's message fields an system fields (_unique_id,
        context's data etc)
        """
        msg = self.message.copy()

        if self.context:
            for key, value in self.context.items():
                key = six.text_type(key)
                msg['_$_' + key] = value

        props = pika_spec.BasicProperties(
            content_type=self._content_type,
            headers={_VERSION_HEADER: _VERSION},
            message_id=self.unique_id,
        )
        return msg, props

    @staticmethod
    def _publish(pool, exchange, routing_key, body, properties, mandatory,
                 stopwatch):
        """Execute pika publish method using connection from connection pool
        Also this message catches all pika related exceptions and raise
        oslo.messaging specific exceptions

        :param pool: Pool, pika connection pool for connection choosing
        :param exchange: String, RabbitMQ exchange name for message sending
        :param routing_key: String, RabbitMQ routing key for message routing
        :param body: Bytes, RabbitMQ message payload
        :param properties: Properties, RabbitMQ message properties
        :param mandatory: Boolean, RabbitMQ publish mandatory flag (raise
            exception if it is not possible to deliver message to any queue)
        :param stopwatch: StopWatch, stopwatch object for calculating
            allowed timeouts
        """
        if stopwatch.expired():
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired."
            )
        try:
            timeout = stopwatch.leftover(return_none=True)
            with pool.acquire(timeout=timeout) as conn:
                if timeout is not None:
                    properties.expiration = str(int(timeout * 1000))
                conn.channel.publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                    mandatory=mandatory
                )
        except pika_exceptions.NackError as e:
            raise pika_drv_exc.MessageRejectedException(
                "Can not send message: [body: {}], properties: {}] to "
                "target [exchange: {}, routing_key: {}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except pika_exceptions.UnroutableError as e:
            raise pika_drv_exc.RoutingException(
                "Can not deliver message:[body:{}, properties: {}] to any "
                "queue using target: [exchange:{}, "
                "routing_key:{}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except pika_pool.Timeout as e:
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired. {}".format(str(e))
            )
        except pika_pool.Connection.connectivity_errors as e:
            if (isinstance(e, pika_exceptions.ChannelClosed)
                    and e.args and e.args[0] == 404):
                raise pika_drv_exc.ExchangeNotFoundException(
                    "Attempt to send message to not existing exchange "
                    "detected, message: [body:{}, properties: {}], target: "
                    "[exchange:{}, routing_key:{}]. {}".format(
                        body, properties, exchange, routing_key, str(e)
                    )
                )

            raise pika_drv_exc.ConnectionException(
                "Connectivity problem detected during sending the message: "
                "[body:{}, properties: {}] to target: [exchange:{}, "
                "routing_key:{}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except socket.timeout:
            raise pika_drv_exc.TimeoutConnectionException(
                "Socket timeout exceeded."
            )

    def _do_send(self, exchange, routing_key, msg_dict, msg_props,
                 confirm=True, mandatory=True, persistent=False,
                 stopwatch=pika_drv_cmns.INFINITE_STOP_WATCH, retrier=None):
        """Send prepared message with configured retrying

        :param exchange: String, RabbitMQ exchange name for message sending
        :param routing_key: String, RabbitMQ routing key for message routing
        :param msg_dict: Dictionary, message payload
        :param msg_props: Properties, message properties
        :param confirm: Boolean, enable publisher confirmation if True
        :param mandatory: Boolean, RabbitMQ publish mandatory flag (raise
            exception if it is not possible to deliver message to any queue)
        :param persistent: Boolean, send persistent message if True, works only
            for routing into durable queues
        :param stopwatch: StopWatch, stopwatch object for calculating
            allowed timeouts
        :param retrier: tenacity.Retrying, configured retrier object for
            sending message, if None no retrying is performed
        """
        msg_props.delivery_mode = 2 if persistent else 1

        pool = (self._pika_engine.connection_with_confirmation_pool
                if confirm else
                self._pika_engine.connection_without_confirmation_pool)

        body = self._serializer.dump_as_bytes(msg_dict)

        LOG.debug(
            "Sending message:[body:%s; properties: %s] to target: "
            "[exchange:%s; routing_key:%s]", body, msg_props, exchange,
            routing_key
        )

        publish = (self._publish if retrier is None else
                   retrier(self._publish))

        return publish(pool, exchange, routing_key, body, msg_props,
                       mandatory, stopwatch)

    def send(self, exchange, routing_key='', confirm=True, mandatory=True,
             persistent=False, stopwatch=pika_drv_cmns.INFINITE_STOP_WATCH,
             retrier=None):
        """Send message with configured retrying

        :param exchange: String, RabbitMQ exchange name for message sending
        :param routing_key: String, RabbitMQ routing key for message routing
        :param confirm: Boolean, enable publisher confirmation if True
        :param mandatory: Boolean, RabbitMQ publish mandatory flag (raise
            exception if it is not possible to deliver message to any queue)
        :param persistent: Boolean, send persistent message if True, works only
            for routing into durable queues
        :param stopwatch: StopWatch, stopwatch object for calculating
            allowed timeouts
        :param retrier: tenacity.Retrying, configured retrier object for
            sending message, if None no retrying is performed
        """
        msg_dict, msg_props = self._prepare_message_to_send()

        return self._do_send(exchange, routing_key, msg_dict, msg_props,
                             confirm, mandatory, persistent,
                             stopwatch, retrier)


class RpcPikaOutgoingMessage(PikaOutgoingMessage):
    """PikaOutgoingMessage implementation for RPC messages. It adds
    possibility to wait and receive RPC reply
    """
    def __init__(self, pika_engine, message, context, content_type=None):
        super(RpcPikaOutgoingMessage, self).__init__(
            pika_engine, message, context, content_type
        )
        self.msg_id = None
        self.reply_q = None

    def send(self, exchange, routing_key, reply_listener=None,
             stopwatch=pika_drv_cmns.INFINITE_STOP_WATCH, retrier=None):
        """Send RPC message with configured retrying

        :param exchange: String, RabbitMQ exchange name for message sending
        :param routing_key: String, RabbitMQ routing key for message routing
        :param reply_listener: RpcReplyPikaListener, listener for waiting
            reply. If None - return immediately without reply waiting
        :param stopwatch: StopWatch, stopwatch object for calculating
            allowed timeouts
        :param retrier: tenacity.Retrying, configured retrier object for
            sending message, if None no retrying is performed
        """
        msg_dict, msg_props = self._prepare_message_to_send()

        if reply_listener:
            self.msg_id = uuid.uuid4().hex
            msg_props.correlation_id = self.msg_id
            LOG.debug('MSG_ID is %s', self.msg_id)

            self.reply_q = reply_listener.get_reply_qname()
            msg_props.reply_to = self.reply_q

            future = reply_listener.register_reply_waiter(msg_id=self.msg_id)

            self._do_send(
                exchange=exchange, routing_key=routing_key, msg_dict=msg_dict,
                msg_props=msg_props, confirm=True, mandatory=True,
                persistent=False, stopwatch=stopwatch, retrier=retrier
            )

            try:
                return future.result(stopwatch.leftover(return_none=True))
            except BaseException as e:
                reply_listener.unregister_reply_waiter(self.msg_id)
                if isinstance(e, futures.TimeoutError):
                    e = exceptions.MessagingTimeout()
                raise e
        else:
            self._do_send(
                exchange=exchange, routing_key=routing_key, msg_dict=msg_dict,
                msg_props=msg_props, confirm=True, mandatory=True,
                persistent=False, stopwatch=stopwatch, retrier=retrier
            )


class RpcReplyPikaOutgoingMessage(PikaOutgoingMessage):
    """PikaOutgoingMessage implementation for RPC reply messages. It sets
    correlation_id AMQP property to link this reply with response
    """
    def __init__(self, pika_engine, msg_id, reply=None, failure_info=None,
                 content_type=None):
        """Initialize with reply information for sending

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param msg_id: String, msg_id of RPC request, which waits for reply
        :param reply: Dictionary, reply. In case of exception should be None
        :param failure_info: Tuple, should be a sys.exc_info() tuple.
            Should be None if RPC request was successfully processed.
        :param content_type: String, content-type header, defines serialization
            mechanism, if None default content-type from pika_engine is used
        """
        self.msg_id = msg_id

        if failure_info is not None:
            ex_class = failure_info[0]
            ex = failure_info[1]
            tb = traceback.format_exception(*failure_info)
            if issubclass(ex_class, RemoteExceptionMixin):
                failure_data = {
                    'c': ex.clazz,
                    'm': ex.module,
                    's': ex.message,
                    't': tb
                }
            else:
                failure_data = {
                    'c': six.text_type(ex_class.__name__),
                    'm': six.text_type(ex_class.__module__),
                    's': six.text_type(ex),
                    't': tb
                }

            msg = {'e': failure_data}
        else:
            msg = {'s': reply}

        super(RpcReplyPikaOutgoingMessage, self).__init__(
            pika_engine, msg, None, content_type
        )

    def send(self, reply_q, stopwatch=pika_drv_cmns.INFINITE_STOP_WATCH,
             retrier=None):
        """Send RPC message with configured retrying

        :param reply_q: String, queue name for sending reply
        :param stopwatch: StopWatch, stopwatch object for calculating
            allowed timeouts
        :param retrier: tenacity.Retrying, configured retrier object for
            sending message, if None no retrying is performed
        """

        msg_dict, msg_props = self._prepare_message_to_send()
        msg_props.correlation_id = self.msg_id

        self._do_send(
            exchange=self._pika_engine.rpc_reply_exchange, routing_key=reply_q,
            msg_dict=msg_dict, msg_props=msg_props, confirm=True,
            mandatory=True, persistent=False, stopwatch=stopwatch,
            retrier=retrier
        )
