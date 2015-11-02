#    Copyright 2011 OpenStack Foundation
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
import collections
from concurrent import futures

import pika
from pika import adapters as pika_adapters
from pika import credentials as pika_credentials
from pika import exceptions as pika_exceptions
from pika import spec as pika_spec

import pika_pool
import retrying

import six
import socket
import sys
import threading
import time
import uuid


from oslo_config import cfg
from oslo_log import log as logging

from oslo_messaging._drivers import common
from oslo_messaging import exceptions

from oslo_serialization import jsonutils


LOG = logging.getLogger(__name__)

pika_opts = [
    cfg.IntOpt('channel_max', default=None,
               help='Maximum number of channels to allow'),
    cfg.IntOpt('frame_max', default=None,
               help='The maximum byte size for an AMQP frame'),
    cfg.IntOpt('heartbeat_interval', default=1,
               help="How often to send heartbeats for consumer's connections"),
    cfg.BoolOpt('ssl', default=None,
                help='Enable SSL'),
    cfg.DictOpt('ssl_options', default=None,
                help='Arguments passed to ssl.wrap_socket'),
    cfg.FloatOpt('socket_timeout', default=0.25,
                 help="Set socket timeout in seconds for connection's socket"),
    cfg.FloatOpt('tcp_user_timeout', default=0.25,
                 help="Set TCP_USER_TIMEOUT in seconds for connection's "
                      "socket"),
    cfg.FloatOpt('host_connection_reconnect_delay', default=5,
                 help="Set delay for reconnection to some host which has "
                      "connection error")
]

pika_pool_opts = [
    cfg.IntOpt('pool_max_size', default=10,
               help="Maximum number of connections to keep queued."),
    cfg.IntOpt('pool_max_overflow', default=0,
               help="Maximum number of connections to create above "
                    "`pool_max_size`."),
    cfg.IntOpt('pool_timeout', default=30,
               help="Default number of seconds to wait for a connections to "
                    "available"),
    cfg.IntOpt('pool_recycle', default=600,
               help="Lifetime of a connection (since creation) in seconds "
                    "or None for no recycling. Expired connections are "
                    "closed on acquire."),
    cfg.IntOpt('pool_stale', default=60,
               help="Threshold at which inactive (since release) connections "
                    "are considered stale in seconds or None for no "
                    "staleness. Stale connections are closed on acquire.")
]

notification_opts = [
    cfg.BoolOpt('notification_persistence', default=False,
                help="Persist notification messages."),
    cfg.StrOpt('default_notification_exchange',
               default="${control_exchange}_notification",
               help="Exchange name for for sending notifications"),
    cfg.IntOpt(
        'default_notification_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending notification, -1 means infinite retry."
    ),
    cfg.FloatOpt(
        'notification_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending notification message"
    )
]

rpc_opts = [
    cfg.IntOpt('rpc_queue_expiration', default=60,
               help="Time to live for rpc queues without consumers in "
                    "seconds."),
    cfg.StrOpt('default_rpc_exchange', default="${control_exchange}_rpc",
               help="Exchange name for for sending RPC messages"),
    cfg.StrOpt('rpc_reply_exchange', default="${control_exchange}_rpc_reply",
               help="Exchange name for for receiving RPC replies"),

    cfg.BoolOpt('rpc_listener_ack', default=True,
                help="Disable to increase performance. If disabled - some "
                     "messages may be lost in case of connectivity problem. "
                     "If enabled - may cause not needed message redelivery "
                     "and rpc request could be processed more then one time"),
    cfg.BoolOpt('rpc_reply_listener_ack', default=True,
                help="Disable to increase performance. If disabled - some "
                     "replies may be lost in case of connectivity problem."),
    cfg.IntOpt(
        'rpc_listener_prefetch_count', default=10,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc listener. Works only if rpc_listener_ack == True"
    ),
    cfg.IntOpt(
        'rpc_reply_listener_prefetch_count', default=10,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc reply listener. Works only if rpc_reply_listener_ack == "
             "True"
    ),
    cfg.IntOpt(
        'rpc_reply_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending reply. -1 means infinite retry during rpc_timeout"
    ),
    cfg.FloatOpt(
        'rpc_reply_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending reply."
    ),
    cfg.IntOpt(
        'default_rpc_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending RPC message, -1 means infinite retry. If actual "
             "retry attempts in not 0 the rpc request could be processed more "
             "then one time"
    ),
    cfg.FloatOpt(
        'rpc_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending RPC message"
    )
]


def _is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('thread')


class ExchangeNotFoundException(exceptions.MessageDeliveryFailure):
    pass


class MessageRejectedException(exceptions.MessageDeliveryFailure):
    pass


class RoutingException(exceptions.MessageDeliveryFailure):
    pass


class ConnectionException(exceptions.MessagingException):
    pass


class HostConnectionNotAllowedException(ConnectionException):
    pass


class EstablishConnectionException(ConnectionException):
    pass


class TimeoutConnectionException(ConnectionException):
    pass


class PooledConnectionWithConfirmations(pika_pool.Connection):
    @property
    def channel(self):
        if self.fairy.channel is None:
            self.fairy.channel = self.fairy.cxn.channel()
            self.fairy.channel.confirm_delivery()
        return self.fairy.channel


class PikaEngine(object):
    HOST_CONNECTION_LAST_TRY_TIME = "last_try_time"
    HOST_CONNECTION_LAST_SUCCESS_TRY_TIME = "last_success_try_time"

    TCP_USER_TIMEOUT = 18

    def __init__(self, conf, url, default_exchange=None):
        self.conf = conf

        # processing rpc options

        self.default_rpc_exchange = (
            conf.oslo_messaging_pika.default_rpc_exchange if
            conf.oslo_messaging_pika.default_rpc_exchange else
            default_exchange
        )
        self.rpc_reply_exchange = (
            conf.oslo_messaging_pika.rpc_reply_exchange if
            conf.oslo_messaging_pika.rpc_reply_exchange else
            default_exchange
        )

        self.rpc_listener_ack = conf.oslo_messaging_pika.rpc_listener_ack

        self.rpc_reply_listener_ack = (
            conf.oslo_messaging_pika.rpc_reply_listener_ack
        )

        self.rpc_listener_prefetch_count = (
            conf.oslo_messaging_pika.rpc_listener_prefetch_count
        )

        self.rpc_reply_listener_prefetch_count = (
            conf.oslo_messaging_pika.rpc_listener_prefetch_count
        )

        self.rpc_reply_retry_attempts = (
            conf.oslo_messaging_pika.rpc_reply_retry_attempts
        )
        if self.rpc_reply_retry_attempts is None:
            raise ValueError("rpc_reply_retry_attempts should be integer")
        self.rpc_reply_retry_delay = (
            conf.oslo_messaging_pika.rpc_reply_retry_delay
        )
        if (self.rpc_reply_retry_delay is None or
                self.rpc_reply_retry_delay < 0):
            raise ValueError("rpc_reply_retry_delay should be non-negative "
                             "integer")

        # processing notification options
        self.default_notification_exchange = (
            conf.oslo_messaging_pika.default_notification_exchange if
            conf.oslo_messaging_pika.default_notification_exchange else
            default_exchange
        )

        self.notification_persistence = (
            conf.oslo_messaging_pika.notification_persistence
        )

        self.default_rpc_retry_attempts = (
            conf.oslo_messaging_pika.default_rpc_retry_attempts
        )
        if self.default_rpc_retry_attempts is None:
            raise ValueError("default_rpc_retry_attempts should be an integer")
        self.rpc_retry_delay = (
            conf.oslo_messaging_pika.rpc_retry_delay
        )
        if (self.rpc_retry_delay is None or
                self.rpc_retry_delay < 0):
            raise ValueError("rpc_retry_delay should be non-negative integer")

        self.default_notification_retry_attempts = (
            conf.oslo_messaging_pika.default_notification_retry_attempts
        )
        if self.default_notification_retry_attempts is None:
            raise ValueError("default_notification_retry_attempts should be "
                             "an integer")
        self.notification_retry_delay = (
            conf.oslo_messaging_pika.notification_retry_delay
        )
        if (self.notification_retry_delay is None or
                self.notification_retry_delay < 0):
            raise ValueError("notification_retry_delay should be non-negative "
                             "integer")

        # preparing poller for listening replies
        self._reply_queue = None

        self._reply_listener = None
        self._reply_waiting_future_list = []

        self._reply_consumer_enabled = False
        self._reply_consumer_thread_run_flag = True
        self._reply_consumer_lock = threading.Lock()
        self._puller_thread = None

        self._tcp_user_timeout = self.conf.oslo_messaging_pika.tcp_user_timeout
        self._host_connection_reconnect_delay = (
            self.conf.oslo_messaging_pika.host_connection_reconnect_delay
        )

        # initializing connection parameters for configured RabbitMQ hosts
        common_pika_params = {
            'virtual_host': url.virtual_host,
            'channel_max': self.conf.oslo_messaging_pika.channel_max,
            'frame_max': self.conf.oslo_messaging_pika.frame_max,
            'ssl': self.conf.oslo_messaging_pika.ssl,
            'ssl_options': self.conf.oslo_messaging_pika.ssl_options,
            'socket_timeout': self.conf.oslo_messaging_pika.socket_timeout,
        }

        self._connection_lock = threading.Lock()

        self._connection_host_param_list = []
        self._connection_host_status_list = []
        self._next_connection_host_num = 0

        for transport_host in url.hosts:
            pika_params = common_pika_params.copy()
            pika_params.update(
                host=transport_host.hostname,
                port=transport_host.port,
                credentials=pika_credentials.PlainCredentials(
                    transport_host.username, transport_host.password
                ),
            )
            self._connection_host_param_list.append(pika_params)
            self._connection_host_status_list.append({
                self.HOST_CONNECTION_LAST_TRY_TIME: 0,
                self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME: 0
            })

        # initializing 2 connection pools: 1st for connections without
        # confirmations, 2nd - with confirmations
        self.connection_pool = pika_pool.QueuedPool(
            create=self.create_connection,
            max_size=self.conf.oslo_messaging_pika.pool_max_size,
            max_overflow=self.conf.oslo_messaging_pika.pool_max_overflow,
            timeout=self.conf.oslo_messaging_pika.pool_timeout,
            recycle=self.conf.oslo_messaging_pika.pool_recycle,
            stale=self.conf.oslo_messaging_pika.pool_stale,
        )

        self.connection_with_confirmation_pool = pika_pool.QueuedPool(
            create=self.create_connection,
            max_size=self.conf.oslo_messaging_pika.pool_max_size,
            max_overflow=self.conf.oslo_messaging_pika.pool_max_overflow,
            timeout=self.conf.oslo_messaging_pika.pool_timeout,
            recycle=self.conf.oslo_messaging_pika.pool_recycle,
            stale=self.conf.oslo_messaging_pika.pool_stale,
        )

        self.connection_with_confirmation_pool.Connection = (
            PooledConnectionWithConfirmations
        )

    def _next_connection_num(self):
        with self._connection_lock:
            cur_num = self._next_connection_host_num
            self._next_connection_host_num += 1
            self._next_connection_host_num %= len(
                self._connection_host_param_list
            )
        return cur_num

    def create_connection(self, for_listening=False):
        """Create and return connection to any available host.

        :return: cerated connection
        :raise: ConnectionException if all hosts are not reachable
        """
        host_count = len(self._connection_host_param_list)
        connection_attempts = host_count

        pika_next_connection_num = self._next_connection_num()

        while connection_attempts > 0:
            try:
                return self.create_host_connection(
                    pika_next_connection_num, for_listening
                )
            except pika_pool.Connection.connectivity_errors as e:
                LOG.warn(str(e))
            except HostConnectionNotAllowedException as e:
                LOG.warn(str(e))

            connection_attempts -= 1
            pika_next_connection_num += 1
            pika_next_connection_num %= host_count

        raise EstablishConnectionException(
            "Can not establish connection to any configured RabbitMQ host: " +
            str(self._connection_host_param_list)
        )

    def _set_tcp_user_timeout(self, s):
        if not self._tcp_user_timeout:
            return
        try:
            s.setsockopt(
                socket.IPPROTO_TCP, self.TCP_USER_TIMEOUT,
                int(self._tcp_user_timeout * 1000)
            )
        except socket.error:
            LOG.warn(
                "Whoops, this kernel doesn't seem to support TCP_USER_TIMEOUT."
            )

    def create_host_connection(self, host_index, for_listening=False):
        """Create new connection to host #host_index

        :return: New connection
        """

        with self._connection_lock:
            cur_time = time.time()

            last_success_time = self._connection_host_status_list[host_index][
                self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
            ]
            last_time = self._connection_host_status_list[host_index][
                self.HOST_CONNECTION_LAST_TRY_TIME
            ]
            if (last_time != last_success_time and
                    cur_time - last_time <
                    self._host_connection_reconnect_delay):
                raise HostConnectionNotAllowedException(
                    "Connection to host #{} is not allowed now because of "
                    "previous failure".format(host_index)
                )

            try:
                base_host_params = self._connection_host_param_list[host_index]

                connection = pika_adapters.BlockingConnection(
                    pika.ConnectionParameters(
                        heartbeat_interval=(
                            self.conf.oslo_messaging_pika.heartbeat_interval
                            if for_listening else None
                        ),
                        **base_host_params
                    )
                )

                self._set_tcp_user_timeout(connection._impl.socket)

                self._connection_host_status_list[host_index][
                    self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
                ] = cur_time

                return connection
            finally:
                self._connection_host_status_list[host_index][
                    self.HOST_CONNECTION_LAST_TRY_TIME
                ] = cur_time

    @staticmethod
    def declare_queue_binding_by_channel(channel, exchange, queue, routing_key,
                                         exchange_type, queue_expiration,
                                         queue_auto_delete, durable):
        channel.exchange_declare(
            exchange, exchange_type, auto_delete=True, durable=durable
        )
        arguments = {}

        if queue_expiration > 0:
            arguments['x-expires'] = queue_expiration * 1000

        channel.queue_declare(
            queue, auto_delete=queue_auto_delete, durable=durable,
            arguments=arguments
        )

        channel.queue_bind(queue, exchange, routing_key)

    def declare_queue_binding(self, exchange, queue, routing_key,
                              exchange_type, queue_expiration,
                              queue_auto_delete, durable,
                              timeout=None):
        if timeout is not None and timeout < 0:
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired."
            )
        try:
            with self.connection_pool.acquire(timeout=timeout) as conn:
                self.declare_queue_binding_by_channel(
                    conn.channel, exchange, queue, routing_key, exchange_type,
                    queue_expiration, queue_auto_delete, durable
                )
        except pika_pool.Timeout as e:
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired. {}.".format(str(e))
            )
        except pika_pool.Connection.connectivity_errors as e:
            raise ConnectionException(
                "Connectivity problem detected during declaring queue "
                "binding: exchange:{}, queue: {}, routing_key: {}, "
                "exchange_type: {}, queue_expiration: {}, queue_auto_delete: "
                "{}, durable: {}. {}".format(
                    exchange, queue, routing_key, exchange_type,
                    queue_expiration, queue_auto_delete, durable, str(e)
                )
            )

    @staticmethod
    def _do_publish(pool, exchange, routing_key, body, properties,
                    mandatory, expiration_time):
        timeout = (None if expiration_time is None else
                   expiration_time - time.time())
        if timeout is not None and timeout < 0:
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired."
            )
        try:
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
            raise MessageRejectedException(
                "Can not send message: [body: {}], properties: {}] to "
                "target [exchange: {}, routing_key: {}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except pika_exceptions.UnroutableError as e:
            raise RoutingException(
                "Can not deliver message:[body:{}, properties: {}] to any"
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
                raise ExchangeNotFoundException(
                    "Attempt to send message to not existing exchange "
                    "detected, message: [body:{}, properties: {}], target: "
                    "[exchange:{}, routing_key:{}]. {}".format(
                        body, properties, exchange, routing_key, str(e)
                    )
                )

            raise ConnectionException(
                "Connectivity problem detected during sending the message: "
                "[body:{}, properties: {}] to target: [exchange:{}, "
                "routing_key:{}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except socket.timeout:
            raise TimeoutConnectionException(
                "Socket timeout exceeded."
            )

    def publish(self, exchange, routing_key, body, properties, confirm,
                mandatory, expiration_time, retrier):
        pool = (self.connection_with_confirmation_pool if confirm else
                self.connection_pool)

        LOG.debug(
            "Sending message:[body:{}; properties: {}] to target: "
            "[exchange:{}; routing_key:{}]".format(
                body, properties, exchange, routing_key
            )
        )

        do_publish = (self._do_publish if retrier is None else
                      retrier(self._do_publish))

        return do_publish(pool, exchange, routing_key, body, properties,
                          mandatory, expiration_time)

    def get_reply_q(self, timeout=None):
        if self._reply_consumer_enabled:
            return self._reply_queue

        with self._reply_consumer_lock:
            if self._reply_consumer_enabled:
                return self._reply_queue

            if self._reply_queue is None:
                self._reply_queue = "reply.{}.{}.{}".format(
                    self.conf.project, self.conf.prog, uuid.uuid4().hex
                )

            if self._reply_listener is None:
                self._reply_listener = RpcReplyPikaListener(
                    pika_engine=self,
                    exchange=self.rpc_reply_exchange,
                    queue=self._reply_queue,
                    no_ack=not self.rpc_reply_listener_ack,
                    prefetch_count=self.rpc_reply_listener_prefetch_count
                )

                self._reply_listener.start(timeout=timeout)

            if self._puller_thread is None:
                self._puller_thread = threading.Thread(target=self._poller)
                self._puller_thread.daemon = True

            if not self._puller_thread.is_alive():
                self._puller_thread.start()

            self._reply_consumer_enabled = True

        return self._reply_queue

    def _poller(self):
        while self._reply_consumer_thread_run_flag:
            try:
                message = self._reply_listener.poll(timeout=1)
                message.acknowledge()
                if message is None:
                    continue
                i = 0
                curtime = time.time()
                while (i < len(self._reply_waiting_future_list) and
                        self._reply_consumer_thread_run_flag):
                    msg_id, future, expiration = (
                        self._reply_waiting_future_list[i]
                    )
                    if expiration and expiration < curtime:
                        del self._reply_waiting_future_list[i]
                    elif msg_id == message.msg_id:
                        del self._reply_waiting_future_list[i]
                        future.set_result(message)
                    else:
                        i += 1
            except BaseException:
                LOG.exception("Exception during reply polling")

    def register_reply_waiter(self, msg_id, future, expiration_time):
        self._reply_waiting_future_list.append(
            (msg_id, future, expiration_time)
        )

    def cleanup(self):
        with self._reply_consumer_lock:
            self._reply_consumer_enabled = False

            if self._puller_thread:
                if self._puller_thread.is_alive():
                    self._reply_consumer_thread_run_flag = False
                    self._puller_thread.join()
                self._puller_thread = None

            if self._reply_listener:
                self._reply_listener.stop()
                self._reply_listener.cleanup()
                self._reply_listener = None

                self._reply_queue = None


class PikaIncomingMessage(object):

    def __init__(self, pika_engine, channel, method, properties, body, no_ack):
        self._pika_engine = pika_engine
        self._no_ack = no_ack
        self._channel = channel
        self.delivery_tag = method.delivery_tag

        self.content_type = getattr(properties, "content_type",
                                    "application/json")
        self.content_encoding = getattr(properties, "content_encoding",
                                        "utf-8")

        self.expiration_time = (
            None if properties.expiration is None else
            time.time() + int(properties.expiration) / 1000
        )

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        message_dict = common.deserialize_msg(
            jsonutils.loads(body, encoding=self.content_encoding)
        )

        self.unique_id = message_dict.pop('_unique_id')
        self.msg_id = message_dict.pop('_msg_id', None)
        self.reply_q = message_dict.pop('_reply_q', None)

        context_dict = {}

        for key in list(message_dict.keys()):
            key = six.text_type(key)
            if key.startswith('_context_'):
                value = message_dict.pop(key)
                context_dict[key[9:]] = value

        self.message = message_dict
        self.ctxt = context_dict

    def reply(self, reply=None, failure=None, log_failure=True):
        if not (self.msg_id and self.reply_q):
            return

        if failure:
            failure = common.serialize_remote_exception(failure, log_failure)

        msg = {
            'result': reply,
            'failure': failure,
            '_unique_id': uuid.uuid4().hex,
            '_msg_id': self.msg_id,
            'ending': True
        }

        def on_exception(ex):
            if isinstance(ex, ConnectionException):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = retrying.retry(
            stop_max_attempt_number=(
                None if self._pika_engine.rpc_reply_retry_attempts == -1
                else self._pika_engine.rpc_reply_retry_attempts
            ),
            retry_on_exception=on_exception,
            wait_fixed=self._pika_engine.rpc_reply_retry_delay * 1000,
        )

        try:
            self._pika_engine.publish(
                exchange=self._pika_engine.rpc_reply_exchange,
                routing_key=self.reply_q,
                body=jsonutils.dumps(
                    common.serialize_msg(msg),
                    encoding=self.content_encoding
                ),
                properties=pika_spec.BasicProperties(
                    content_encoding=self.content_encoding,
                    content_type=self.content_type,
                ),
                confirm=True,
                mandatory=False,
                expiration_time=self.expiration_time,
                retrier=retrier
            )
            LOG.debug(
                "Message [id:'{}'] replied to '{}'.".format(
                    self.msg_id, self.reply_q
                )
            )
        except Exception:
            LOG.exception(
                "Message [id:'{}'] wasn't replied to : {}".format(
                    self.msg_id, self.reply_q
                )
            )

    def acknowledge(self):
        if not self._no_ack:
            self._channel.basic_ack(delivery_tag=self.delivery_tag)

    def requeue(self):
        if not self._no_ack:
            return self._channel.basic_nack(delivery_tag=self.delivery_tag,
                                            requeue=True)


class PikaOutgoingMessage(object):

    def __init__(self, pika_engine, message, context,
                 content_type="application/json", content_encoding="utf-8"):
        self._pika_engine = pika_engine

        self.content_type = content_type
        self.content_encoding = content_encoding

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        self.message = message
        self.context = context

        self.unique_id = uuid.uuid4().hex
        self.msg_id = None

    def send(self, exchange, routing_key='', confirm=True,
             wait_for_reply=False, mandatory=True, persistent=False,
             timeout=None, retrier=None):
        msg = self.message.copy()

        msg['_unique_id'] = self.unique_id

        for key, value in self.context.iteritems():
            key = six.text_type(key)
            msg['_context_' + key] = value

        properties = pika_spec.BasicProperties(
            content_encoding=self.content_encoding,
            content_type=self.content_type,
            delivery_mode=2 if persistent else 1
        )

        expiration_time = (
            None if timeout is None else (timeout + time.time())
        )

        if wait_for_reply:
            self.msg_id = uuid.uuid4().hex
            msg['_msg_id'] = self.msg_id
            LOG.debug('MSG_ID is %s', self.msg_id)

            msg['_reply_q'] = self._pika_engine.get_reply_q(timeout)

            future = futures.Future()

            self._pika_engine.register_reply_waiter(
                msg_id=self.msg_id, future=future,
                expiration_time=expiration_time
            )

        self._pika_engine.publish(
            exchange=exchange, routing_key=routing_key,
            body=jsonutils.dumps(
                common.serialize_msg(msg),
                encoding=self.content_encoding
            ),
            properties=properties,
            confirm=confirm,
            mandatory=mandatory,
            expiration_time=expiration_time,
            retrier=retrier
        )

        if wait_for_reply:
            try:
                return future.result(timeout)
            except futures.TimeoutError:
                raise exceptions.MessagingTimeout()


class PikaListener(object):
    def __init__(self, pika_engine, no_ack, prefetch_count):
        self._pika_engine = pika_engine

        self._connection = None
        self._channel = None
        self._lock = threading.Lock()

        self._prefetch_count = prefetch_count
        self._no_ack = no_ack

        self._started = False

        self._message_queue = collections.deque()

    def _reconnect(self):
        self._connection = self._pika_engine.create_connection(
            for_listening=True
        )
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self._prefetch_count)

        self._on_reconnected()

    def _on_reconnected(self):
        raise NotImplementedError(
            "It is base class. Please declare consumers here"
        )

    def _start_consuming(self, queue):
        self._channel.basic_consume(self._on_message_callback,
                                    queue, no_ack=self._no_ack)

    def _on_message_callback(self, unused, method, properties, body):
        self._message_queue.append((self._channel, method, properties, body))

    def _cleanup(self):
        if self._channel:
            try:
                self._channel.close()
            except Exception as ex:
                if not pika_pool.Connection.is_connection_invalidated(ex):
                    LOG.exception("Unexpected error during closing channel")
            self._channel = None

        if self._connection:
            try:
                self._connection.close()
            except Exception as ex:
                if not pika_pool.Connection.is_connection_invalidated(ex):
                    LOG.exception("Unexpected error during closing connection")
            self._connection = None

    def poll(self, timeout=None):
        start = time.time()
        while not self._message_queue:
            with self._lock:
                if not self._started:
                    return None

                try:
                    if self._channel is None:
                        self._reconnect()
                    self._connection.process_data_events()
                except Exception:
                    self._cleanup()
                    raise
            if timeout and time.time() - start > timeout:
                return None

        return self._message_queue.popleft()

    def start(self):
        self._started = True

    def stop(self):
        with self._lock:
            if not self._started:
                return

            self._started = False
            self._cleanup()

    def reconnect(self):
        with self._lock:
            self._cleanup()
            try:
                self._reconnect()
            except Exception:
                self._cleanup()
                raise

    def cleanup(self):
        with self._lock:
            self._cleanup()


class RpcServicePikaListener(PikaListener):
    def __init__(self, pika_engine, target, no_ack, prefetch_count):
        self._target = target

        super(RpcServicePikaListener, self).__init__(
            pika_engine, no_ack=no_ack, prefetch_count=prefetch_count)

    def _on_reconnected(self):
        exchange = (self._target.exchange or
                    self._pika_engine.default_rpc_exchange)
        queue = '{}'.format(self._target.topic)
        server_queue = '{}.{}'.format(queue, self._target.server)

        fanout_exchange = '{}_fanout_{}'.format(
            self._pika_engine.default_rpc_exchange, self._target.topic
        )

        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=exchange, queue=queue, routing_key=queue,
            exchange_type='direct', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )
        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=exchange, queue=server_queue, routing_key=server_queue,
            exchange_type='direct', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )
        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=fanout_exchange, queue=server_queue, routing_key="",
            exchange_type='fanout', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )

        self._start_consuming(queue)
        self._start_consuming(server_queue)

    def poll(self, timeout=None):
        msg = super(RpcServicePikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class RpcReplyPikaListener(PikaListener):
    def __init__(self, pika_engine, exchange, queue, no_ack, prefetch_count):
        self._exchange = exchange
        self._queue = queue

        super(RpcReplyPikaListener, self).__init__(
            pika_engine, no_ack, prefetch_count
        )

    def _on_reconnected(self):
        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=self._exchange, queue=self._queue,
            routing_key=self._queue, exchange_type='direct',
            queue_expiration=queue_expiration, queue_auto_delete=False,
            durable=False
        )
        self._start_consuming(self._queue)

    def start(self, timeout=None):
        super(RpcReplyPikaListener, self).start()

        def on_exception(ex):
            LOG.warn(str(ex))

            return True

        retrier = retrying.retry(
            stop_max_attempt_number=self._pika_engine.rpc_reply_retry_attempts,
            stop_max_delay=timeout * 1000,
            wait_fixed=self._pika_engine.rpc_reply_retry_delay * 1000,
            retry_on_exception=on_exception,
        )

        retrier(self.reconnect)()

    def poll(self, timeout=None):
        msg = super(RpcReplyPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class NotificationPikaListener(PikaListener):
    def __init__(self, pika_engine, targets_and_priorities,
                 queue_name=None, prefetch_count=100):
        self._targets_and_priorities = targets_and_priorities
        self._queue_name = queue_name

        super(NotificationPikaListener, self).__init__(
            pika_engine, no_ack=False, prefetch_count=prefetch_count
        )

    def _on_reconnected(self):
        queues_to_consume = set()
        for target, priority in self._targets_and_priorities:
            routing_key = '%s.%s' % (target.topic, priority)
            queue = self._queue_name or routing_key
            self._pika_engine.declare_queue_binding_by_channel(
                channel=self._channel,
                exchange=(
                    target.exchange or
                    self._pika_engine.default_notification_exchange
                ),
                queue = queue,
                routing_key=routing_key,
                exchange_type='direct',
                queue_expiration=None,
                queue_auto_delete=False,
                durable=self._pika_engine.notification_persistence,
            )
            queues_to_consume.add(queue)

        for queue_to_consume in queues_to_consume:
            self._start_consuming(queue_to_consume)

    def poll(self, timeout=None):
        msg = super(NotificationPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class PikaDriver(object):
    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        if 'eventlet.patcher' in sys.modules:
            import eventlet.patcher
            if eventlet.patcher.is_monkey_patched('select'):
                import select

                try:
                    del select.poll
                except AttributeError:
                    pass

                try:
                    del select.epoll
                except AttributeError:
                    pass

        opt_group = cfg.OptGroup(name='oslo_messaging_pika',
                                 title='Pika driver options')
        conf.register_group(opt_group)
        conf.register_opts(pika_opts, group=opt_group)
        conf.register_opts(pika_pool_opts, group=opt_group)
        conf.register_opts(rpc_opts, group=opt_group)
        conf.register_opts(notification_opts, group=opt_group)

        self.conf = conf
        self._allowed_remote_exmods = allowed_remote_exmods

        self._pika_engine = PikaEngine(conf, url, default_exchange)

    def require_features(self, requeue=False):
        pass

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):

        if retry is None:
            retry = self._pika_engine.default_rpc_retry_attempts

        def on_exception(ex):
            if isinstance(ex, (ConnectionException,
                               exceptions.MessageDeliveryFailure)):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = (
            None if retry == 0 else
            retrying.retry(
                stop_max_attempt_number=(None if retry == -1 else retry),
                retry_on_exception=on_exception,
                wait_fixed=self._pika_engine.rpc_retry_delay * 1000,
            )
        )

        msg = PikaOutgoingMessage(self._pika_engine, message, ctxt)

        if target.fanout:
            return msg.send(
                exchange='{}_fanout_{}'.format(
                    self._pika_engine.default_rpc_exchange, target.topic
                ),
                timeout=timeout, confirm=True, mandatory=False,
                retrier=retrier
            )

        queue = target.topic
        if target.server:
            queue = '{}.{}'.format(queue, target.server)

        reply = msg.send(
            exchange=target.exchange or self._pika_engine.default_rpc_exchange,
            routing_key=queue,
            wait_for_reply=wait_for_reply,
            timeout=timeout,
            confirm=True,
            mandatory=True,
            retrier=retrier
        )

        if reply is not None:
            if reply.message['failure']:
                ex = common.deserialize_remote_exception(
                    reply.message['failure'], self._allowed_remote_exmods
                )
                raise ex

            return reply.message['result']

    def send_notification(self, target, ctxt, message, version, retry=None):
        if retry is None:
            retry = self._pika_engine.default_notification_retry_attempts

        def on_exception(ex):
            if isinstance(ex, (ExchangeNotFoundException, RoutingException)):
                LOG.warn(str(ex))
                try:
                    self._pika_engine.declare_queue_binding(
                        exchange=(
                            target.exchange or
                            self._pika_engine.default_notification_exchange
                        ),
                        queue=target.topic,
                        routing_key=target.topic,
                        exchange_type='direct',
                        queue_expiration=None,
                        queue_auto_delete=False,
                        durable=self._pika_engine.notification_persistence,
                    )
                except ConnectionException as e:
                    LOG.warn(str(e))
                return True
            elif isinstance(ex,
                            (ConnectionException, MessageRejectedException)):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = retrying.retry(
            stop_max_attempt_number=(None if retry == -1 else retry),
            retry_on_exception=on_exception,
            wait_fixed=self._pika_engine.notification_retry_delay * 1000,
        )

        msg = PikaOutgoingMessage(self._pika_engine, message, ctxt)

        return msg.send(
            exchange=(
                target.exchange or
                self._pika_engine.default_notification_exchange
            ),
            routing_key=target.topic,
            wait_for_reply=False,
            confirm=True,
            mandatory=True,
            persistent=self._pika_engine.notification_persistence,
            retrier=retrier
        )

    def listen(self, target):
        listener = RpcServicePikaListener(
            self._pika_engine, target,
            no_ack=not self._pika_engine.rpc_listener_ack,
            prefetch_count=self._pika_engine.rpc_listener_prefetch_count
        )
        listener.start()
        return listener

    def listen_for_notifications(self, targets_and_priorities, pool):
        listener = NotificationPikaListener(self._pika_engine,
                                            targets_and_priorities, pool)
        listener.start()
        return listener

    def cleanup(self):
        self._pika_engine.cleanup()


class PikaDriverCompatibleWithRabbitDriver(PikaDriver):
    """Old RabbitMQ driver creates exchange before sending message.
    In this case if no rpc service listen this exchange message will be sent
    to /dev/null but client will know anything about it. That is strange.
    But for now we need to keep original behaviour
    """
    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        try:
            return super(PikaDriverCompatibleWithRabbitDriver, self).send(
                target=target,
                ctxt=ctxt,
                message=message,
                wait_for_reply=wait_for_reply,
                timeout=timeout,
                retry=retry
            )
        except exceptions.MessageDeliveryFailure:
            if wait_for_reply:
                raise exceptions.MessagingTimeout()
            else:
                return None
