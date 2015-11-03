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

from oslo_log import log as logging

from oslo_messaging import exceptions

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_listener

import pika
from pika import adapters as pika_adapters
from pika import credentials as pika_credentials
from pika import exceptions as pika_exceptions

import pika_pool

import threading
import time
import uuid

LOG = logging.getLogger(__name__)


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
            except pika_drv_exc.HostConnectionNotAllowedException as e:
                LOG.warn(str(e))

            connection_attempts -= 1
            pika_next_connection_num += 1
            pika_next_connection_num %= host_count

        raise pika_drv_exc.EstablishConnectionException(
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
                raise pika_drv_exc.HostConnectionNotAllowedException(
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
            raise pika_drv_exc.ConnectionException(
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
            raise pika_drv_exc.MessageRejectedException(
                "Can not send message: [body: {}], properties: {}] to "
                "target [exchange: {}, routing_key: {}]. {}".format(
                    body, properties, exchange, routing_key, str(e)
                )
            )
        except pika_exceptions.UnroutableError as e:
            raise pika_drv_exc.RoutingException(
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
                self._reply_listener = pika_listener.RpcReplyPikaListener(
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
