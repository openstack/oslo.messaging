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
import os
import random
import socket
import threading
import time

from oslo_log import log as logging
import pika
from pika import credentials as pika_credentials

import pika_pool
import uuid

from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_connection
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc

LOG = logging.getLogger(__name__)

_PID = None


class _PooledConnectionWithConfirmations(pika_pool.Connection):
    """Derived from 'pika_pool.Connection' and extends its logic - adds
    'confirm_delivery' call after channel creation to enable delivery
    confirmation for channel
    """
    @property
    def channel(self):
        if self.fairy.channel is None:
            self.fairy.channel = self.fairy.cxn.channel()
            self.fairy.channel.confirm_delivery()
        return self.fairy.channel


class PikaEngine(object):
    """Used for shared functionality between other pika driver modules, like
    connection factory, connection pools, processing and holding configuration,
    etc.
    """

    # constants for creating connection statistics
    HOST_CONNECTION_LAST_TRY_TIME = "last_try_time"
    HOST_CONNECTION_LAST_SUCCESS_TRY_TIME = "last_success_try_time"

    # constant for setting tcp_user_timeout socket option
    # (it should be defined in 'select' module of standard library in future)
    TCP_USER_TIMEOUT = 18

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        self.conf = conf

        # processing rpc options
        self.default_rpc_exchange = (
            conf.oslo_messaging_pika.default_rpc_exchange
        )
        self.rpc_reply_exchange = (
            conf.oslo_messaging_pika.rpc_reply_exchange
        )

        self.allowed_remote_exmods = [pika_drv_cmns.EXCEPTIONS_MODULE]
        if allowed_remote_exmods:
            self.allowed_remote_exmods.extend(allowed_remote_exmods)

        self.rpc_listener_prefetch_count = (
            conf.oslo_messaging_pika.rpc_listener_prefetch_count
        )

        self.default_rpc_retry_attempts = (
            conf.oslo_messaging_pika.default_rpc_retry_attempts
        )

        self.rpc_retry_delay = (
            conf.oslo_messaging_pika.rpc_retry_delay
        )
        if self.rpc_retry_delay < 0:
            raise ValueError("rpc_retry_delay should be non-negative integer")

        self.rpc_reply_listener_prefetch_count = (
            conf.oslo_messaging_pika.rpc_listener_prefetch_count
        )

        self.rpc_reply_retry_attempts = (
            conf.oslo_messaging_pika.rpc_reply_retry_attempts
        )
        self.rpc_reply_retry_delay = (
            conf.oslo_messaging_pika.rpc_reply_retry_delay
        )
        if self.rpc_reply_retry_delay < 0:
            raise ValueError("rpc_reply_retry_delay should be non-negative "
                             "integer")

        self.rpc_queue_expiration = (
            self.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        # processing notification options
        self.default_notification_exchange = (
            conf.oslo_messaging_pika.default_notification_exchange
        )

        self.notification_persistence = (
            conf.oslo_messaging_pika.notification_persistence
        )

        self.notification_listener_prefetch_count = (
            conf.oslo_messaging_pika.notification_listener_prefetch_count
        )

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

        self._tcp_user_timeout = self.conf.oslo_messaging_pika.tcp_user_timeout
        self.host_connection_reconnect_delay = (
            self.conf.oslo_messaging_pika.host_connection_reconnect_delay
        )
        self._heartbeat_interval = (
            self.conf.oslo_messaging_pika.heartbeat_interval
        )

        # initializing connection parameters for configured RabbitMQ hosts
        self._common_pika_params = {
            'virtual_host': url.virtual_host,
            'channel_max': self.conf.oslo_messaging_pika.channel_max,
            'frame_max': self.conf.oslo_messaging_pika.frame_max,
            'ssl': self.conf.oslo_messaging_pika.ssl,
            'ssl_options': self.conf.oslo_messaging_pika.ssl_options,
            'socket_timeout': self.conf.oslo_messaging_pika.socket_timeout,
        }

        self._connection_lock = threading.RLock()
        self._pid = None

        self._connection_host_status = {}

        if not url.hosts:
            raise ValueError("You should provide at least one RabbitMQ host")

        self._host_list = url.hosts

        self._cur_connection_host_num = random.randint(
            0, len(self._host_list) - 1
        )

        # initializing 2 connection pools: 1st for connections without
        # confirmations, 2nd - with confirmations
        self.connection_without_confirmation_pool = pika_pool.QueuedPool(
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
            _PooledConnectionWithConfirmations
        )

    def create_connection(self, for_listening=False):
        """Create and return connection to any available host.

        :return: created connection
        :raise: ConnectionException if all hosts are not reachable
        """

        with self._connection_lock:
            self._init_if_needed()

            host_count = len(self._host_list)
            connection_attempts = host_count

            while connection_attempts > 0:
                self._cur_connection_host_num += 1
                self._cur_connection_host_num %= host_count
                try:
                    return self.create_host_connection(
                        self._cur_connection_host_num, for_listening
                    )
                except pika_pool.Connection.connectivity_errors as e:
                    LOG.warning("Can't establish connection to host. %s", e)
                except pika_drv_exc.HostConnectionNotAllowedException as e:
                    LOG.warning("Connection to host is not allowed. %s", e)

                connection_attempts -= 1

            raise pika_drv_exc.EstablishConnectionException(
                "Can not establish connection to any configured RabbitMQ "
                "host: " + str(self._host_list)
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
            LOG.warning(
                "Whoops, this kernel doesn't seem to support TCP_USER_TIMEOUT."
            )

    def _init_if_needed(self):
        global _PID

        cur_pid = os.getpid()

        if _PID != cur_pid:
            if _PID:
                LOG.warning("New pid is detected. Old: %s, new: %s. "
                            "Cleaning up...", _PID, cur_pid)
            # Note(dukhlov): we need to force select poller usage in case when
            # 'thread' module is monkey patched becase current eventlet
            # implementation does not support patching of poll/epoll/kqueue
            if pika_drv_cmns.is_eventlet_monkey_patched("thread"):
                from pika.adapters import select_connection
                select_connection.SELECT_TYPE = "select"

            _PID = cur_pid

    def create_host_connection(self, host_index, for_listening=False):
        """Create new connection to host #host_index

        :param host_index: Integer, number of host for connection establishing
        :param for_listening: Boolean, creates connection for listening
            if True
        :return: New connection
        """
        with self._connection_lock:
            self._init_if_needed()

            host = self._host_list[host_index]

            connection_params = pika.ConnectionParameters(
                host=host.hostname,
                port=host.port,
                credentials=pika_credentials.PlainCredentials(
                    host.username, host.password
                ),
                heartbeat_interval=(
                    self._heartbeat_interval if for_listening else None
                ),
                **self._common_pika_params
            )

            cur_time = time.time()

            host_connection_status = self._connection_host_status.get(host)

            if host_connection_status is None:
                host_connection_status = {
                    self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME: 0,
                    self.HOST_CONNECTION_LAST_TRY_TIME: 0
                }
                self._connection_host_status[host] = host_connection_status

            last_success_time = host_connection_status[
                self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
            ]
            last_time = host_connection_status[
                self.HOST_CONNECTION_LAST_TRY_TIME
            ]

            # raise HostConnectionNotAllowedException if we tried to establish
            # connection in last 'host_connection_reconnect_delay' and got
            # failure
            if (last_time != last_success_time and
                    cur_time - last_time <
                    self.host_connection_reconnect_delay):
                raise pika_drv_exc.HostConnectionNotAllowedException(
                    "Connection to host #{} is not allowed now because of "
                    "previous failure".format(host_index)
                )

            try:
                if for_listening:
                    connection = pika_connection.ThreadSafePikaConnection(
                        params=connection_params
                    )
                else:
                    connection = pika.BlockingConnection(
                        parameters=connection_params
                    )
                    connection.params = connection_params

                self._set_tcp_user_timeout(connection._impl.socket)

                self._connection_host_status[host][
                    self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
                ] = cur_time

                return connection
            finally:
                self._connection_host_status[host][
                    self.HOST_CONNECTION_LAST_TRY_TIME
                ] = cur_time

    def declare_exchange_by_channel(self, channel, exchange, exchange_type,
                                    durable):
        """Declare exchange using already created channel, if they don't exist

        :param channel: Channel for communication with RabbitMQ
        :param exchange:  String, RabbitMQ exchange name
        :param exchange_type: String ('direct', 'topic' or 'fanout')
            exchange type for exchange to be declared
        :param durable: Boolean, creates durable exchange if true
        """
        try:
            channel.exchange_declare(
                exchange, exchange_type, auto_delete=True, durable=durable
            )
        except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as e:
            raise pika_drv_exc.ConnectionException(
                "Connectivity problem detected during declaring exchange: "
                "exchange:{}, exchange_type: {}, durable: {}. {}".format(
                    exchange, exchange_type, durable, str(e)
                )
            )

    def declare_queue_binding_by_channel(self, channel, exchange, queue,
                                         routing_key, exchange_type,
                                         queue_expiration, durable):
        """Declare exchange, queue and bind them using already created
        channel, if they don't exist

        :param channel: Channel for communication with RabbitMQ
        :param exchange:  String, RabbitMQ exchange name
        :param queue: Sting, RabbitMQ queue name
        :param routing_key: Sting, RabbitMQ routing key for queue binding
        :param exchange_type: String ('direct', 'topic' or 'fanout')
            exchange type for exchange to be declared
        :param queue_expiration: Integer, time in seconds which queue will
            remain existing in RabbitMQ when there no consumers connected
        :param durable: Boolean, creates durable exchange and queue if true
        """
        try:
            channel.exchange_declare(
                exchange, exchange_type, auto_delete=True, durable=durable
            )
            arguments = {}

            if queue_expiration > 0:
                arguments['x-expires'] = queue_expiration * 1000

            channel.queue_declare(queue, durable=durable, arguments=arguments)

            channel.queue_bind(queue, exchange, routing_key)
        except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as e:
            raise pika_drv_exc.ConnectionException(
                "Connectivity problem detected during declaring queue "
                "binding: exchange:{}, queue: {}, routing_key: {}, "
                "exchange_type: {}, queue_expiration: {}, "
                "durable: {}. {}".format(
                    exchange, queue, routing_key, exchange_type,
                    queue_expiration, durable, str(e)
                )
            )

    def get_rpc_exchange_name(self, exchange):
        """Returns RabbitMQ exchange name for given rpc request

        :param exchange: String, oslo.messaging target's exchange

        :return: String, RabbitMQ exchange name
        """
        return exchange or self.default_rpc_exchange

    @staticmethod
    def get_rpc_queue_name(topic, server, no_ack, worker=False):
        """Returns RabbitMQ queue name for given rpc request

        :param topic: String, oslo.messaging target's topic
        :param server: String, oslo.messaging target's server
        :param no_ack: Boolean, use message delivery with acknowledges or not
        :param worker: Boolean, use queue by single worker only or not

        :return: String, RabbitMQ queue name
        """
        queue_parts = ["no_ack" if no_ack else "with_ack", topic]
        if server is not None:
            queue_parts.append(server)
        if worker:
            queue_parts.append("worker")
            queue_parts.append(uuid.uuid4().hex)
        queue = '.'.join(queue_parts)
        return queue
