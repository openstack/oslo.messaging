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

import random
import socket
import sys
import threading
import time

from oslo_log import log as logging
import pika
from pika.adapters import select_connection
from pika import credentials as pika_credentials
import pika_pool
import six

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc

LOG = logging.getLogger(__name__)

_EXCEPTIONS_MODULE = 'exceptions' if six.PY2 else 'builtins'


def _is_eventlet_monkey_patched(module):
    """Determines safely is eventlet patching for module enabled or not

    :param module: String, module name
    :return Bool, True if module is pathed, False otherwise
    """

    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched(module)


def _create_select_poller_connection_impl(
        parameters, on_open_callback, on_open_error_callback,
        on_close_callback, stop_ioloop_on_close):
    """Used for disabling autochoise of poller ('select', 'poll', 'epool', etc)
    inside default 'SelectConnection.__init__(...)' logic. It is necessary to
    force 'select' poller usage if eventlet is monkeypatched because eventlet
    patches only 'select' system call

    Method signature is copied form 'SelectConnection.__init__(...)', because
    it is used as replacement of 'SelectConnection' class to create instances
    """
    return select_connection.SelectConnection(
        parameters=parameters,
        on_open_callback=on_open_callback,
        on_open_error_callback=on_open_error_callback,
        on_close_callback=on_close_callback,
        stop_ioloop_on_close=stop_ioloop_on_close,
        custom_ioloop=select_connection.SelectPoller()
    )


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

        self._force_select_poller_use = _is_eventlet_monkey_patched('select')

        # processing rpc options
        self.default_rpc_exchange = (
            conf.oslo_messaging_pika.default_rpc_exchange
        )
        self.rpc_reply_exchange = (
            conf.oslo_messaging_pika.rpc_reply_exchange
        )

        self.allowed_remote_exmods = [_EXCEPTIONS_MODULE]
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

        if not url.hosts:
            raise ValueError("You should provide at least one RabbitMQ host")

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

        self._next_connection_host_num = random.randint(
            0, len(self._connection_host_param_list) - 1
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

    def _next_connection_num(self):
        """Used for creating connections to different RabbitMQ nodes in
        round robin order

        :return: next host number to create connection to
        """
        with self._connection_lock:
            cur_num = self._next_connection_host_num
            self._next_connection_host_num += 1
            self._next_connection_host_num %= len(
                self._connection_host_param_list
            )
        return cur_num

    def create_connection(self, for_listening=False):
        """Create and return connection to any available host.

        :return: created connection
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
                LOG.warn("Can't establish connection to host. %s", e)
            except pika_drv_exc.HostConnectionNotAllowedException as e:
                LOG.warn("Connection to host is not Allowed. %s", e)

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
        :param host_index: Integer, number of host for connection establishing
        :param for_listening: Boolean, creates connection for listening
            (enable heartbeats) if True
        :return: New connection
        """

        connection_params = pika.ConnectionParameters(
            heartbeat_interval=(
                self._heartbeat_interval if for_listening else None
            ),
            **self._connection_host_param_list[host_index]
        )

        with self._connection_lock:
            cur_time = time.time()

            last_success_time = self._connection_host_status_list[host_index][
                self.HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
            ]
            last_time = self._connection_host_status_list[host_index][
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
                connection = pika.BlockingConnection(
                    parameters=connection_params,
                    _impl_class=(_create_select_poller_connection_impl
                                 if self._force_select_poller_use else None)
                )

                # It is needed for pika_pool library which expects that
                # connections has params attribute defined in BaseConnection
                # but BlockingConnection is not derived from BaseConnection
                # and doesn't have it
                connection.params = connection_params

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
                                         durable):
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
        except pika_pool.Connection.connectivity_errors as e:
            raise pika_drv_exc.ConnectionException(
                "Connectivity problem detected during declaring queue "
                "binding: exchange:{}, queue: {}, routing_key: {}, "
                "exchange_type: {}, queue_expiration: {}, "
                "durable: {}. {}".format(
                    exchange, queue, routing_key, exchange_type,
                    queue_expiration, durable, str(e)
                )
            )

    def get_rpc_exchange_name(self, exchange, topic, fanout, no_ack):
        """Returns RabbitMQ exchange name for given rpc request

        :param exchange: String, oslo.messaging target's exchange
        :param topic: String, oslo.messaging target's topic
        :param fanout: Boolean, oslo.messaging target's fanout mode
        :param no_ack: Boolean, use message delivery with acknowledges or not

        :return: String, RabbitMQ exchange name
        """
        exchange = (exchange or self.default_rpc_exchange)

        if fanout:
            exchange = '{}_fanout_{}_{}'.format(
                exchange, "no_ack" if no_ack else "with_ack", topic
            )
        return exchange

    @staticmethod
    def get_rpc_queue_name(topic, server, no_ack):
        """Returns RabbitMQ queue name for given rpc request

        :param topic: String, oslo.messaging target's topic
        :param server: String, oslo.messaging target's server
        :param no_ack: Boolean, use message delivery with acknowledges or not

        :return: String, RabbitMQ exchange name
        """
        queue_parts = ["no_ack" if no_ack else "with_ack", topic]
        if server is not None:
            queue_parts.append(server)
        queue = '.'.join(queue_parts)
        return queue
