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
import logging
import os
import threading
import uuid

from oslo_utils import eventletutils
import pika_pool
from stevedore import driver

from oslo_messaging._drivers import common as drv_cmn
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc

LOG = logging.getLogger(__name__)


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

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        conf = drv_cmn.ConfigOptsProxy(conf, url)
        self.conf = conf
        self.url = url

        self._connection_factory_type = (
            self.conf.oslo_messaging_pika.connection_factory
        )

        self._connection_factory = None
        self._connection_without_confirmation_pool = None
        self._connection_with_confirmation_pool = None
        self._pid = None
        self._init_lock = threading.Lock()

        self.host_connection_reconnect_delay = (
            conf.oslo_messaging_pika.host_connection_reconnect_delay
        )

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

        self.default_content_type = (
            'application/' + conf.oslo_messaging_pika.default_serializer_type
        )

    def _init_if_needed(self):
        cur_pid = os.getpid()

        if self._pid == cur_pid:
            return

        with self._init_lock:
            if self._pid == cur_pid:
                return

            if self._pid:
                LOG.warning("New pid is detected. Old: %s, new: %s. "
                            "Cleaning up...", self._pid, cur_pid)

            # Note(dukhlov): we need to force select poller usage in case
            # when 'thread' module is monkey patched becase current
            # eventlet implementation does not support patching of
            # poll/epoll/kqueue
            if eventletutils.is_monkey_patched("thread"):
                from pika.adapters import select_connection
                select_connection.SELECT_TYPE = "select"

            mgr = driver.DriverManager(
                'oslo.messaging.pika.connection_factory',
                self._connection_factory_type
            )

            self._connection_factory = mgr.driver(self.url, self.conf)

            # initializing 2 connection pools: 1st for connections without
            # confirmations, 2nd - with confirmations
            self._connection_without_confirmation_pool = pika_pool.QueuedPool(
                create=self.create_connection,
                max_size=self.conf.oslo_messaging_pika.pool_max_size,
                max_overflow=self.conf.oslo_messaging_pika.pool_max_overflow,
                timeout=self.conf.oslo_messaging_pika.pool_timeout,
                recycle=self.conf.oslo_messaging_pika.pool_recycle,
                stale=self.conf.oslo_messaging_pika.pool_stale,
            )

            self._connection_with_confirmation_pool = pika_pool.QueuedPool(
                create=self.create_connection,
                max_size=self.conf.oslo_messaging_pika.pool_max_size,
                max_overflow=self.conf.oslo_messaging_pika.pool_max_overflow,
                timeout=self.conf.oslo_messaging_pika.pool_timeout,
                recycle=self.conf.oslo_messaging_pika.pool_recycle,
                stale=self.conf.oslo_messaging_pika.pool_stale,
            )

            self._connection_with_confirmation_pool.Connection = (
                _PooledConnectionWithConfirmations
            )

            self._pid = cur_pid

    def create_connection(self, for_listening=False):
        self._init_if_needed()
        return self._connection_factory.create_connection(for_listening)

    @property
    def connection_without_confirmation_pool(self):
        self._init_if_needed()
        return self._connection_without_confirmation_pool

    @property
    def connection_with_confirmation_pool(self):
        self._init_if_needed()
        return self._connection_with_confirmation_pool

    def cleanup(self):
        if self._connection_factory:
            self._connection_factory.cleanup()

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
