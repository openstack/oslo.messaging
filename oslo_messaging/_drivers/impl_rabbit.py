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
import contextlib
import errno
import functools
import itertools
import math
import os
import random
import socket
import ssl
import sys
import threading
import time
from urllib import parse
import uuid

from amqp import exceptions as amqp_ex
import kombu
import kombu.connection
import kombu.entity
import kombu.messaging
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import eventletutils
from oslo_utils import netutils

import oslo_messaging
from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers import pool
from oslo_messaging import _utils
from oslo_messaging import exceptions


# The QuorumMemConfig will hold the quorum queue memory configurations
QuorumMemConfig = collections.namedtuple('QuorumMemConfig',
                                         'delivery_limit'
                                         ' max_memory_length'
                                         ' max_memory_bytes')

# NOTE(sileht): don't exist in py2 socket module
TCP_USER_TIMEOUT = 18

rabbit_opts = [
    cfg.IntOpt('rpc_conn_pool_size', default=30,
               help='Size of RPC connection pool.',
               min=1),
    cfg.IntOpt('conn_pool_min_size', default=2,
               help='The pool size limit for connections expiration policy'),
    cfg.IntOpt('conn_pool_ttl', default=1200,
               help='The time-to-live in sec of idle connections in the pool'),
    cfg.BoolOpt('ssl',
                default=False,
                help='Connect over SSL.'),
    cfg.StrOpt('ssl_version',
               default='',
               help='SSL version to use (valid only if SSL enabled). '
                    'Valid values are TLSv1 and SSLv23. SSLv2, SSLv3, '
                    'TLSv1_1, and TLSv1_2 may be available on some '
                    'distributions.'
               ),
    cfg.StrOpt('ssl_key_file',
               default='',
               help='SSL key file (valid only if SSL enabled).'),
    cfg.StrOpt('ssl_cert_file',
               default='',
               help='SSL cert file (valid only if SSL enabled).'),
    cfg.StrOpt('ssl_ca_file',
               default='',
               help='SSL certification authority file '
                    '(valid only if SSL enabled).'),
    cfg.BoolOpt('ssl_enforce_fips_mode',
                default=False,
                help='Global toggle for enforcing the OpenSSL FIPS mode. '
                'This feature requires Python support. '
                'This is available in Python 3.9 in all '
                'environments and may have been backported to older '
                'Python versions on select environments. If the Python '
                'executable used does not support OpenSSL FIPS mode, '
                'an exception will be raised.'),
    cfg.BoolOpt('heartbeat_in_pthread',
                default=False,
                deprecated_for_removal=True,
                deprecated_reason='The option is related to Eventlet which '
                                  'will be removed. In addition this has '
                                  'never worked as expected with services '
                                  'using eventlet for core service framework.',
                help="(DEPRECATED) It is recommend not to use this option "
                     "anymore. Run the health check heartbeat thread "
                     "through a native python thread by default. If this "
                     "option is equal to False then the health check "
                     "heartbeat will inherit the execution model "
                     "from the parent process. For "
                     "example if the parent process has monkey patched the "
                     "stdlib by using eventlet/greenlet then the heartbeat "
                     "will be run through a green thread. "
                     "This option should be set to True only for the "
                     "wsgi services.",
                ),
    cfg.FloatOpt('kombu_reconnect_delay',
                 default=1.0,
                 min=0.0,
                 max=amqpdriver.ACK_REQUEUE_EVERY_SECONDS_MAX * 0.9,
                 help='How long to wait (in seconds) before reconnecting in '
                      'response to an AMQP consumer cancel notification.'),
    cfg.FloatOpt('kombu_reconnect_splay',
                 default=0.0,
                 min=0.0,
                 help='Random time to wait for when reconnecting in response '
                      'to an AMQP consumer cancel notification.'),
    cfg.StrOpt('kombu_compression',
               help="EXPERIMENTAL: Possible values are: gzip, bz2. If not "
                    "set compression will not be used. This option may not "
                    "be available in future versions."),
    cfg.IntOpt('kombu_missing_consumer_retry_timeout',
               deprecated_name="kombu_reconnect_timeout",
               default=60,
               help='How long to wait a missing client before abandoning to '
                    'send it its replies. This value should not be longer '
                    'than rpc_response_timeout.'),
    cfg.StrOpt('kombu_failover_strategy',
               choices=('round-robin', 'shuffle'),
               default='round-robin',
               help='Determines how the next RabbitMQ node is chosen in case '
                    'the one we are currently connected to becomes '
                    'unavailable. Takes effect only if more than one '
                    'RabbitMQ node is provided in config.'),
    cfg.StrOpt('rabbit_login_method',
               choices=('PLAIN', 'AMQPLAIN', 'EXTERNAL', 'RABBIT-CR-DEMO'),
               default='AMQPLAIN',
               help='The RabbitMQ login method.'),
    cfg.IntOpt('rabbit_retry_interval',
               min=1,
               default=1,
               help='How frequently to retry connecting with RabbitMQ.'),
    cfg.IntOpt('rabbit_retry_backoff',
               default=2,
               min=0,
               help='How long to backoff for between retries when connecting '
                    'to RabbitMQ.'),
    cfg.IntOpt('rabbit_interval_max',
               default=30,
               min=1,
               help='Maximum interval of RabbitMQ connection retries.'),
    cfg.BoolOpt('rabbit_ha_queues',
                default=False,
                help='Try to use HA queues in RabbitMQ (x-ha-policy: all). '
                'If you change this option, you must wipe the RabbitMQ '
                'database. In RabbitMQ 3.0, queue mirroring is no longer '
                'controlled by the x-ha-policy argument when declaring a '
                'queue. If you just want to make sure that all queues (except '
                'those with auto-generated names) are mirrored across all '
                'nodes, run: '
                """\"rabbitmqctl set_policy HA '^(?!amq\\.).*' """
                """'{"ha-mode": "all"}' \""""),
    cfg.BoolOpt('rabbit_quorum_queue',
                default=False,
                help='Use quorum queues in RabbitMQ (x-queue-type: quorum). '
                'The quorum queue is a modern queue type for RabbitMQ '
                'implementing a durable, replicated FIFO queue based on the '
                'Raft consensus algorithm. It is available as of '
                'RabbitMQ 3.8.0. If set this option will conflict with '
                'the HA queues (``rabbit_ha_queues``) aka mirrored queues, '
                'in other words the HA queues should be disabled. '
                'Quorum queues are also durable by default so the '
                'amqp_durable_queues option is ignored when this option is '
                'enabled.'),
    cfg.BoolOpt('rabbit_transient_quorum_queue',
                default=False,
                help='Use quorum queues for transients queues in RabbitMQ. '
                'Enabling this option will then make sure those queues are '
                'also using quorum kind of rabbit queues, which are HA by '
                'default.'),
    cfg.IntOpt('rabbit_quorum_delivery_limit',
               default=0,
               help='Each time a message is redelivered to a consumer, '
               'a counter is incremented. Once the redelivery count '
               'exceeds the delivery limit the message gets dropped '
               'or dead-lettered (if a DLX exchange has been configured) '
               'Used only when rabbit_quorum_queue is enabled, '
               'Default 0 which means dont set a limit.'),
    cfg.IntOpt('rabbit_quorum_max_memory_length',
               deprecated_name='rabbit_quroum_max_memory_length',
               default=0,
               help='By default all messages are maintained in memory '
               'if a quorum queue grows in length it can put memory '
               'pressure on a cluster. This option can limit the number '
               'of messages in the quorum queue. '
               'Used only when rabbit_quorum_queue is enabled, '
               'Default 0 which means dont set a limit.'),
    cfg.IntOpt('rabbit_quorum_max_memory_bytes',
               deprecated_name='rabbit_quroum_max_memory_bytes',
               default=0,
               help='By default all messages are maintained in memory '
               'if a quorum queue grows in length it can put memory '
               'pressure on a cluster. This option can limit the number '
               'of memory bytes used by the quorum queue. '
               'Used only when rabbit_quorum_queue is enabled, '
               'Default 0 which means dont set a limit.'),
    cfg.IntOpt('rabbit_transient_queues_ttl',
               min=0,
               default=1800,
               help='Positive integer representing duration in seconds for '
                    'queue TTL (x-expires). Queues which are unused for the '
                    'duration of the TTL are automatically deleted. The '
                    'parameter affects only reply and fanout queues. Setting '
                    '0 as value will disable the x-expires. If doing so, '
                    'make sure you have a rabbitmq policy to delete the '
                    'queues or you deployment will create an infinite number '
                    'of queue over time.'
                    'In case rabbit_stream_fanout is set to True, this option '
                    'will control data retention policy (x-max-age) for '
                    'messages in the fanout queue rather then the queue '
                    'duration itself. So the oldest data in the stream queue '
                    'will be discarded from it once reaching TTL '
                    'Setting to 0 will disable x-max-age for stream which '
                    'make stream grow indefinitely filling up the diskspace'),
    cfg.IntOpt('rabbit_qos_prefetch_count',
               default=0,
               help='Specifies the number of messages to prefetch. Setting to '
                    'zero allows unlimited messages.'),
    cfg.IntOpt('heartbeat_timeout_threshold',
               default=60,
               help="Number of seconds after which the Rabbit broker is "
               "considered down if heartbeat's keep-alive fails "
               "(0 disables heartbeat)."),
    cfg.IntOpt('heartbeat_rate',
               default=3,
               help='How often times during the heartbeat_timeout_threshold '
               'we check the heartbeat.'),
    cfg.BoolOpt('direct_mandatory_flag',
                default=True,
                deprecated_for_removal=True,
                deprecated_reason='Mandatory flag no longer deactivable.',
                help='(DEPRECATED) Enable/Disable the RabbitMQ mandatory '
                'flag for direct send. The direct send is used as reply, '
                'so the MessageUndeliverable exception is raised '
                'in case the client queue does not exist.'
                'MessageUndeliverable exception will be used to loop for a '
                'timeout to lets a chance to sender to recover.'
                'This flag is deprecated and it will not be possible to '
                'deactivate this functionality anymore'),
    cfg.BoolOpt('enable_cancel_on_failover',
                default=False,
                help="Enable x-cancel-on-ha-failover flag so that "
                     "rabbitmq server will cancel and notify consumers"
                     "when queue is down"),
    cfg.BoolOpt('use_queue_manager',
                default=False,
                help='Should we use consistant queue names or random ones'),
    cfg.StrOpt('hostname',
               sample_default='node1.example.com',
               default=socket.gethostname(),
               help='Hostname used by queue manager. Defaults to the value '
               'returned by socket.gethostname().'),
    cfg.StrOpt('processname',
               sample_default='nova-api',
               default=os.path.basename(sys.argv[0]),
               help='Process name used by queue manager'),
    cfg.BoolOpt('rabbit_stream_fanout',
                default=False,
                help='Use stream queues in RabbitMQ (x-queue-type: stream). '
                'Streams are a new persistent and replicated data structure '
                '("queue type") in RabbitMQ which models an append-only log '
                'with non-destructive consumer semantics. It is available '
                'as of RabbitMQ 3.9.0. If set this option will replace all '
                'fanout queues with only one stream queue.'),
]

LOG = logging.getLogger(__name__)


def _get_queue_arguments(rabbit_ha_queues, rabbit_queue_ttl,
                         rabbit_quorum_queue,
                         rabbit_quorum_queue_config,
                         rabbit_stream_fanout):
    """Construct the arguments for declaring a queue.

    If the rabbit_ha_queues option is set, we try to declare a mirrored queue
    as described here:

      http://www.rabbitmq.com/ha.html

    Setting x-ha-policy to all means that the queue will be mirrored
    to all nodes in the cluster. In RabbitMQ 3.0, queue mirroring is
    no longer controlled by the x-ha-policy argument when declaring a
    queue. If you just want to make sure that all queues (except those
    with auto-generated names) are mirrored across all nodes, run:
      rabbitmqctl set_policy HA '^(?!amq\\.).*' '{"ha-mode": "all"}'

    If the rabbit_queue_ttl option is > 0, then the queue is
    declared with the "Queue TTL" value as described here:

      https://www.rabbitmq.com/ttl.html

    Setting a queue TTL causes the queue to be automatically deleted
    if it is unused for the TTL duration.  This is a helpful safeguard
    to prevent queues with zero consumers from growing without bound.

    If the rabbit_quorum_queue option is set, we try to declare a mirrored
    queue as described here:

      https://www.rabbitmq.com/quorum-queues.html

    Setting x-queue-type to quorum means that replicated FIFO queue based on
    the Raft consensus algorithm will be used. It is available as of
    RabbitMQ 3.8.0. If set this option will conflict with
    the HA queues (``rabbit_ha_queues``) aka mirrored queues,
    in other words HA queues should be disabled.

    rabbit_quorum_queue_config:
    Quorum queues provides three options to handle message poisoning
    and limit the resources the quorum queue can use
    x-delivery-limit number of times the queue will try to deliver
    a message before it decide to discard it
    x-max-in-memory-length, x-max-in-memory-bytes control the size
    of memory used by quorum queue

    If the rabbit_stream_fanout option is set, fanout queues are going to use
    stream instead of quorum queues. See here:
      https://www.rabbitmq.com/streams.html
    """
    args = {}

    if rabbit_quorum_queue and rabbit_ha_queues:
        raise RuntimeError('Configuration Error: rabbit_quorum_queue '
                           'and rabbit_ha_queues both enabled, queue '
                           'type is quorum or HA (mirrored) not both')

    if rabbit_ha_queues:
        args['x-ha-policy'] = 'all'

    if rabbit_quorum_queue:
        args['x-queue-type'] = 'quorum'
        if rabbit_quorum_queue_config.delivery_limit:
            args['x-delivery-limit'] = \
                rabbit_quorum_queue_config.delivery_limit
        if rabbit_quorum_queue_config.max_memory_length:
            args['x-max-in-memory-length'] = \
                rabbit_quorum_queue_config.max_memory_length
        if rabbit_quorum_queue_config.max_memory_bytes:
            args['x-max-in-memory-bytes'] = \
                rabbit_quorum_queue_config.max_memory_bytes

    if rabbit_queue_ttl > 0:
        args['x-expires'] = rabbit_queue_ttl * 1000

    if rabbit_stream_fanout:
        args = {'x-queue-type': 'stream'}
        if rabbit_queue_ttl > 0:
            # max-age is a string
            args['x-max-age'] = f"{rabbit_queue_ttl}s"

    return args


class RabbitMessage(dict):
    def __init__(self, raw_message):
        super().__init__(
            rpc_common.deserialize_msg(raw_message.payload))
        LOG.trace('RabbitMessage.Init: message %s', self)
        self._raw_message = raw_message

    def acknowledge(self):
        LOG.trace('RabbitMessage.acknowledge: message %s', self)
        self._raw_message.ack()

    def requeue(self):
        LOG.trace('RabbitMessage.requeue: message %s', self)
        self._raw_message.requeue()


class Consumer:
    """Consumer class."""

    def __init__(self, exchange_name, queue_name, routing_key, type, durable,
                 exchange_auto_delete, queue_auto_delete, callback,
                 nowait=False, rabbit_ha_queues=None, rabbit_queue_ttl=0,
                 enable_cancel_on_failover=False, rabbit_quorum_queue=False,
                 rabbit_quorum_queue_config=QuorumMemConfig(0, 0, 0),
                 rabbit_stream_fanout=False):
        """Init the Consumer class with the exchange_name, routing_key,
        type, durable auto_delete
        """
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.exchange_auto_delete = exchange_auto_delete
        self.queue_auto_delete = queue_auto_delete
        self.durable = durable
        self.callback = callback
        self.type = type
        self.nowait = nowait
        rabbit_quorum_queue_config = rabbit_quorum_queue_config or {}
        self.queue_arguments = _get_queue_arguments(
            rabbit_ha_queues, rabbit_queue_ttl, rabbit_quorum_queue,
            rabbit_quorum_queue_config, rabbit_stream_fanout)
        self.queue = None
        self._declared_on = None
        self.exchange = kombu.entity.Exchange(
            name=exchange_name,
            type=self.type,
            durable=self.durable,
            auto_delete=self.exchange_auto_delete)
        self.enable_cancel_on_failover = enable_cancel_on_failover
        self.rabbit_stream_fanout = rabbit_stream_fanout
        self.next_stream_offset = "last"

    def _declare_fallback(self, err, conn, consumer_arguments):
        """Fallback by declaring a non durable queue.

        When a control exchange is shared between services it is possible
        that some service created first a non durable control exchange and
        then after that an other service can try to create the same control
        exchange but as a durable control exchange. In this case RabbitMQ
        will raise an exception (PreconditionFailed), and then it will stop
        our execution and our service will fail entirly. In this case we want
        to fallback by creating a non durable queue to match the default
        config.
        """
        if "PRECONDITION_FAILED - inequivalent arg 'durable'" in str(err):
            LOG.info(
                "[%s] Retrying to declare the exchange (%s) as "
                "non durable", conn.connection_id, self.exchange_name)
            self.exchange = kombu.entity.Exchange(
                name=self.exchange_name,
                type=self.type,
                durable=False,
                auto_delete=self.queue_auto_delete)
            self.queue = kombu.entity.Queue(
                name=self.queue_name,
                channel=conn.channel,
                exchange=self.exchange,
                durable=False,
                auto_delete=self.queue_auto_delete,
                routing_key=self.routing_key,
                queue_arguments=self.queue_arguments,
                consumer_arguments=consumer_arguments
            )
            self.queue.declare()

    def reset_stream_offset(self):
        if not self.rabbit_stream_fanout:
            return
        LOG.warn("Reset consumer for queue %s next offset was at %s.",
                 self.queue_name, self.next_stream_offset)
        self.next_stream_offset = "last"

    def declare(self, conn):
        """Re-declare the queue after a rabbit (re)connect."""

        consumer_arguments = None
        if self.enable_cancel_on_failover:
            consumer_arguments = {
                "x-cancel-on-ha-failover": True}

        if self.rabbit_stream_fanout:
            consumer_arguments = {
                "x-stream-offset": self.next_stream_offset}

        self.queue = kombu.entity.Queue(
            name=self.queue_name,
            channel=conn.channel,
            exchange=self.exchange,
            durable=self.durable,
            auto_delete=self.queue_auto_delete,
            routing_key=self.routing_key,
            queue_arguments=self.queue_arguments,
            consumer_arguments=consumer_arguments
        )

        try:
            if self.rabbit_stream_fanout:
                LOG.info('[%s] Stream Queue.declare: %s after offset %s',
                         conn.connection_id, self.queue_name,
                         self.next_stream_offset)
            else:
                LOG.debug('[%s] Queue.declare: %s',
                          conn.connection_id, self.queue_name)
            try:
                self.queue.declare()
            except amqp_ex.PreconditionFailed as err:
                # NOTE(hberaud): This kind of exception may be triggered
                # when a control exchange is shared between services and
                # when services try to create it with configs that differ
                # from each others. RabbitMQ will reject the services
                # that try to create it with a configuration that differ
                # from the one used first.
                LOG.warning('[%s] Queue %s could not be declared probably '
                            'because of conflicting configurations: %s',
                            conn.connection_id, self.queue_name, err)
                self._declare_fallback(err, conn, consumer_arguments)
            except amqp_ex.NotFound as ex:
                # NOTE(viktor.krivak): This exception is raised when
                # non-durable and non-ha queue is hosted on node that
                # is currently unresponsive or down. Thus, it is
                # not possible to redeclare the queue since its status
                # is unknown, and it could disappear in
                # a few moments. However, the queue could be explicitly
                # deleted and the redeclaration will then succeed.
                # Queue will be then scheduled on any of the
                # running/responsive nodes.
                # This fixes bug:
                # https://bugs.launchpad.net/oslo.messaging/+bug/2068630

                LOG.warning("Queue %s is stuck on unresponsive node. "
                            "Trying to delete the queue and redeclare it "
                            "again, Error info: %s", self.queue_name, ex)
                try:
                    self.queue.delete()
                except Exception as in_ex:
                    LOG.warning("During cleanup of stuck queue deletion "
                                "another exception occurred: %s. Ignoring...",
                                in_ex)
                self.queue.declare()

        except conn.connection.channel_errors as exc:
            # NOTE(jrosenboom): This exception may be triggered by a race
            # condition. Simply retrying will solve the error most of the time
            # and should work well enough as a workaround until the race
            # condition itself can be fixed.
            # See https://bugs.launchpad.net/neutron/+bug/1318721 for details.
            if exc.code == 404:
                self.queue.declare()
            else:
                raise
        except kombu.exceptions.ConnectionError as exc:
            # NOTE(gsantomaggio): This exception happens when the
            # connection is established,but it fails to create the queue.
            # Add some delay to avoid too many requests to the server.
            # See: https://bugs.launchpad.net/oslo.messaging/+bug/1822778
            # for details.
            if exc.code == 541:
                interval = 2
                info = {'sleep_time': interval,
                        'queue': self.queue_name,
                        'err_str': exc
                        }
                LOG.error('Internal amqp error (541) '
                          'during queue declare,'
                          'retrying in %(sleep_time)s seconds. '
                          'Queue: [%(queue)s], '
                          'error message: [%(err_str)s]', info)
                time.sleep(interval)
                if self.queue_arguments.get('x-queue-type') == 'quorum':
                    # Before re-declare queue, try to delete it
                    # This is helping with issue #2028384
                    # NOTE(amorin) we need to make sure the connection is
                    # established again, because when an error occur, the
                    # connection is closed.
                    conn.ensure_connection()
                    self.queue.delete()
                self.queue.declare()
            else:
                raise

        self._declared_on = conn.channel

    def consume(self, conn, tag):
        """Actually declare the consumer on the amqp channel.  This will
        start the flow of messages from the queue.  Using the
        Connection.consume() will process the messages,
        calling the appropriate callback.
        """

        # Ensure we are on the correct channel before consuming
        if conn.channel != self._declared_on:
            self.declare(conn)
        try:
            self.queue.consume(callback=self._callback,
                               consumer_tag=str(tag),
                               nowait=self.nowait)
        except conn.connection.channel_errors as exc:
            # We retries once because of some races that we can
            # recover before informing the deployer
            # https://bugs.launchpad.net/oslo.messaging/+bug/1581148
            # https://bugs.launchpad.net/oslo.messaging/+bug/1609766
            # https://bugs.launchpad.net/neutron/+bug/1318721

            # 406 error code relates to messages that are doubled ack'd

            # At any channel error, the RabbitMQ closes
            # the channel, but the amqp-lib quietly re-open
            # it. So, we must reset all tags and declare
            # all consumers again.
            conn._new_tags = set(conn._consumers.values())
            if exc.code == 404 or (exc.code == 406 and
                                   exc.method_name == 'Basic.ack'):
                self.declare(conn)
                self.queue.consume(callback=self._callback,
                                   consumer_tag=str(tag),
                                   nowait=self.nowait)
            else:
                raise
        except amqp_ex.InternalError as exc:
            if self.queue_arguments.get('x-queue-type') == 'quorum':
                # Before re-consume queue, try to delete it
                # This is helping with issue #2028384
                if exc.code == 541:
                    LOG.warning('Queue %s seems broken, will try delete it '
                                'before starting over.', self.queue.name)
                    # NOTE(amorin) we need to make sure the connection is
                    # established again, because when an error occur, the
                    # connection is closed.
                    conn.ensure_connection()
                    self.queue.delete()
                    self.declare(conn)
                    self.queue.consume(callback=self._callback,
                                       consumer_tag=str(tag),
                                       nowait=self.nowait)
            else:
                raise

    def cancel(self, tag):
        LOG.trace('ConsumerBase.cancel: canceling %s', tag)
        self.queue.cancel(str(tag))

    def _callback(self, message):
        """Call callback with deserialized message.

        Messages that are processed and ack'ed.
        """
        if self.rabbit_stream_fanout:
            offset = message.headers.get("x-stream-offset")
            if offset is not None:
                LOG.debug("Stream for %s current offset: %s",
                          self.queue_name, offset)
                self.next_stream_offset = offset + 1

        m2p = getattr(self.queue.channel, 'message_to_python', None)
        if m2p:
            message = m2p(message)
        try:
            self.callback(RabbitMessage(message))
        except Exception:
            LOG.exception("Failed to process message ... skipping it.")
            message.reject()


class DummyConnectionLock(_utils.DummyLock):
    def heartbeat_acquire(self):
        pass


class ConnectionLock(DummyConnectionLock):
    """Lock object to protect access to the kombu connection

    This is a lock object to protect access to the kombu connection
    object between the heartbeat thread and the driver thread.

    They are two way to acquire this lock:
        * lock.acquire()
        * lock.heartbeat_acquire()

    In both case lock.release(), release the lock.

    The goal is that the heartbeat thread always have the priority
    for acquiring the lock. This ensures we have no heartbeat
    starvation when the driver sends a lot of messages.

    So when lock.heartbeat_acquire() is called next time the lock
    is released(), the caller unconditionally acquires
    the lock, even someone else have asked for the lock before it.
    """

    def __init__(self):
        self._workers_waiting = 0
        self._heartbeat_waiting = False
        self._lock_acquired = None
        self._monitor = threading.Lock()
        self._workers_locks = threading.Condition(self._monitor)
        self._heartbeat_lock = threading.Condition(self._monitor)
        self._get_thread_id = eventletutils.fetch_current_thread_functor()

    def acquire(self):
        with self._monitor:
            while self._lock_acquired:
                self._workers_waiting += 1
                self._workers_locks.wait()
                self._workers_waiting -= 1
            self._lock_acquired = self._get_thread_id()

    def heartbeat_acquire(self):
        # NOTE(sileht): must be called only one time
        with self._monitor:
            while self._lock_acquired is not None:
                self._heartbeat_waiting = True
                self._heartbeat_lock.wait()
                self._heartbeat_waiting = False
            self._lock_acquired = self._get_thread_id()

    def release(self):
        with self._monitor:
            if self._lock_acquired is None:
                raise RuntimeError("We can't release a not acquired lock")
            thread_id = self._get_thread_id()
            if self._lock_acquired != thread_id:
                raise RuntimeError("We can't release lock acquired by another "
                                   "thread/greenthread; %s vs %s" %
                                   (self._lock_acquired, thread_id))
            self._lock_acquired = None
            if self._heartbeat_waiting:
                self._heartbeat_lock.notify()
            elif self._workers_waiting > 0:
                self._workers_locks.notify()

    @contextlib.contextmanager
    def for_heartbeat(self):
        self.heartbeat_acquire()
        try:
            yield
        finally:
            self.release()


class Connection:
    """Connection object."""

    def __init__(self, conf, url, purpose, retry=None):
        # NOTE(viktors): Parse config options
        driver_conf = conf.oslo_messaging_rabbit

        self.interval_start = driver_conf.rabbit_retry_interval
        self.interval_stepping = driver_conf.rabbit_retry_backoff
        self.interval_max = driver_conf.rabbit_interval_max
        self.max_retries = retry

        self.login_method = driver_conf.rabbit_login_method
        self.rabbit_ha_queues = driver_conf.rabbit_ha_queues
        self.rabbit_quorum_queue = driver_conf.rabbit_quorum_queue
        self.rabbit_quorum_queue_config = self._get_quorum_configurations(
            driver_conf)
        self.rabbit_transient_quorum_queue = \
            driver_conf.rabbit_transient_quorum_queue
        self.rabbit_stream_fanout = driver_conf.rabbit_stream_fanout
        self.rabbit_transient_queues_ttl = \
            driver_conf.rabbit_transient_queues_ttl
        self.rabbit_qos_prefetch_count = driver_conf.rabbit_qos_prefetch_count
        self.heartbeat_timeout_threshold = \
            driver_conf.heartbeat_timeout_threshold
        self.heartbeat_rate = driver_conf.heartbeat_rate
        self.kombu_reconnect_delay = driver_conf.kombu_reconnect_delay
        self.kombu_reconnect_splay = driver_conf.kombu_reconnect_splay
        self.amqp_durable_queues = driver_conf.amqp_durable_queues
        self.amqp_auto_delete = driver_conf.amqp_auto_delete
        self.ssl = driver_conf.ssl
        self.kombu_missing_consumer_retry_timeout = \
            driver_conf.kombu_missing_consumer_retry_timeout
        self.kombu_failover_strategy = driver_conf.kombu_failover_strategy
        self.kombu_compression = driver_conf.kombu_compression
        self.heartbeat_in_pthread = driver_conf.heartbeat_in_pthread
        self.ssl_enforce_fips_mode = driver_conf.ssl_enforce_fips_mode
        self.enable_cancel_on_failover = driver_conf.enable_cancel_on_failover
        self.use_queue_manager = driver_conf.use_queue_manager

        if self.rabbit_stream_fanout and self.rabbit_qos_prefetch_count <= 0:
            raise RuntimeError('Configuration Error: rabbit_stream_fanout '
                               'need rabbit_qos_prefetch_count to be set to '
                               'a value greater than 0.')

        if (self.rabbit_stream_fanout and not
                self.rabbit_transient_quorum_queue):
            raise RuntimeError('Configuration Error: rabbit_stream_fanout '
                               'need rabbit_transient_quorum_queue to be set '
                               'to true.')

        if self.heartbeat_in_pthread:
            # NOTE(hberaud): Experimental: threading module is in use to run
            # the rabbitmq health check heartbeat. in some situation like
            # with nova-api, nova need green threads to run the cells
            # mechanismes in an async mode, so they used eventlet and
            # greenlet to monkey patch the python stdlib and get green threads.
            # The issue here is that nova-api run under the apache MPM prefork
            # module and mod_wsgi. The apache prefork module doesn't support
            # epoll and recent kernel features, and evenlet is built over epoll
            # and libevent, so when we run the rabbitmq heartbeat we inherit
            # from the execution model of the parent process (nova-api), and
            # in this case we will run the heartbeat through a green thread.
            # We want to allow users to choose between pthread and
            # green threads if needed in some specific situations.
            # This experimental feature allow user to use pthread in an env
            # that doesn't support eventlet without forcing the parent process
            # to stop to use eventlet if they need monkey patching for some
            # specific reasons.
            # If users want to use pthread we need to make sure that we
            # will use the *native* threading module for
            # initialize the heartbeat thread.
            # Here we override globaly the previously imported
            # threading module with the native python threading module
            # if it was already monkey patched by eventlet/greenlet.
            global threading
            threading = _utils.stdlib_threading
            amqpdriver.threading = _utils.stdlib_threading
            amqpdriver.queue = _utils.stdlib_queue

        self.direct_mandatory_flag = driver_conf.direct_mandatory_flag

        if self.ssl:
            self.ssl_version = driver_conf.ssl_version
            self.ssl_key_file = driver_conf.ssl_key_file
            self.ssl_cert_file = driver_conf.ssl_cert_file
            self.ssl_ca_file = driver_conf.ssl_ca_file

            if self.ssl_enforce_fips_mode:
                if hasattr(ssl, 'FIPS_mode'):
                    LOG.info("Enforcing the use of the OpenSSL FIPS mode")
                    ssl.FIPS_mode_set(1)
                else:
                    raise exceptions.ConfigurationError(
                        "OpenSSL FIPS mode is not supported by your Python "
                        "version. You must either change the Python "
                        "executable used to a version with FIPS mode "
                        "support or disable FIPS mode by setting the "
                        "'[oslo_messaging_rabbit] ssl_enforce_fips_mode' "
                        "configuration option to 'False'.")

        self._url = ''
        if url.hosts:
            if url.transport.startswith('kombu+'):
                LOG.warning('Selecting the kombu transport through the '
                            'transport url (%s) is a experimental feature '
                            'and this is not yet supported.',
                            url.transport)
            if len(url.hosts) > 1:
                random.shuffle(url.hosts)
            transformed_urls = [
                self._transform_transport_url(url, host)
                for host in url.hosts]
            self._url = ';'.join(transformed_urls)
        elif url.transport.startswith('kombu+'):
            # NOTE(sileht): url have a + but no hosts
            # (like kombu+memory:///), pass it to kombu as-is
            transport = url.transport.replace('kombu+', '')
            self._url = "%s://" % transport
            if url.virtual_host:
                self._url += url.virtual_host
        elif not url.hosts:
            host = oslo_messaging.transport.TransportHost('')
            # NOTE(moguimar): default_password in this function's context is
            #                 a fallback option, not a hardcoded password.
            #                 username and password are read from host.
            self._url = self._transform_transport_url(  # nosec
                url, host, default_username='guest', default_password='guest',
                default_hostname='localhost')

        self._initial_pid = os.getpid()

        self._consumers = {}
        self._producer = None
        self._new_tags = set()
        self._active_tags = {}
        self._tags = itertools.count(1)

        # Set of exchanges and queues declared on the channel to avoid
        # unnecessary redeclaration. This set is resetted each time
        # the connection is resetted in Connection._set_current_channel
        self._declared_exchanges = set()
        self._declared_queues = set()

        self._consume_loop_stopped = False
        self.channel = None
        self.purpose = purpose

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # we don't need the lock because we don't
        # have a heartbeat thread
        if purpose == rpc_common.PURPOSE_SEND:
            self._connection_lock = ConnectionLock()
        else:
            self._connection_lock = DummyConnectionLock()

        self.connection_id = str(uuid.uuid4())
        self.name = "%s:%d:%s" % (os.path.basename(sys.argv[0]),
                                  os.getpid(),
                                  self.connection_id)
        self.connection = kombu.connection.Connection(
            self._url, ssl=self._fetch_ssl_params(),
            login_method=self.login_method,
            heartbeat=self.heartbeat_timeout_threshold,
            failover_strategy=self.kombu_failover_strategy,
            transport_options={
                'confirm_publish': True,
                'client_properties': {
                    'capabilities': {
                        'authentication_failure_close': True,
                        'connection.blocked': True,
                        'consumer_cancel_notify': True
                    },
                    'connection_name': self.name},
                'on_blocked': self._on_connection_blocked,
                'on_unblocked': self._on_connection_unblocked,
            },
        )

        LOG.debug('[%(connection_id)s] Connecting to AMQP server on'
                  ' %(hostname)s:%(port)s',
                  self._get_connection_info())

        # NOTE(sileht): kombu recommend to run heartbeat_check every
        # seconds, but we use a lock around the kombu connection
        # so, to not lock to much this lock to most of the time do nothing
        # expected waiting the events drain, we start heartbeat_check and
        # retrieve the server heartbeat packet only two times more than
        # the minimum required for the heartbeat works
        # (heartbeat_timeout/heartbeat_rate/2.0, default kombu
        # heartbeat_rate is 2)
        self._heartbeat_wait_timeout = (
            float(self.heartbeat_timeout_threshold) /
            float(self.heartbeat_rate) / 2.0)
        self._heartbeat_support_log_emitted = False

        # NOTE(sileht): just ensure the connection is setuped at startup
        with self._connection_lock:
            self.ensure_connection()

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # the consume code does the heartbeat stuff
        # we don't need a thread
        self._heartbeat_thread = None
        if purpose == rpc_common.PURPOSE_SEND:
            self._heartbeat_start()

        LOG.debug('[%(connection_id)s] Connected to AMQP server on '
                  '%(hostname)s:%(port)s via [%(transport)s] client with'
                  ' port %(client_port)s.',
                  self._get_connection_info())

        # NOTE(sileht): value chosen according the best practice from kombu
        # http://kombu.readthedocs.org/en/latest/reference/kombu.common.html#kombu.common.eventloop
        # For heartbeat, we can set a bigger timeout, and check we receive the
        # heartbeat packets regulary
        if self._heartbeat_supported_and_enabled():
            self._poll_timeout = self._heartbeat_wait_timeout
        else:
            self._poll_timeout = 1

        if self._url.startswith('memory://'):
            # Kludge to speed up tests.
            self.connection.transport.polling_interval = 0.0
            # Fixup logging
            self.connection.hostname = "memory_driver"
            self.connection.port = 1234
            self._poll_timeout = 0.05

        if self.use_queue_manager:
            self._q_manager = amqpdriver.QManager(
                hostname=driver_conf.hostname,
                processname=driver_conf.processname)
        else:
            self._q_manager = None

    # FIXME(markmc): use oslo sslutils when it is available as a library
    _SSL_PROTOCOLS = {
        "tlsv1": ssl.PROTOCOL_TLSv1,
        "sslv23": ssl.PROTOCOL_SSLv23
    }

    _OPTIONAL_PROTOCOLS = {
        'sslv2': 'PROTOCOL_SSLv2',
        'sslv3': 'PROTOCOL_SSLv3',
        'tlsv1_1': 'PROTOCOL_TLSv1_1',
        'tlsv1_2': 'PROTOCOL_TLSv1_2',
    }
    for protocol in _OPTIONAL_PROTOCOLS:
        try:
            _SSL_PROTOCOLS[protocol] = getattr(ssl,
                                               _OPTIONAL_PROTOCOLS[protocol])
        except AttributeError:
            pass

    @property
    def durable(self):
        # Quorum queues are durable by default, durable option should
        # be enabled by default with quorum queues
        return self.amqp_durable_queues or self.rabbit_quorum_queue

    @classmethod
    def validate_ssl_version(cls, version):
        key = version.lower()
        try:
            return cls._SSL_PROTOCOLS[key]
        except KeyError:
            raise RuntimeError("Invalid SSL version : %s" % version)

    def _get_quorum_configurations(self, driver_conf):
        """Get the quorum queue configurations"""
        delivery_limit = driver_conf.rabbit_quorum_delivery_limit
        max_memory_length = driver_conf.rabbit_quorum_max_memory_length
        max_memory_bytes = driver_conf.rabbit_quorum_max_memory_bytes
        return QuorumMemConfig(delivery_limit, max_memory_length,
                               max_memory_bytes)

    # NOTE(moguimar): default_password in this function's context is just
    #                 a fallback option, not a hardcoded password.
    def _transform_transport_url(self, url, host, default_username='',  # nosec
                                 default_password='', default_hostname=''):
        transport = url.transport.replace('kombu+', '')
        transport = transport.replace('rabbit', 'amqp')
        return '{}://{}:{}@{}:{}/{}'.format(
            transport,
            parse.quote(host.username or default_username),
            parse.quote(host.password or default_password),
            netutils.escape_ipv6(host.hostname) or default_hostname,
            str(host.port or 5672),
            url.virtual_host or '')

    def _fetch_ssl_params(self):
        """Handles fetching what ssl params should be used for the connection
        (if any).
        """
        if self.ssl:
            ssl_params = dict()

            # http://docs.python.org/library/ssl.html
            if self.ssl_version:
                ssl_params['ssl_version'] = self.validate_ssl_version(
                    self.ssl_version)
            if self.ssl_key_file:
                ssl_params['keyfile'] = self.ssl_key_file
            if self.ssl_cert_file:
                ssl_params['certfile'] = self.ssl_cert_file
            if self.ssl_ca_file:
                ssl_params['ca_certs'] = self.ssl_ca_file
                # We might want to allow variations in the
                # future with this?
                ssl_params['cert_reqs'] = ssl.CERT_REQUIRED
            return ssl_params or True
        return False

    @staticmethod
    def _on_connection_blocked(reason):
        LOG.error("The broker has blocked the connection: %s", reason)

    @staticmethod
    def _on_connection_unblocked():
        LOG.info("The broker has unblocked the connection")

    def ensure_connection(self):
        # NOTE(sileht): we reset the channel and ensure
        # the kombu underlying connection works
        def on_error(exc, interval):
            LOG.error("Connection failed: %s (retrying in %s seconds)",
                      str(exc), interval)

        self._set_current_channel(None)
        self.connection.ensure_connection(
            errback=on_error,
            max_retries=self.max_retries,
            interval_start=self.interval_start,
            interval_step=self.interval_stepping,
            interval_max=self.interval_max,
        )
        self._set_current_channel(self.connection.channel())
        self.set_transport_socket_timeout()

    def ensure(self, method, retry=None,
               recoverable_error_callback=None, error_callback=None,
               timeout_is_error=True):
        """Will retry up to retry number of times.
        retry = None or -1 means to retry forever
        retry = 0 means no retry
        retry = N means N retries

        NOTE(sileht): Must be called within the connection lock
        """

        current_pid = os.getpid()
        if self._initial_pid != current_pid:
            LOG.warning("Process forked after connection established! "
                        "This can result in unpredictable behavior. "
                        "See: https://docs.openstack.org/oslo.messaging/"
                        "latest/reference/transport.html")
            self._initial_pid = current_pid

        if retry is None or retry < 0:
            retry = float('inf')

        def on_error(exc, interval):
            LOG.debug("[%s] Received recoverable error from kombu:"
                      % self.connection_id,
                      exc_info=True)

            recoverable_error_callback and recoverable_error_callback(exc)

            interval = (self.kombu_reconnect_delay + interval
                        if self.kombu_reconnect_delay > 0
                        else interval)
            if self.kombu_reconnect_splay > 0:
                interval += random.uniform(
                    0,
                    self.kombu_reconnect_splay)  # nosec

            info = {'err_str': exc, 'sleep_time': interval}
            info.update(self._get_connection_info(conn_error=True))

            if self.rabbit_stream_fanout and 'Basic.cancel' in str(exc):
                # This branch allows for consumer offset reset
                # in the unlikely case consumers are cancelled. This may
                # happen, for example, when we delete the stream queue.
                # We need to start consuming from "last" because the stream
                # offset maybe reset.
                LOG.warn('[%s] Basic.cancel received. '
                         'Resetting consumers offsets to last.',
                         self.connection_id)
                for consumer in self._consumers:
                    consumer.reset_stream_offset()

            if 'Socket closed' in str(exc):
                LOG.error('[%(connection_id)s] AMQP server'
                          ' %(hostname)s:%(port)s closed'
                          ' the connection. Check login credentials:'
                          ' %(err_str)s', info)

            else:
                LOG.error('[%(connection_id)s] AMQP server on '
                          '%(hostname)s:%(port)s is unreachable: '
                          '%(err_str)s. Trying again in '
                          '%(sleep_time)d seconds.', info)

            # XXX(nic): when reconnecting to a RabbitMQ cluster
            # with mirrored queues in use, the attempt to release the
            # connection can hang "indefinitely" somewhere deep down
            # in Kombu.  Blocking the thread for a bit prior to
            # release seems to kludge around the problem where it is
            # otherwise reproduceable.
            # TODO(sileht): Check if this is useful since we
            # use kombu for HA connection, the interval_step
            # should sufficient, because the underlying kombu transport
            # connection object freed.
            if self.kombu_reconnect_delay > 0:
                LOG.trace('Delaying reconnect for %1.1f seconds ...',
                          self.kombu_reconnect_delay)
                time.sleep(self.kombu_reconnect_delay)

        def on_reconnection(new_channel):
            """Callback invoked when the kombu reconnects and creates
            a new channel, we use it the reconfigure our consumers.
            """
            self._set_current_channel(new_channel)
            self.set_transport_socket_timeout()

            LOG.info('[%(connection_id)s] Reconnected to AMQP server on '
                     '%(hostname)s:%(port)s via [%(transport)s] client '
                     'with port %(client_port)s.',
                     self._get_connection_info())

        def execute_method(channel):
            self._set_current_channel(channel)
            method()

        try:
            autoretry_method = self.connection.autoretry(
                execute_method, channel=self.channel,
                max_retries=retry,
                errback=on_error,
                interval_start=self.interval_start,
                interval_step=self.interval_stepping,
                interval_max=self.interval_max,
                on_revive=on_reconnection)
            ret, channel = autoretry_method()
            self._set_current_channel(channel)
            return ret
        except rpc_amqp.AMQPDestinationNotFound:
            # NOTE(sileht): we must reraise this without
            # trigger error_callback
            raise
        except exceptions.MessageUndeliverable:
            # NOTE(gsantomaggio): we must reraise this without
            # trigger error_callback
            raise
        except Exception as exc:
            error_callback and error_callback(exc)
            self._set_current_channel(None)
            # NOTE(sileht): number of retry exceeded and the connection
            # is still broken
            info = {'err_str': exc, 'retry': retry}
            info.update(self.connection.info())
            msg = ('Unable to connect to AMQP server on '
                   '%(hostname)s:%(port)s after %(retry)s '
                   'tries: %(err_str)s' % info)
            LOG.error(msg)
            raise exceptions.MessageDeliveryFailure(msg)

    @staticmethod
    def on_return(exception, exchange, routing_key, message):
        raise exceptions.MessageUndeliverable(exception, exchange, routing_key,
                                              message)

    def _set_current_channel(self, new_channel):
        """Change the channel to use.

        NOTE(sileht): Must be called within the connection lock
        """
        if new_channel == self.channel:
            return

        if self.channel is not None:
            self._declared_queues.clear()
            self._declared_exchanges.clear()
            self.connection.maybe_close_channel(self.channel)

        self.channel = new_channel

        if new_channel is not None:
            if self.purpose == rpc_common.PURPOSE_LISTEN:
                self._set_qos(new_channel)
            self._producer = kombu.messaging.Producer(new_channel,
                                                      on_return=self.on_return)
            for consumer in self._consumers:
                consumer.declare(self)

    def _set_qos(self, channel):
        """Set QoS prefetch count on the channel"""
        if self.rabbit_qos_prefetch_count > 0:
            channel.basic_qos(0,
                              self.rabbit_qos_prefetch_count,
                              False)

    def close(self):
        """Close/release this connection."""
        self._heartbeat_stop()
        if self.connection:
            # NOTE(jcosmao) Delete queue should be called only when queue name
            # is randomized. When using streams, queue is shared between
            # all consumers, thus deleting fanout queue will force all other
            # consumers to disconnect/reconnect by throwing
            # amqp.exceptions.ConsumerCancelled.
            # When using QManager, queue name is consistent accross agent
            # restart, so we don't need to delete it either. Deletion must be
            # handled by expiration policy.
            if not self.rabbit_stream_fanout and not self.use_queue_manager:
                for consumer in filter(lambda c: c.type == 'fanout',
                                       self._consumers):
                    LOG.debug('[connection close] Deleting fanout '
                              'queue: %s ' % consumer.queue.name)
                    consumer.queue.delete()
            self._set_current_channel(None)
            self.connection.release()
            self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        with self._connection_lock:
            try:
                for consumer, tag in self._consumers.items():
                    consumer.cancel(tag=tag)
            except kombu.exceptions.OperationalError:
                self.ensure_connection()
            self._consumers.clear()
            self._active_tags.clear()
            self._new_tags.clear()
            self._tags = itertools.count(1)

    def _heartbeat_supported_and_enabled(self):
        if self.heartbeat_timeout_threshold <= 0:
            return False

        if self.connection.supports_heartbeats:
            return True
        elif not self._heartbeat_support_log_emitted:
            LOG.warning("Heartbeat support requested but it is not "
                        "supported by the kombu driver or the broker")
            self._heartbeat_support_log_emitted = True
        return False

    def set_transport_socket_timeout(self, timeout=None):
        # NOTE(sileht): they are some case where the heartbeat check
        # or the producer.send return only when the system socket
        # timeout if reach. kombu doesn't allow use to customise this
        # timeout so for py-amqp we tweak ourself
        # NOTE(dmitryme): Current approach works with amqp==1.4.9 and
        # kombu==3.0.33. Once the commit below is released, we should
        # try to set the socket timeout in the constructor:
        # https://github.com/celery/py-amqp/pull/64

        heartbeat_timeout = self.heartbeat_timeout_threshold
        if self._heartbeat_supported_and_enabled():
            # NOTE(sileht): we are supposed to send heartbeat every
            # heartbeat_timeout, no need to wait more otherwise will
            # disconnect us, so raise timeout earlier ourself
            if timeout is None:
                timeout = heartbeat_timeout
            else:
                timeout = min(heartbeat_timeout, timeout)

        try:
            sock = self.channel.connection.sock
        except AttributeError as e:
            # Level is set to debug because otherwise we would spam the logs
            LOG.debug('[%s] Failed to get socket attribute: %s'
                      % (self.connection_id, str(e)))
        else:
            sock.settimeout(timeout)
            # TCP_USER_TIMEOUT is not defined on Windows and Mac OS X
            if sys.platform != 'win32' and sys.platform != 'darwin':
                try:
                    timeout = timeout * 1000 if timeout is not None else 0
                    # NOTE(gdavoian): only integers and strings are allowed
                    # as socket options' values, and TCP_USER_TIMEOUT option
                    # can take only integer values, so we round-up the timeout
                    # to the nearest integer in order to ensure that the
                    # connection is not broken before the expected timeout
                    sock.setsockopt(socket.IPPROTO_TCP,
                                    TCP_USER_TIMEOUT,
                                    int(math.ceil(timeout)))
                except OSError as error:
                    code = error[0]
                    # TCP_USER_TIMEOUT not defined on kernels <2.6.37
                    if code != errno.ENOPROTOOPT:
                        raise

    @contextlib.contextmanager
    def _transport_socket_timeout(self, timeout):
        self.set_transport_socket_timeout(timeout)
        yield
        self.set_transport_socket_timeout()

    def _heartbeat_check(self):
        # NOTE(sileht): we are supposed to send at least one heartbeat
        # every heartbeat_timeout_threshold, so no need to way more
        self.connection.heartbeat_check(rate=self.heartbeat_rate)

    def _heartbeat_start(self):
        if self._heartbeat_supported_and_enabled():
            self._heartbeat_exit_event = threading.Event()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_thread_job, name="Rabbit-heartbeat")
            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()
        else:
            self._heartbeat_thread = None

    def _heartbeat_stop(self):
        if self._heartbeat_thread is not None:
            self._heartbeat_exit_event.set()
            self._heartbeat_thread.join()
            self._heartbeat_thread = None

    def _heartbeat_thread_job(self):
        """Thread that maintains inactive connections
        """

        while not self._heartbeat_exit_event.is_set():
            with self._connection_lock.for_heartbeat():

                try:
                    try:
                        self._heartbeat_check()
                        # NOTE(sileht): We need to drain event to receive
                        # heartbeat from the broker but don't hold the
                        # connection too much times. In amqpdriver a connection
                        # is used exclusively for read or for write, so we have
                        # to do this for connection used for write drain_events
                        # already do that for other connection
                        try:
                            self.connection.drain_events(timeout=0.001)
                        except socket.timeout:
                            pass
                    # NOTE(hberaud): In a clustered rabbitmq when
                    # a node disappears, we get a ConnectionRefusedError
                    # because the socket get disconnected.
                    # The socket access yields a OSError because the heartbeat
                    # tries to reach an unreachable host (No route to host).
                    # Catch these exceptions to ensure that we call
                    # ensure_connection for switching the
                    # connection destination.
                    except (socket.timeout,
                            ConnectionRefusedError,
                            OSError,
                            kombu.exceptions.OperationalError,
                            amqp_ex.ConnectionForced) as exc:
                        LOG.info("A recoverable connection/channel error "
                                 "occurred, trying to reconnect: %s", exc)
                        self.ensure_connection()
                except Exception:
                    LOG.warning("Unexpected error during heartbeat "
                                "thread processing, retrying...")
                    LOG.debug('Exception', exc_info=True)

            self._heartbeat_exit_event.wait(
                timeout=self._heartbeat_wait_timeout)
        self._heartbeat_exit_event.clear()

    def declare_consumer(self, consumer):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """

        def _connect_error(exc):
            log_info = {'topic': consumer.routing_key, 'err_str': exc}
            LOG.error("Failed to declare consumer for topic '%(topic)s': "
                      "%(err_str)s", log_info)

        def _declare_consumer():
            consumer.declare(self)
            tag = self._active_tags.get(consumer.queue_name)
            if tag is None:
                tag = next(self._tags)
                self._active_tags[consumer.queue_name] = tag
                self._new_tags.add(tag)

            self._consumers[consumer] = tag
            return consumer

        with self._connection_lock:
            return self.ensure(_declare_consumer,
                               error_callback=_connect_error)

    def consume(self, timeout=None):
        """Consume from all queues/consumers."""

        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()

        def _raise_timeout():
            raise rpc_common.Timeout()

        def _recoverable_error_callback(exc):
            if not isinstance(exc, rpc_common.Timeout):
                self._new_tags = set(self._consumers.values())
            timer.check_return(_raise_timeout)

        def _error_callback(exc):
            _recoverable_error_callback(exc)
            LOG.error('Failed to consume message from queue: %s', exc)

        def _consume():
            # NOTE(sileht): in case the acknowledgment or requeue of a
            # message fail, the kombu transport can be disconnected
            # In this case, we must redeclare our consumers, so raise
            # a recoverable error to trigger the reconnection code.
            if not self.connection.connected:
                raise self.connection.recoverable_connection_errors[0]

            while self._new_tags:
                for consumer, tag in self._consumers.items():
                    if tag in self._new_tags:
                        consumer.consume(self, tag=tag)
                        self._new_tags.remove(tag)

            poll_timeout = (self._poll_timeout if timeout is None
                            else min(timeout, self._poll_timeout))
            while True:
                if self._consume_loop_stopped:
                    return

                if self._heartbeat_supported_and_enabled():
                    self._heartbeat_check()

                try:
                    self.connection.drain_events(timeout=poll_timeout)
                    return
                except socket.timeout:
                    poll_timeout = timer.check_return(
                        _raise_timeout, maximum=self._poll_timeout)
                except self.connection.channel_errors as exc:
                    if exc.code == 406 and exc.method_name == 'Basic.ack':
                        # NOTE(gordc): occasionally multiple workers will grab
                        # same message and acknowledge it. if it happens, meh.
                        raise self.connection.recoverable_channel_errors[0]
                    raise

        with self._connection_lock:
            self.ensure(_consume,
                        recoverable_error_callback=_recoverable_error_callback,
                        error_callback=_error_callback)

    def stop_consuming(self):
        self._consume_loop_stopped = True

    def declare_direct_consumer(self, topic, callback):
        """Create a 'direct' queue.
        In nova's use, this is generally a msg_id queue used for
        responses for call/multicall
        """

        consumer = Consumer(
            exchange_name='',  # using default exchange
            queue_name=topic,
            routing_key='',
            type='direct',
            durable=self.rabbit_transient_quorum_queue,
            exchange_auto_delete=False,
            queue_auto_delete=False,
            callback=callback,
            rabbit_ha_queues=self.rabbit_ha_queues,
            rabbit_queue_ttl=self.rabbit_transient_queues_ttl,
            enable_cancel_on_failover=self.enable_cancel_on_failover,
            rabbit_quorum_queue=self.rabbit_transient_quorum_queue,
            rabbit_quorum_queue_config=self.rabbit_quorum_queue_config)

        self.declare_consumer(consumer)

    def declare_topic_consumer(self, exchange_name, topic, callback=None,
                               queue_name=None):
        """Create a 'topic' consumer."""
        consumer = Consumer(
            exchange_name=exchange_name,
            queue_name=queue_name or topic,
            routing_key=topic,
            type='topic',
            durable=self.durable,
            exchange_auto_delete=self.amqp_auto_delete,
            queue_auto_delete=self.amqp_auto_delete,
            callback=callback,
            rabbit_ha_queues=self.rabbit_ha_queues,
            enable_cancel_on_failover=self.enable_cancel_on_failover,
            rabbit_quorum_queue=self.rabbit_quorum_queue,
            rabbit_quorum_queue_config=self.rabbit_quorum_queue_config)

        self.declare_consumer(consumer)

    def declare_fanout_consumer(self, topic, callback):
        """Create a 'fanout' consumer."""

        exchange_name = '%s_fanout' % topic
        if self.rabbit_stream_fanout:
            queue_name = '%s_fanout' % topic
        else:
            if self._q_manager:
                unique = self._q_manager.get()
            else:
                unique = uuid.uuid4().hex
            queue_name = '{}_fanout_{}'.format(topic, unique)
        LOG.debug('Creating fanout queue: %s', queue_name)

        is_durable = (self.rabbit_transient_quorum_queue or
                      self.rabbit_stream_fanout)

        consumer = Consumer(
            exchange_name=exchange_name,
            queue_name=queue_name,
            routing_key=topic,
            type='fanout',
            durable=is_durable,
            exchange_auto_delete=True,
            queue_auto_delete=False,
            callback=callback,
            rabbit_ha_queues=self.rabbit_ha_queues,
            rabbit_queue_ttl=self.rabbit_transient_queues_ttl,
            enable_cancel_on_failover=self.enable_cancel_on_failover,
            rabbit_quorum_queue=self.rabbit_transient_quorum_queue,
            rabbit_quorum_queue_config=self.rabbit_quorum_queue_config,
            rabbit_stream_fanout=self.rabbit_stream_fanout)

        self.declare_consumer(consumer)

    def _ensure_publishing(self, method, exchange, msg, routing_key=None,
                           timeout=None, retry=None, transport_options=None):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            log_info = {'topic': exchange.name, 'err_str': exc}
            LOG.error("Failed to publish message to topic "
                      "'%(topic)s': %(err_str)s", log_info)
            LOG.debug('Exception', exc_info=exc)

        method = functools.partial(method, exchange, msg, routing_key,
                                   timeout, transport_options)

        with self._connection_lock:
            self.ensure(method, retry=retry, error_callback=_error_callback)

    def _get_connection_info(self, conn_error=False):
        # Bug #1745166: set 'conn_error' true if this is being called when the
        # connection is in a known error state.  Otherwise attempting to access
        # the connection's socket while it is in an error state will cause
        # py-amqp to attempt reconnecting.
        ci = self.connection.info()
        info = {k: ci.get(k) for k in
                ['hostname', 'port', 'transport']}
        client_port = None
        if (not conn_error and self.channel and
                hasattr(self.channel.connection, 'sock') and
                self.channel.connection.sock):
            client_port = self.channel.connection.sock.getsockname()[1]
        info.update({'client_port': client_port,
                     'connection_id': self.connection_id})
        return info

    def _publish(self, exchange, msg, routing_key=None, timeout=None,
                 transport_options=None):
        """Publish a message."""

        if not (exchange.passive or exchange.name in self._declared_exchanges):
            try:
                exchange(self.channel).declare()
            except amqp_ex.PreconditionFailed as err:
                # NOTE(hberaud): This kind of exception may be triggered
                # when a control exchange is shared between services and
                # when services try to create it with configs that differ
                # from each others. RabbitMQ will reject the services
                # that try to create it with a configuration that differ
                # from the one used first.
                if "PRECONDITION_FAILED - inequivalent arg 'durable'" \
                   in str(err):
                    LOG.warning("Force creating a non durable exchange.")
                    exchange.durable = False
                    exchange(self.channel).declare()
            self._declared_exchanges.add(exchange.name)

        log_info = {'msg': msg,
                    'who': exchange or 'default',
                    'key': routing_key,
                    'transport_options': str(transport_options)}
        LOG.trace('Connection._publish: sending message %(msg)s to'
                  ' %(who)s with routing key %(key)s', log_info)
        # NOTE(sileht): no need to wait more, caller expects
        # a answer before timeout is reached
        with self._transport_socket_timeout(timeout):
            self._producer.publish(
                msg,
                mandatory=transport_options.at_least_once if
                transport_options else False,
                exchange=exchange,
                routing_key=routing_key,
                expiration=timeout,
                compression=self.kombu_compression)

    def _publish_and_creates_default_queue(self, exchange, msg,
                                           routing_key=None, timeout=None,
                                           transport_options=None):
        """Publisher that declares a default queue

        When the exchange is missing instead of silently creates an exchange
        not binded to a queue, this publisher creates a default queue
        named with the routing_key

        This is mainly used to not miss notification in case of nobody consumes
        them yet. If the future consumer bind the default queue it can retrieve
        missing messages.

        _set_current_channel is responsible to cleanup the cache.
        """
        queue_identifier = (exchange.name, routing_key)
        # NOTE(sileht): We only do it once per reconnection
        # the Connection._set_current_channel() is responsible to clear
        # this cache
        if queue_identifier not in self._declared_queues:
            queue = kombu.entity.Queue(
                channel=self.channel,
                exchange=exchange,
                durable=exchange.durable,
                auto_delete=exchange.auto_delete,
                name=routing_key,
                routing_key=routing_key,
                queue_arguments=_get_queue_arguments(
                    self.rabbit_ha_queues,
                    0,
                    self.rabbit_quorum_queue,
                    self.rabbit_quorum_queue_config,
                    False))
            log_info = {'key': routing_key, 'exchange': exchange}
            LOG.trace(
                'Connection._publish_and_creates_default_queue: '
                'declare queue %(key)s on %(exchange)s exchange', log_info)
            queue.declare()
            self._declared_queues.add(queue_identifier)

        self._publish(exchange, msg, routing_key=routing_key, timeout=timeout)

    def _publish_and_raises_on_missing_exchange(self, exchange, msg,
                                                routing_key=None,
                                                timeout=None,
                                                transport_options=None):
        """Publisher that raises exception if exchange is missing."""
        if not exchange.passive:
            raise RuntimeError("_publish_and_retry_on_missing_exchange() must "
                               "be called with an passive exchange.")

        try:
            self._publish(exchange, msg, routing_key=routing_key,
                          timeout=timeout, transport_options=transport_options)
            return
        except self.connection.channel_errors as exc:
            if exc.code == 404:
                # NOTE(noelbk/sileht):
                # If rabbit dies, the consumer can be disconnected before the
                # publisher sends, and if the consumer hasn't declared the
                # queue, the publisher's will send a message to an exchange
                # that's not bound to a queue, and the message wll be lost.
                # So we set passive=True to the publisher exchange and catch
                # the 404 kombu ChannelError and retry until the exchange
                # appears
                raise rpc_amqp.AMQPDestinationNotFound(
                    "exchange %s doesn't exist" % exchange.name)
            raise

    def direct_send(self, msg_id, msg):
        """Send a 'direct' message."""
        exchange = kombu.entity.Exchange(
            name='',  # using default exchange
            type='direct',
            durable=self.rabbit_transient_quorum_queue,
            auto_delete=True,
            passive=True)
        options = oslo_messaging.TransportOptions(
            at_least_once=self.direct_mandatory_flag)

        LOG.debug('Sending direct to %s', msg_id)
        self._ensure_publishing(self._publish_and_raises_on_missing_exchange,
                                exchange, msg, routing_key=msg_id,
                                transport_options=options)

    def topic_send(self, exchange_name, topic, msg, timeout=None, retry=None,
                   transport_options=None):
        """Send a 'topic' message."""
        exchange = kombu.entity.Exchange(
            name=exchange_name,
            type='topic',
            durable=self.durable,
            auto_delete=self.amqp_auto_delete)

        LOG.debug('Sending topic to %s with routing_key %s', exchange_name,
                  topic)
        self._ensure_publishing(self._publish, exchange, msg,
                                routing_key=topic, timeout=timeout,
                                retry=retry,
                                transport_options=transport_options)

    def fanout_send(self, topic, msg, retry=None):
        """Send a 'fanout' message."""
        exchange = kombu.entity.Exchange(
            name='%s_fanout' % topic,
            type='fanout',
            durable=self.rabbit_transient_quorum_queue,
            auto_delete=True)

        LOG.debug('Sending fanout to %s_fanout', topic)
        self._ensure_publishing(self._publish, exchange, msg, retry=retry)

    def notify_send(self, exchange_name, topic, msg, retry=None, **kwargs):
        """Send a notify message on a topic."""
        exchange = kombu.entity.Exchange(
            name=exchange_name,
            type='topic',
            durable=self.durable,
            auto_delete=self.amqp_auto_delete)

        self._ensure_publishing(self._publish_and_creates_default_queue,
                                exchange, msg, routing_key=topic, retry=retry)


class RabbitDriver(amqpdriver.AMQPDriverBase):
    """RabbitMQ Driver

    The ``rabbit`` driver is the default driver used in OpenStack's
    integration tests.

    The driver is aliased as ``kombu`` to support upgrading existing
    installations with older settings.

    """

    def __init__(self, conf, url,
                 default_exchange=None,
                 allowed_remote_exmods=None):
        opt_group = cfg.OptGroup(name='oslo_messaging_rabbit',
                                 title='RabbitMQ driver options')
        conf.register_group(opt_group)
        conf.register_opts(rabbit_opts, group=opt_group)
        conf.register_opts(rpc_amqp.amqp_opts, group=opt_group)
        conf = rpc_common.ConfigOptsProxy(conf, url, opt_group.name)

        self.missing_destination_retry_timeout = (
            conf.oslo_messaging_rabbit.kombu_missing_consumer_retry_timeout)

        self.prefetch_size = (
            conf.oslo_messaging_rabbit.rabbit_qos_prefetch_count)

        # the pool configuration properties
        max_size = conf.oslo_messaging_rabbit.rpc_conn_pool_size
        min_size = conf.oslo_messaging_rabbit.conn_pool_min_size
        if max_size < min_size:
            raise RuntimeError(
                f"rpc_conn_pool_size: {max_size} must be greater than "
                f"or equal to conn_pool_min_size: {min_size}")
        ttl = conf.oslo_messaging_rabbit.conn_pool_ttl

        connection_pool = pool.ConnectionPool(
            conf, max_size, min_size, ttl,
            url, Connection)

        super().__init__(
            conf, url,
            connection_pool,
            default_exchange,
            allowed_remote_exmods
        )

    def require_features(self, requeue=True):
        pass
