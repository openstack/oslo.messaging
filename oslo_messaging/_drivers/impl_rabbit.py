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
import functools
import os
import random
import socket
import ssl
import threading
import time
import uuid

import kombu
import kombu.connection
import kombu.entity
import kombu.messaging
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import netutils
import six
from six.moves.urllib import parse

from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers import pool
from oslo_messaging._i18n import _
from oslo_messaging._i18n import _LE
from oslo_messaging._i18n import _LI
from oslo_messaging._i18n import _LW
from oslo_messaging import _utils
from oslo_messaging import exceptions


rabbit_opts = [
    cfg.StrOpt('kombu_ssl_version',
               default='',
               deprecated_group='DEFAULT',
               help='SSL version to use (valid only if SSL enabled). '
                    'Valid values are TLSv1 and SSLv23. SSLv2, SSLv3, '
                    'TLSv1_1, and TLSv1_2 may be available on some '
                    'distributions.'
               ),
    cfg.StrOpt('kombu_ssl_keyfile',
               default='',
               deprecated_group='DEFAULT',
               help='SSL key file (valid only if SSL enabled).'),
    cfg.StrOpt('kombu_ssl_certfile',
               default='',
               deprecated_group='DEFAULT',
               help='SSL cert file (valid only if SSL enabled).'),
    cfg.StrOpt('kombu_ssl_ca_certs',
               default='',
               deprecated_group='DEFAULT',
               help='SSL certification authority file '
                    '(valid only if SSL enabled).'),
    cfg.FloatOpt('kombu_reconnect_delay',
                 default=1.0,
                 deprecated_group='DEFAULT',
                 help='How long to wait before reconnecting in response to an '
                      'AMQP consumer cancel notification.'),
    cfg.IntOpt('kombu_missing_consumer_retry_timeout',
               deprecated_name="kombu_reconnect_timeout",
               default=60,
               help='How long to wait a missing client beforce abandoning to '
                    'send it its replies. This value should not be longer '
                    'than rpc_response_timeout.'),
    cfg.StrOpt('kombu_failover_strategy',
               choices=('round-robin', 'shuffle'),
               default='round-robin',
               help='Determines how the next RabbitMQ node is chosen in case '
                    'the one we are currently connected to becomes '
                    'unavailable. Takes effect only if more than one '
                    'RabbitMQ node is provided in config.'),
    cfg.StrOpt('rabbit_host',
               default='localhost',
               deprecated_group='DEFAULT',
               help='The RabbitMQ broker address where a single node is '
                    'used.'),
    cfg.PortOpt('rabbit_port',
                default=5672,
                deprecated_group='DEFAULT',
                help='The RabbitMQ broker port where a single node is used.'),
    cfg.ListOpt('rabbit_hosts',
                default=['$rabbit_host:$rabbit_port'],
                deprecated_group='DEFAULT',
                help='RabbitMQ HA cluster host:port pairs.'),
    cfg.BoolOpt('rabbit_use_ssl',
                default=False,
                deprecated_group='DEFAULT',
                help='Connect over SSL for RabbitMQ.'),
    cfg.StrOpt('rabbit_userid',
               default='guest',
               deprecated_group='DEFAULT',
               help='The RabbitMQ userid.'),
    cfg.StrOpt('rabbit_password',
               default='guest',
               deprecated_group='DEFAULT',
               help='The RabbitMQ password.',
               secret=True),
    cfg.StrOpt('rabbit_login_method',
               default='AMQPLAIN',
               deprecated_group='DEFAULT',
               help='The RabbitMQ login method.'),
    cfg.StrOpt('rabbit_virtual_host',
               default='/',
               deprecated_group='DEFAULT',
               help='The RabbitMQ virtual host.'),
    cfg.IntOpt('rabbit_retry_interval',
               default=1,
               help='How frequently to retry connecting with RabbitMQ.'),
    cfg.IntOpt('rabbit_retry_backoff',
               default=2,
               deprecated_group='DEFAULT',
               help='How long to backoff for between retries when connecting '
                    'to RabbitMQ.'),
    cfg.IntOpt('rabbit_max_retries',
               default=0,
               deprecated_group='DEFAULT',
               help='Maximum number of RabbitMQ connection retries. '
                    'Default is 0 (infinite retry count).'),
    cfg.BoolOpt('rabbit_ha_queues',
                default=False,
                deprecated_group='DEFAULT',
                help='Use HA queues in RabbitMQ (x-ha-policy: all). '
                     'If you change this option, you must wipe the '
                     'RabbitMQ database.'),
    cfg.IntOpt('heartbeat_timeout_threshold',
               default=60,
               help="Number of seconds after which the Rabbit broker is "
               "considered down if heartbeat's keep-alive fails "
               "(0 disable the heartbeat). EXPERIMENTAL"),
    cfg.IntOpt('heartbeat_rate',
               default=2,
               help='How often times during the heartbeat_timeout_threshold '
               'we check the heartbeat.'),

    # NOTE(sileht): deprecated option since oslo_messaging 1.5.0,
    cfg.BoolOpt('fake_rabbit',
                default=False,
                deprecated_group='DEFAULT',
                help='Deprecated, use rpc_backend=kombu+memory or '
                'rpc_backend=fake'),
]

LOG = logging.getLogger(__name__)


def _get_queue_arguments(rabbit_ha_queues):
    """Construct the arguments for declaring a queue.

    If the rabbit_ha_queues option is set, we declare a mirrored queue
    as described here:

      http://www.rabbitmq.com/ha.html

    Setting x-ha-policy to all means that the queue will be mirrored
    to all nodes in the cluster.
    """
    return {'x-ha-policy': 'all'} if rabbit_ha_queues else {}


class RabbitMessage(dict):
    def __init__(self, raw_message):
        super(RabbitMessage, self).__init__(
            rpc_common.deserialize_msg(raw_message.payload))
        LOG.trace('RabbitMessage.Init: message %s', self)
        self._raw_message = raw_message

    def acknowledge(self):
        LOG.trace('RabbitMessage.acknowledge: message %s', self)
        self._raw_message.ack()

    def requeue(self):
        LOG.trace('RabbitMessage.requeue: message %s', self)
        self._raw_message.requeue()


class Consumer(object):
    """Consumer class."""

    def __init__(self, exchange_name, queue_name, routing_key, type, durable,
                 auto_delete, callback, nowait=True, rabbit_ha_queues=None):
        """Init the Publisher class with the exchange_name, routing_key,
        type, durable auto_delete
        """
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.auto_delete = auto_delete
        self.durable = durable
        self.callback = callback
        self.type = type
        self.nowait = nowait
        self.queue_arguments = _get_queue_arguments(rabbit_ha_queues)

        self.queue = None
        self.exchange = kombu.entity.Exchange(
            name=exchange_name,
            type=type,
            durable=self.durable,
            auto_delete=self.auto_delete)

    def declare(self, conn):
        """Re-declare the queue after a rabbit (re)connect."""
        self.queue = kombu.entity.Queue(
            name=self.queue_name,
            channel=conn.channel,
            exchange=self.exchange,
            durable=self.durable,
            auto_delete=self.auto_delete,
            routing_key=self.routing_key,
            queue_arguments=self.queue_arguments)

        try:
            LOG.trace('ConsumerBase.declare: '
                      'queue %s', self.queue_name)
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

    def consume(self, tag):
        """Actually declare the consumer on the amqp channel.  This will
        start the flow of messages from the queue.  Using the
        Connection.consume() will process the messages,
        calling the appropriate callback.
        """

        self.queue.consume(callback=self._callback,
                           consumer_tag=six.text_type(tag),
                           nowait=self.nowait)

    def cancel(self, tag):
        LOG.trace('ConsumerBase.cancel: canceling %s', tag)
        self.queue.cancel(six.text_type(tag))

    def _callback(self, message):
        """Call callback with deserialized message.

        Messages that are processed and ack'ed.
        """

        m2p = getattr(self.queue.channel, 'message_to_python', None)
        if m2p:
            message = m2p(message)

        try:
            self.callback(RabbitMessage(message))
        except Exception:
            LOG.exception(_LE("Failed to process message"
                              " ... skipping it."))
            message.ack()


class DummyConnectionLock(_utils.DummyLock):
    def heartbeat_acquire(self):
        pass


class ConnectionLock(DummyConnectionLock):
    """Lock object to protect access the the kombu connection

    This is a lock object to protect access the the kombu connection
    object between the heartbeat thread and the driver thread.

    They are two way to acquire this lock:
        * lock.acquire()
        * lock.heartbeat_acquire()

    In both case lock.release(), release the lock.

    The goal is that the heartbeat thread always have the priority
    for acquiring the lock. This ensures we have no heartbeat
    starvation when the driver sends a lot of messages.

    So when lock.heartbeat_acquire() is called next time the lock
    is released(), the caller unconditionnaly acquires
    the lock, even someone else have asked for the lock before it.
    """

    def __init__(self):
        self._workers_waiting = 0
        self._heartbeat_waiting = False
        self._lock_acquired = None
        self._monitor = threading.Lock()
        self._workers_locks = threading.Condition(self._monitor)
        self._heartbeat_lock = threading.Condition(self._monitor)
        self._get_thread_id = _utils.fetch_current_thread_functor()

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


class Connection(object):
    """Connection object."""

    pools = {}

    def __init__(self, conf, url, purpose):
        # NOTE(viktors): Parse config options
        driver_conf = conf.oslo_messaging_rabbit

        self.max_retries = driver_conf.rabbit_max_retries
        self.interval_start = driver_conf.rabbit_retry_interval
        self.interval_stepping = driver_conf.rabbit_retry_backoff

        self.login_method = driver_conf.rabbit_login_method
        self.fake_rabbit = driver_conf.fake_rabbit
        self.virtual_host = driver_conf.rabbit_virtual_host
        self.rabbit_hosts = driver_conf.rabbit_hosts
        self.rabbit_port = driver_conf.rabbit_port
        self.rabbit_userid = driver_conf.rabbit_userid
        self.rabbit_password = driver_conf.rabbit_password
        self.rabbit_ha_queues = driver_conf.rabbit_ha_queues
        self.heartbeat_timeout_threshold = \
            driver_conf.heartbeat_timeout_threshold
        self.heartbeat_rate = driver_conf.heartbeat_rate
        self.kombu_reconnect_delay = driver_conf.kombu_reconnect_delay
        self.amqp_durable_queues = driver_conf.amqp_durable_queues
        self.amqp_auto_delete = driver_conf.amqp_auto_delete
        self.rabbit_use_ssl = driver_conf.rabbit_use_ssl
        self.kombu_missing_consumer_retry_timeout = \
            driver_conf.kombu_missing_consumer_retry_timeout
        self.kombu_failover_strategy = driver_conf.kombu_failover_strategy

        if self.rabbit_use_ssl:
            self.kombu_ssl_version = driver_conf.kombu_ssl_version
            self.kombu_ssl_keyfile = driver_conf.kombu_ssl_keyfile
            self.kombu_ssl_certfile = driver_conf.kombu_ssl_certfile
            self.kombu_ssl_ca_certs = driver_conf.kombu_ssl_ca_certs

        # Try forever?
        if self.max_retries <= 0:
            self.max_retries = None

        # max retry-interval = 30 seconds
        self.interval_max = 30

        if url.virtual_host is not None:
            virtual_host = url.virtual_host
        else:
            virtual_host = self.virtual_host

        self._url = ''
        if self.fake_rabbit:
            LOG.warn(_LW("Deprecated: fake_rabbit option is deprecated, set "
                         "rpc_backend to kombu+memory or use the fake "
                         "driver instead."))
            self._url = 'memory://%s/' % virtual_host
        elif url.hosts:
            if url.transport.startswith('kombu+'):
                LOG.warn(_LW('Selecting the kombu transport through the '
                             'transport url (%s) is a experimental feature '
                             'and this is not yet supported.'), url.transport)
            if len(url.hosts) > 1:
                random.shuffle(url.hosts)
            for host in url.hosts:
                transport = url.transport.replace('kombu+', '')
                transport = transport.replace('rabbit', 'amqp')
                self._url += '%s%s://%s:%s@%s:%s/%s' % (
                    ";" if self._url else '',
                    transport,
                    parse.quote(host.username or ''),
                    parse.quote(host.password or ''),
                    self._parse_url_hostname(host.hostname) or '',
                    str(host.port or 5672),
                    virtual_host)
        elif url.transport.startswith('kombu+'):
            # NOTE(sileht): url have a + but no hosts
            # (like kombu+memory:///), pass it to kombu as-is
            transport = url.transport.replace('kombu+', '')
            self._url = "%s://%s" % (transport, virtual_host)
        else:
            if len(self.rabbit_hosts) > 1:
                random.shuffle(self.rabbit_hosts)
            for adr in self.rabbit_hosts:
                hostname, port = netutils.parse_host_port(
                    adr, default_port=self.rabbit_port)
                self._url += '%samqp://%s:%s@%s:%s/%s' % (
                    ";" if self._url else '',
                    parse.quote(self.rabbit_userid, ''),
                    parse.quote(self.rabbit_password, ''),
                    self._parse_url_hostname(hostname), port,
                    virtual_host)

        self._initial_pid = os.getpid()

        self._consumers = []
        self._new_consumers = []
        self._consume_loop_stopped = False
        self.channel = None

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # we don't need the lock because we don't
        # have a heartbeat thread
        if purpose == rpc_common.PURPOSE_SEND:
            self._connection_lock = ConnectionLock()
        else:
            self._connection_lock = DummyConnectionLock()

        self.connection = kombu.connection.Connection(
            self._url, ssl=self._fetch_ssl_params(),
            login_method=self.login_method,
            heartbeat=self.heartbeat_timeout_threshold,
            failover_strategy=self.kombu_failover_strategy,
            transport_options={
                'confirm_publish': True,
                'on_blocked': self._on_connection_blocked,
                'on_unblocked': self._on_connection_unblocked,
            },
        )

        LOG.debug('Connecting to AMQP server on %(hostname)s:%(port)s',
                  self.connection.info())

        # NOTE(sileht): kombu recommend to run heartbeat_check every
        # seconds, but we use a lock around the kombu connection
        # so, to not lock to much this lock to most of the time do nothing
        # expected waiting the events drain, we start heartbeat_check and
        # retrieve the server heartbeat packet only two times more than
        # the minimum required for the heartbeat works
        # (heatbeat_timeout/heartbeat_rate/2.0, default kombu
        # heartbeat_rate is 2)
        self._heartbeat_wait_timeout = (
            float(self.heartbeat_timeout_threshold) /
            float(self.heartbeat_rate) / 2.0)
        self._heartbeat_support_log_emitted = False

        # NOTE(sileht): just ensure the connection is setuped at startup
        self.ensure_connection()

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # the consume code does the heartbeat stuff
        # we don't need a thread
        self._heartbeat_thread = None
        if purpose == rpc_common.PURPOSE_SEND:
            self._heartbeat_start()

        LOG.debug('Connected to AMQP server on %(hostname)s:%(port)s '
                  'via [%(transport)s] client',
                  self.connection.info())

        # NOTE(sileht): value chosen according the best practice from kombu
        # http://kombu.readthedocs.org/en/latest/reference/kombu.common.html#kombu.common.eventloop
        # For heatbeat, we can set a bigger timeout, and check we receive the
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

    @classmethod
    def validate_ssl_version(cls, version):
        key = version.lower()
        try:
            return cls._SSL_PROTOCOLS[key]
        except KeyError:
            raise RuntimeError(_("Invalid SSL version : %s") % version)

    def _parse_url_hostname(self, hostname):
        """Handles hostname returned from urlparse and checks whether it's
        ipaddress. If it's ipaddress it ensures that it has brackets for IPv6.
        """
        return '[%s]' % hostname if ':' in hostname else hostname

    def _fetch_ssl_params(self):
        """Handles fetching what ssl params should be used for the connection
        (if any).
        """
        if self.rabbit_use_ssl:
            ssl_params = dict()

            # http://docs.python.org/library/ssl.html - ssl.wrap_socket
            if self.kombu_ssl_version:
                ssl_params['ssl_version'] = self.validate_ssl_version(
                    self.kombu_ssl_version)
            if self.kombu_ssl_keyfile:
                ssl_params['keyfile'] = self.kombu_ssl_keyfile
            if self.kombu_ssl_certfile:
                ssl_params['certfile'] = self.kombu_ssl_certfile
            if self.kombu_ssl_ca_certs:
                ssl_params['ca_certs'] = self.kombu_ssl_ca_certs
                # We might want to allow variations in the
                # future with this?
                ssl_params['cert_reqs'] = ssl.CERT_REQUIRED
            return ssl_params or True
        return False

    @staticmethod
    def _on_connection_blocked(reason):
        LOG.error(_LE("The broker has blocked the connection: %s"), reason)

    @staticmethod
    def _on_connection_unblocked():
        LOG.info(_LI("The broker has unblocked the connection"))

    def ensure_connection(self):
        # NOTE(sileht): we reset the channel and ensure
        # the kombu underlying connection works
        self._set_current_channel(None)
        self.ensure(method=lambda: self.connection.connection)

    def ensure(self, method, retry=None,
               recoverable_error_callback=None, error_callback=None,
               timeout_is_error=True):
        """Will retry up to retry number of times.
        retry = None means use the value of rabbit_max_retries
        retry = -1 means to retry forever
        retry = 0 means no retry
        retry = N means N retries

        NOTE(sileht): Must be called within the connection lock
        """

        current_pid = os.getpid()
        if self._initial_pid != current_pid:
            LOG.warn(_LW("Process forked after connection established! "
                         "This can result in unpredictable behavior. "
                         "See: http://docs.openstack.org/developer/"
                         "oslo_messaging/transport.html"))
            self._initial_pid = current_pid

        if retry is None:
            retry = self.max_retries
        if retry is None or retry < 0:
            retry = None

        def on_error(exc, interval):
            LOG.debug("Received recoverable error from kombu:",
                      exc_info=True)

            recoverable_error_callback and recoverable_error_callback(exc)

            interval = (self.kombu_reconnect_delay + interval
                        if self.kombu_reconnect_delay > 0
                        else interval)

            info = {'err_str': exc, 'sleep_time': interval}
            info.update(self.connection.info())

            if 'Socket closed' in six.text_type(exc):
                LOG.error(_LE('AMQP server %(hostname)s:%(port)s closed'
                              ' the connection. Check login credentials:'
                              ' %(err_str)s'), info)
            else:
                LOG.error(_LE('AMQP server on %(hostname)s:%(port)s is '
                              'unreachable: %(err_str)s. Trying again in '
                              '%(sleep_time)d seconds.'), info)

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
            for consumer in self._consumers:
                consumer.declare(self)

            LOG.info(_LI('Reconnected to AMQP server on '
                         '%(hostname)s:%(port)s via [%(transport)s] client'),
                     self.connection.info())

        def execute_method(channel):
            self._set_current_channel(channel)
            method()

        # NOTE(sileht): Some dummy driver like the in-memory one doesn't
        # have notion of recoverable connection, so we must raise the original
        # exception like kombu does in this case.
        has_modern_errors = hasattr(
            self.connection.transport, 'recoverable_connection_errors',
        )
        if has_modern_errors:
            recoverable_errors = (
                self.connection.recoverable_channel_errors +
                self.connection.recoverable_connection_errors)
        else:
            recoverable_errors = ()

        try:
            autoretry_method = self.connection.autoretry(
                execute_method, channel=self.channel,
                max_retries=retry,
                errback=on_error,
                interval_start=self.interval_start or 1,
                interval_step=self.interval_stepping,
                interval_max=self.interval_max,
                on_revive=on_reconnection,
            )
            ret, channel = autoretry_method()
            self._set_current_channel(channel)
            return ret
        except recoverable_errors as exc:
            LOG.debug("Received recoverable error from kombu:",
                      exc_info=True)
            error_callback and error_callback(exc)
            self._set_current_channel(None)
            # NOTE(sileht): number of retry exceeded and the connection
            # is still broken
            info = {'err_str': exc, 'retry': retry}
            info.update(self.connection.info())
            msg = _('Unable to connect to AMQP server on '
                    '%(hostname)s:%(port)s after %(retry)s '
                    'tries: %(err_str)s') % info
            LOG.error(msg)
            raise exceptions.MessageDeliveryFailure(msg)
        except rpc_amqp.AMQPDestinationNotFound:
            # NOTE(sileht): we must reraise this without
            # trigger error_callback
            raise
        except Exception as exc:
            error_callback and error_callback(exc)
            raise

    def _set_current_channel(self, new_channel):
        """Change the channel to use.

        NOTE(sileht): Must be called within the connection lock
        """
        if self.channel is not None and new_channel != self.channel:
            self.PUBLISHER_DECLARED_QUEUES.pop(self.channel, None)
            self.connection.maybe_close_channel(self.channel)
        self.channel = new_channel

    def close(self):
        """Close/release this connection."""
        self._heartbeat_stop()
        if self.connection:
            self._set_current_channel(None)
            self.connection.release()
            self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        recoverable_errors = (self.connection.recoverable_channel_errors +
                              self.connection.recoverable_connection_errors)

        with self._connection_lock:
            try:
                for tag, consumer in enumerate(self._consumers):
                    consumer.cancel(tag=tag)
            except recoverable_errors:
                self.ensure_connection()
            self._consumers = []

    def _heartbeat_supported_and_enabled(self):
        if self.heartbeat_timeout_threshold <= 0:
            return False

        if self.connection.supports_heartbeats:
            return True
        elif not self._heartbeat_support_log_emitted:
            LOG.warn(_LW("Heartbeat support requested but it is not supported "
                         "by the kombu driver or the broker"))
            self._heartbeat_support_log_emitted = True
        return False

    @contextlib.contextmanager
    def _transport_socket_timeout(self, timeout):
        # NOTE(sileht): they are some case where the heartbeat check
        # or the producer.send return only when the system socket
        # timeout if reach. kombu doesn't allow use to customise this
        # timeout so for py-amqp we tweak ourself
        sock = getattr(self.connection.transport, 'sock', None)
        if sock:
            orig_timeout = sock.gettimeout()
            sock.settimeout(timeout)
        yield
        if sock:
            sock.settimeout(orig_timeout)

    def _heartbeat_check(self):
        # NOTE(sileht): we are suposed to send at least one heartbeat
        # every heartbeat_timeout_threshold, so no need to way more
        with self._transport_socket_timeout(
                self.heartbeat_timeout_threshold):
            self.connection.heartbeat_check(
                rate=self.heartbeat_rate)

    def _heartbeat_start(self):
        if self._heartbeat_supported_and_enabled():
            self._heartbeat_exit_event = threading.Event()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_thread_job)
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

                recoverable_errors = (
                    self.connection.recoverable_channel_errors +
                    self.connection.recoverable_connection_errors)

                try:
                    try:
                        self._heartbeat_check()
                        # NOTE(sileht): We need to drain event to receive
                        # heartbeat from the broker but don't hold the
                        # connection too much times. In amqpdriver a connection
                        # is used exclusivly for read or for write, so we have
                        # to do this for connection used for write drain_events
                        # already do that for other connection
                        try:
                            self.connection.drain_events(timeout=0.001)
                        except socket.timeout:
                            pass
                    except recoverable_errors as exc:
                        LOG.info(_LI("A recoverable connection/channel error "
                                     "occurred, trying to reconnect: %s"), exc)
                        self.ensure_connection()
                except Exception:
                    LOG.warning(_LW("Unexpected error during heartbeart "
                                    "thread processing, retrying..."))
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
            LOG.error(_LE("Failed to declare consumer for topic '%(topic)s': "
                          "%(err_str)s"), log_info)

        def _declare_consumer():
            consumer.declare(self)
            self._consumers.append(consumer)
            self._new_consumers.append(consumer)
            return consumer

        with self._connection_lock:
            return self.ensure(_declare_consumer,
                               error_callback=_connect_error)

    def consume(self, timeout=None):
        """Consume from all queues/consumers."""

        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()

        def _raise_timeout(exc):
            LOG.debug('Timed out waiting for RPC response: %s', exc)
            raise rpc_common.Timeout()

        def _recoverable_error_callback(exc):
            if not isinstance(exc, rpc_common.Timeout):
                self._new_consumers = self._consumers
            timer.check_return(_raise_timeout, exc)

        def _error_callback(exc):
            _recoverable_error_callback(exc)
            LOG.error(_LE('Failed to consume message from queue: %s'),
                      exc)

        def _consume():
            # NOTE(sileht): in case the acknowledgment or requeue of a
            # message fail, the kombu transport can be disconnected
            # In this case, we must redeclare our consumers, so raise
            # a recoverable error to trigger the reconnection code.
            if not self.connection.connected:
                raise self.connection.recoverable_connection_errors[0]

            if self._new_consumers:
                for tag, consumer in enumerate(self._consumers):
                    if consumer in self._new_consumers:
                        consumer.consume(tag=tag)
                self._new_consumers = []

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
                except socket.timeout as exc:
                    poll_timeout = timer.check_return(
                        _raise_timeout, exc, maximum=self._poll_timeout)

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

        consumer = Consumer(exchange_name=topic,
                            queue_name=topic,
                            routing_key=topic,
                            type='direct',
                            durable=False,
                            auto_delete=True,
                            callback=callback,
                            rabbit_ha_queues=self.rabbit_ha_queues)

        self.declare_consumer(consumer)

    def declare_topic_consumer(self, exchange_name, topic, callback=None,
                               queue_name=None):
        """Create a 'topic' consumer."""
        consumer = Consumer(exchange_name=exchange_name,
                            queue_name=queue_name or topic,
                            routing_key=topic,
                            type='topic',
                            durable=self.amqp_durable_queues,
                            auto_delete=self.amqp_auto_delete,
                            callback=callback,
                            rabbit_ha_queues=self.rabbit_ha_queues)

        self.declare_consumer(consumer)

    def declare_fanout_consumer(self, topic, callback):
        """Create a 'fanout' consumer."""

        unique = uuid.uuid4().hex
        exchange_name = '%s_fanout' % topic
        queue_name = '%s_fanout_%s' % (topic, unique)

        consumer = Consumer(exchange_name=exchange_name,
                            queue_name=queue_name,
                            routing_key=topic,
                            type='fanout',
                            durable=False,
                            auto_delete=True,
                            callback=callback,
                            rabbit_ha_queues=self.rabbit_ha_queues)

        self.declare_consumer(consumer)

    def _ensure_publishing(self, method, exchange, msg, routing_key=None,
                           timeout=None, retry=None):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            log_info = {'topic': exchange.name, 'err_str': exc}
            LOG.error(_LE("Failed to publish message to topic "
                          "'%(topic)s': %(err_str)s"), log_info)
            LOG.debug('Exception', exc_info=exc)

        method = functools.partial(method, exchange, msg, routing_key, timeout)

        with self._connection_lock:
            self.ensure(method, retry=retry, error_callback=_error_callback)

    def _publish(self, exchange, msg, routing_key=None, timeout=None):
        """Publish a message."""
        producer = kombu.messaging.Producer(exchange=exchange,
                                            channel=self.channel,
                                            auto_declare=not exchange.passive,
                                            routing_key=routing_key)

        # NOTE(sileht): no need to wait more, caller expects
        # a answer before timeout is reached
        transport_timeout = timeout

        heartbeat_timeout = self.heartbeat_timeout_threshold
        if (self._heartbeat_supported_and_enabled() and (
                transport_timeout is None or
                transport_timeout > heartbeat_timeout)):
            # NOTE(sileht): we are supposed to send heartbeat every
            # heartbeat_timeout, no need to wait more otherwise will
            # disconnect us, so raise timeout earlier ourself
            transport_timeout = heartbeat_timeout

        log_info = {'msg': msg,
                    'who': exchange or 'default',
                    'key': routing_key}
        LOG.trace('Connection._publish: sending message %(msg)s to'
                  ' %(who)s with routing key %(key)s', log_info)
        with self._transport_socket_timeout(transport_timeout):
            producer.publish(msg, expiration=timeout)

    # List of notification queue declared on the channel to avoid
    # unnecessary redeclaration. This list is resetted each time
    # the connection is resetted in Connection._set_current_channel
    PUBLISHER_DECLARED_QUEUES = collections.defaultdict(set)

    def _publish_and_creates_default_queue(self, exchange, msg,
                                           routing_key=None, timeout=None):
        """Publisher that declares a default queue

        When the exchange is missing instead of silently creates an exchange
        not binded to a queue, this publisher creates a default queue
        named with the routing_key

        This is mainly used to not miss notification in case of nobody consumes
        them yet. If the future consumer bind the default queue it can retrieve
        missing messages.

        _set_current_channel is responsible to cleanup the cache.
        """
        queue_indentifier = (exchange.name, routing_key)
        # NOTE(sileht): We only do it once per reconnection
        # the Connection._set_current_channel() is responsible to clear
        # this cache
        if (queue_indentifier not in
                self.PUBLISHER_DECLARED_QUEUES[self.channel]):
            queue = kombu.entity.Queue(
                channel=self.channel,
                exchange=exchange,
                durable=exchange.durable,
                auto_delete=exchange.auto_delete,
                name=routing_key,
                routing_key=routing_key,
                queue_arguments=_get_queue_arguments(self.rabbit_ha_queues))
            log_info = {'key': routing_key, 'exchange': exchange}
            LOG.trace(
                'Connection._publish_and_creates_default_queue: '
                'declare queue %(key)s on %(exchange)s exchange', log_info)
            queue.declare()
            self.PUBLISHER_DECLARED_QUEUES[self.channel].add(queue_indentifier)

        self._publish(exchange, msg, routing_key=routing_key, timeout=timeout)

    def _publish_and_raises_on_missing_exchange(self, exchange, msg,
                                                routing_key=None,
                                                timeout=None):
        """Publisher that raises exception if exchange is missing."""
        if not exchange.passive:
            raise RuntimeError("_publish_and_retry_on_missing_exchange() must "
                               "be called with an passive exchange.")

        try:
            self._publish(exchange, msg, routing_key=routing_key,
                          timeout=timeout)
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
                    "exchange %s doesn't exists" % exchange.name)
            raise

    def direct_send(self, msg_id, msg):
        """Send a 'direct' message."""
        exchange = kombu.entity.Exchange(name=msg_id,
                                         type='direct',
                                         durable=False,
                                         auto_delete=True,
                                         passive=True)

        self._ensure_publishing(self._publish_and_raises_on_missing_exchange,
                                exchange, msg, routing_key=msg_id)

    def topic_send(self, exchange_name, topic, msg, timeout=None, retry=None):
        """Send a 'topic' message."""
        exchange = kombu.entity.Exchange(
            name=exchange_name,
            type='topic',
            durable=self.amqp_durable_queues,
            auto_delete=self.amqp_auto_delete)

        self._ensure_publishing(self._publish, exchange, msg,
                                routing_key=topic, retry=retry)

    def fanout_send(self, topic, msg, retry=None):
        """Send a 'fanout' message."""
        exchange = kombu.entity.Exchange(name='%s_fanout' % topic,
                                         type='fanout',
                                         durable=False,
                                         auto_delete=True)

        self._ensure_publishing(self._publish, exchange, msg, retry=retry)

    def notify_send(self, exchange_name, topic, msg, retry=None, **kwargs):
        """Send a notify message on a topic."""
        exchange = kombu.entity.Exchange(
            name=exchange_name,
            type='topic',
            durable=self.amqp_durable_queues,
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
        conf.register_opts(base.base_opts, group=opt_group)

        self.missing_destination_retry_timeout = (
            conf.oslo_messaging_rabbit.kombu_missing_consumer_retry_timeout)

        connection_pool = pool.ConnectionPool(
            conf, conf.oslo_messaging_rabbit.rpc_conn_pool_size,
            url, Connection)

        super(RabbitDriver, self).__init__(
            conf, url,
            connection_pool,
            default_exchange,
            allowed_remote_exmods
        )

    def require_features(self, requeue=True):
        pass
