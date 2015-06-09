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

import contextlib
import functools
import itertools
import logging
import os
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
from oslo_utils import netutils
import six
from six.moves.urllib import parse

from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._i18n import _
from oslo_messaging._i18n import _LE
from oslo_messaging._i18n import _LI
from oslo_messaging._i18n import _LW
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
    cfg.StrOpt('rabbit_host',
               default='localhost',
               deprecated_group='DEFAULT',
               help='The RabbitMQ broker address where a single node is '
                    'used.'),
    cfg.IntOpt('rabbit_port',
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
               default=0,
               help="Number of seconds after which the Rabbit broker is "
               "considered down if heartbeat's keep-alive fails "
               "(0 disables the heartbeat, >0 enables it. Enabling heartbeats "
               "requires kombu>=3.0.7 and amqp>=1.4.0). EXPERIMENTAL"),
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


def _get_queue_arguments(conf):
    """Construct the arguments for declaring a queue.

    If the rabbit_ha_queues option is set, we declare a mirrored queue
    as described here:

      http://www.rabbitmq.com/ha.html

    Setting x-ha-policy to all means that the queue will be mirrored
    to all nodes in the cluster.
    """
    return {'x-ha-policy': 'all'} if conf.rabbit_ha_queues else {}


class RabbitMessage(dict):
    def __init__(self, raw_message):
        super(RabbitMessage, self).__init__(
            rpc_common.deserialize_msg(raw_message.payload))
        self._raw_message = raw_message

    def acknowledge(self):
        self._raw_message.ack()

    def requeue(self):
        self._raw_message.requeue()


class ConsumerBase(object):
    """Consumer base class."""

    def __init__(self, channel, callback, tag, **kwargs):
        """Declare a queue on an amqp channel.

        'channel' is the amqp channel to use
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the channel

        queue name, exchange name, and other kombu options are
        passed in here as a dictionary.
        """
        self.callback = callback
        self.tag = six.text_type(tag)
        self.kwargs = kwargs
        self.queue = None
        self.reconnect(channel)

    def reconnect(self, channel):
        """Re-declare the queue after a rabbit reconnect."""
        self.channel = channel
        self.kwargs['channel'] = channel
        self.queue = kombu.entity.Queue(**self.kwargs)
        try:
            self.queue.declare()
        except Exception as e:
            # NOTE: This exception may be triggered by a race condition.
            # Simply retrying will solve the error most of the time and
            # should work well enough as a workaround until the race condition
            # itself can be fixed.
            # TODO(jrosenboom): In order to be able to match the Exception
            # more specifically, we have to refactor ConsumerBase to use
            # 'channel_errors' of the kombu connection object that
            # has created the channel.
            # See https://bugs.launchpad.net/neutron/+bug/1318721 for details.
            LOG.error(_("Declaring queue failed with (%s), retrying"), e)
            self.queue.declare()

    def _callback_handler(self, message, callback):
        """Call callback with deserialized message.

        Messages that are processed and ack'ed.
        """

        try:
            callback(RabbitMessage(message))
        except Exception:
            LOG.exception(_("Failed to process message"
                            " ... skipping it."))
            message.ack()

    def consume(self, *args, **kwargs):
        """Actually declare the consumer on the amqp channel.  This will
        start the flow of messages from the queue.  Using the
        Connection.iterconsume() iterator will process the messages,
        calling the appropriate callback.

        If a callback is specified in kwargs, use that.  Otherwise,
        use the callback passed during __init__()

        If kwargs['nowait'] is True, then this call will block until
        a message is read.

        """

        options = {'consumer_tag': self.tag}
        options['nowait'] = kwargs.get('nowait', False)
        callback = kwargs.get('callback', self.callback)
        if not callback:
            raise ValueError("No callback defined")

        def _callback(message):
            m2p = getattr(self.channel, 'message_to_python', None)
            if m2p:
                message = m2p(message)
            self._callback_handler(message, callback)

        self.queue.consume(*args, callback=_callback, **options)

    def cancel(self):
        """Cancel the consuming from the queue, if it has started."""
        try:
            self.queue.cancel(self.tag)
        except KeyError as e:
            # NOTE(comstud): Kludge to get around a amqplib bug
            if six.text_type(e) != "u'%s'" % self.tag:
                raise
        self.queue = None


class DirectConsumer(ConsumerBase):
    """Queue/consumer class for 'direct'."""

    def __init__(self, conf, channel, msg_id, callback, tag, **kwargs):
        """Init a 'direct' queue.

        'channel' is the amqp channel to use
        'msg_id' is the msg_id to listen on
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the channel

        Other kombu options may be passed
        """
        # Default options
        options = {'durable': False,
                   'queue_arguments': _get_queue_arguments(conf),
                   'auto_delete': True,
                   'exclusive': False}
        options.update(kwargs)
        exchange = kombu.entity.Exchange(name=msg_id,
                                         type='direct',
                                         durable=options['durable'],
                                         auto_delete=options['auto_delete'])
        super(DirectConsumer, self).__init__(channel,
                                             callback,
                                             tag,
                                             name=msg_id,
                                             exchange=exchange,
                                             routing_key=msg_id,
                                             **options)


class TopicConsumer(ConsumerBase):
    """Consumer class for 'topic'."""

    def __init__(self, conf, channel, topic, callback, tag, exchange_name,
                 name=None, **kwargs):
        """Init a 'topic' queue.

        :param channel: the amqp channel to use
        :param topic: the topic to listen on
        :paramtype topic: str
        :param callback: the callback to call when messages are received
        :param tag: a unique ID for the consumer on the channel
        :param exchange_name: the exchange name to use
        :param name: optional queue name, defaults to topic
        :paramtype name: str

        Other kombu options may be passed as keyword arguments
        """
        # Default options
        options = {'durable': conf.amqp_durable_queues,
                   'queue_arguments': _get_queue_arguments(conf),
                   'auto_delete': conf.amqp_auto_delete,
                   'exclusive': False}
        options.update(kwargs)
        exchange = kombu.entity.Exchange(name=exchange_name,
                                         type='topic',
                                         durable=options['durable'],
                                         auto_delete=options['auto_delete'])
        super(TopicConsumer, self).__init__(channel,
                                            callback,
                                            tag,
                                            name=name or topic,
                                            exchange=exchange,
                                            routing_key=topic,
                                            **options)


class FanoutConsumer(ConsumerBase):
    """Consumer class for 'fanout'."""

    def __init__(self, conf, channel, topic, callback, tag, **kwargs):
        """Init a 'fanout' queue.

        'channel' is the amqp channel to use
        'topic' is the topic to listen on
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the channel

        Other kombu options may be passed
        """
        unique = uuid.uuid4().hex
        exchange_name = '%s_fanout' % topic
        queue_name = '%s_fanout_%s' % (topic, unique)

        # Default options
        options = {'durable': False,
                   'queue_arguments': _get_queue_arguments(conf),
                   'auto_delete': True,
                   'exclusive': False}
        options.update(kwargs)
        exchange = kombu.entity.Exchange(name=exchange_name, type='fanout',
                                         durable=options['durable'],
                                         auto_delete=options['auto_delete'])
        super(FanoutConsumer, self).__init__(channel, callback, tag,
                                             name=queue_name,
                                             exchange=exchange,
                                             routing_key=topic,
                                             **options)


class Publisher(object):
    """Publisher that silently creates exchange but no queues."""

    passive = False

    def __init__(self, conf, exchange_name, routing_key, type, durable,
                 auto_delete):
        """Init the Publisher class with the exchange_name, routing_key,
        type, durable auto_delete
        """
        self.queue_arguments = _get_queue_arguments(conf)
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.auto_delete = auto_delete
        self.durable = durable
        self.exchange = kombu.entity.Exchange(name=self.exchange_name,
                                              type=type,
                                              exclusive=False,
                                              durable=durable,
                                              auto_delete=auto_delete,
                                              passive=self.passive)

    def send(self, conn, msg, timeout=None):
        """Send a message on an channel."""
        producer = kombu.messaging.Producer(exchange=self.exchange,
                                            channel=conn.channel,
                                            routing_key=self.routing_key)

        headers = {}
        if timeout:
            # AMQP TTL is in milliseconds when set in the property.
            # Details: http://www.rabbitmq.com/ttl.html#per-message-ttl
            # NOTE(sileht): this amqp header doesn't exists ... LP#1444854
            headers['ttl'] = timeout * 1000

        # NOTE(sileht): no need to wait more, caller expects
        # a answer before timeout is reached
        transport_timeout = timeout

        heartbeat_timeout = conn.driver_conf.heartbeat_timeout_threshold
        if (conn._heartbeat_supported_and_enabled() and (
                transport_timeout is None or
                transport_timeout > heartbeat_timeout)):
            # NOTE(sileht): we are supposed to send heartbeat every
            # heartbeat_timeout, no need to wait more otherwise will
            # disconnect us, so raise timeout earlier ourself
            transport_timeout = heartbeat_timeout

        with conn._transport_socket_timeout(transport_timeout):
            producer.publish(msg, headers=headers)


class DeclareQueuePublisher(Publisher):
    """Publisher that declares a default queue

    When the exchange is missing instead of silently creating an exchange
    not binded to a queue, this publisher creates a default queue
    named with the routing_key.

    This is mainly used to not miss notifications in case of nobody consumes
    them yet. If the future consumer binds the default queue it can retrieve
    missing messages.
    """
    # FIXME(sileht): The side effect of this is that we declare again and
    # again the same queue, and generate a lot of useless rabbit traffic.
    # https://bugs.launchpad.net/oslo.messaging/+bug/1437902

    def send(self, conn, msg, timeout=None):
        queue = kombu.entity.Queue(
            channel=conn.channel,
            exchange=self.exchange,
            durable=self.durable,
            auto_delete=self.auto_delete,
            name=self.routing_key,
            routing_key=self.routing_key,
            queue_arguments=self.queue_arguments)
        queue.declare()
        super(DeclareQueuePublisher, self).send(
            conn, msg, timeout)


class RetryOnMissingExchangePublisher(Publisher):
    """Publisher that retry during 60 seconds if the exchange is missing."""

    passive = True

    def send(self, conn, msg, timeout=None):
        # TODO(sileht):
        # * use timeout parameter when available
        # * use rpc_timeout if not instead of hardcoded 60
        # * use @retrying
        timer = rpc_common.DecayingTimer(duration=60)
        timer.start()

        while True:
            try:
                super(RetryOnMissingExchangePublisher, self).send(conn, msg,
                                                                  timeout)
                return
            except conn.connection.channel_errors as exc:
                # NOTE(noelbk/sileht):
                # If rabbit dies, the consumer can be disconnected before the
                # publisher sends, and if the consumer hasn't declared the
                # queue, the publisher's will send a message to an exchange
                # that's not bound to a queue, and the message wll be lost.
                # So we set passive=True to the publisher exchange and catch
                # the 404 kombu ChannelError and retry until the exchange
                # appears
                if exc.code == 404 and timer.check_return() > 0:
                    LOG.info(_LI("The exchange %(exchange)s to send to "
                                 "%(routing_key)s doesn't exist yet, "
                                 "retrying...") % {
                                     'exchange': self.exchange,
                                     'routing_key': self.routing_key})
                    time.sleep(1)
                    continue
                raise


class DummyConnectionLock(object):
    def acquire(self):
        pass

    def release(self):
        pass

    def heartbeat_acquire(self):
        pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


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
        self._get_thread_id = self._fetch_current_thread_functor()

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

    @staticmethod
    def _fetch_current_thread_functor():
        # Until https://github.com/eventlet/eventlet/issues/172 is resolved
        # or addressed we have to use complicated workaround to get a object
        # that will not be recycled; the usage of threading.current_thread()
        # doesn't appear to currently be monkey patched and therefore isn't
        # reliable to use (and breaks badly when used as all threads share
        # the same current_thread() object)...
        try:
            import eventlet
            from eventlet import patcher
            green_threaded = patcher.is_monkey_patched('thread')
        except ImportError:
            green_threaded = False
        if green_threaded:
            return lambda: eventlet.getcurrent()
        else:
            return lambda: threading.current_thread()


class Connection(object):
    """Connection object."""

    pools = {}

    def __init__(self, conf, url, purpose):
        self.consumers = []
        self.consumer_num = itertools.count(1)
        self.conf = conf
        self.driver_conf = self.conf.oslo_messaging_rabbit
        self.max_retries = self.driver_conf.rabbit_max_retries
        # Try forever?
        if self.max_retries <= 0:
            self.max_retries = None
        self.interval_start = self.driver_conf.rabbit_retry_interval
        self.interval_stepping = self.driver_conf.rabbit_retry_backoff
        # max retry-interval = 30 seconds
        self.interval_max = 30

        self._login_method = self.driver_conf.rabbit_login_method

        if url.virtual_host is not None:
            virtual_host = url.virtual_host
        else:
            virtual_host = self.driver_conf.rabbit_virtual_host

        self._url = ''
        if self.driver_conf.fake_rabbit:
            LOG.warn("Deprecated: fake_rabbit option is deprecated, set "
                     "rpc_backend to kombu+memory or use the fake "
                     "driver instead.")
            self._url = 'memory://%s/' % virtual_host
        elif url.hosts:
            if url.transport.startswith('kombu+'):
                LOG.warn(_LW('Selecting the kombu transport through the '
                             'transport url (%s) is a experimental feature '
                             'and this is not yet supported.') % url.transport)
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
            for adr in self.driver_conf.rabbit_hosts:
                hostname, port = netutils.parse_host_port(
                    adr, default_port=self.driver_conf.rabbit_port)
                self._url += '%samqp://%s:%s@%s:%s/%s' % (
                    ";" if self._url else '',
                    parse.quote(self.driver_conf.rabbit_userid),
                    parse.quote(self.driver_conf.rabbit_password),
                    self._parse_url_hostname(hostname), port,
                    virtual_host)

        self._initial_pid = os.getpid()

        self.do_consume = True
        self._consume_loop_stopped = False
        self.channel = None

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # we don't need the lock because we don't
        # have a heartbeat thread
        if purpose == rpc_amqp.PURPOSE_SEND:
            self._connection_lock = ConnectionLock()
        else:
            self._connection_lock = DummyConnectionLock()

        self.connection = kombu.connection.Connection(
            self._url, ssl=self._fetch_ssl_params(),
            login_method=self._login_method,
            failover_strategy="shuffle",
            heartbeat=self.driver_conf.heartbeat_timeout_threshold,
            transport_options={'confirm_publish': True})

        LOG.info(_LI('Connecting to AMQP server on %(hostname)s:%(port)d'),
                 self.connection.info())

        # NOTE(sileht): kombu recommend to run heartbeat_check every
        # seconds, but we use a lock around the kombu connection
        # so, to not lock to much this lock to most of the time do nothing
        # expected waiting the events drain, we start heartbeat_check and
        # retreive the server heartbeat packet only two times more than
        # the minimum required for the heartbeat works
        # (heatbeat_timeout/heartbeat_rate/2.0, default kombu
        # heartbeat_rate is 2)
        self._heartbeat_wait_timeout = (
            float(self.driver_conf.heartbeat_timeout_threshold) /
            float(self.driver_conf.heartbeat_rate) / 2.0)
        self._heartbeat_support_log_emitted = False

        # NOTE(sileht): just ensure the connection is setuped at startup
        self.ensure_connection()

        # NOTE(sileht): if purpose is PURPOSE_LISTEN
        # the consume code does the heartbeat stuff
        # we don't need a thread
        self._heartbeat_thread = None
        if purpose == rpc_amqp.PURPOSE_SEND:
            self._heartbeat_start()

        LOG.info(_LI('Connected to AMQP server on %(hostname)s:%(port)d'),
                 self.connection.info())

        # NOTE(sileht): value choosen according the best practice from kombu
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
        if self.driver_conf.rabbit_use_ssl:
            ssl_params = dict()

            # http://docs.python.org/library/ssl.html - ssl.wrap_socket
            if self.driver_conf.kombu_ssl_version:
                ssl_params['ssl_version'] = self.validate_ssl_version(
                    self.driver_conf.kombu_ssl_version)
            if self.driver_conf.kombu_ssl_keyfile:
                ssl_params['keyfile'] = self.driver_conf.kombu_ssl_keyfile
            if self.driver_conf.kombu_ssl_certfile:
                ssl_params['certfile'] = self.driver_conf.kombu_ssl_certfile
            if self.driver_conf.kombu_ssl_ca_certs:
                ssl_params['ca_certs'] = self.driver_conf.kombu_ssl_ca_certs
                # We might want to allow variations in the
                # future with this?
                ssl_params['cert_reqs'] = ssl.CERT_REQUIRED
            return ssl_params or True
        return False

    def ensure_connection(self):
        self.ensure(method=lambda: True)

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
            LOG.warn("Process forked after connection established! "
                     "This can result in unpredictable behavior. "
                     "See: http://docs.openstack.org/developer/"
                     "oslo_messaging/transport.html")
            self._initial_pid = current_pid

        if retry is None:
            retry = self.max_retries
        if retry is None or retry < 0:
            retry = None

        def on_error(exc, interval):
            LOG.debug(_("Received recoverable error from kombu:"),
                      exc_info=True)

            recoverable_error_callback and recoverable_error_callback(exc)

            interval = (self.driver_conf.kombu_reconnect_delay + interval
                        if self.driver_conf.kombu_reconnect_delay > 0
                        else interval)

            info = {'err_str': exc, 'sleep_time': interval}
            info.update(self.connection.info())

            if 'Socket closed' in six.text_type(exc):
                LOG.error(_LE('AMQP server %(hostname)s:%(port)d closed'
                              ' the connection. Check login credentials:'
                              ' %(err_str)s'), info)
            else:
                LOG.error(_LE('AMQP server on %(hostname)s:%(port)d is '
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
            if self.driver_conf.kombu_reconnect_delay > 0:
                time.sleep(self.driver_conf.kombu_reconnect_delay)

        def on_reconnection(new_channel):
            """Callback invoked when the kombu reconnects and creates
            a new channel, we use it the reconfigure our consumers.
            """
            self._set_current_channel(new_channel)
            self.consumer_num = itertools.count(1)
            for consumer in self.consumers:
                consumer.reconnect(new_channel)

            LOG.info(_LI('Reconnected to AMQP server on '
                         '%(hostname)s:%(port)d'),
                     {'hostname': self.connection.hostname,
                      'port': self.connection.port})

        def execute_method(channel):
            self._set_current_channel(channel)
            method()

        recoverable_errors = (self.connection.recoverable_channel_errors +
                              self.connection.recoverable_connection_errors)

        try:
            autoretry_method = self.connection.autoretry(
                execute_method, channel=self.channel,
                max_retries=retry,
                errback=on_error,
                interval_start=self.interval_start or 1,
                interval_step=self.interval_stepping,
                on_revive=on_reconnection,
            )
            ret, channel = autoretry_method()
            self._set_current_channel(channel)
            return ret
        except recoverable_errors as exc:
            LOG.debug(_("Received recoverable error from kombu:"),
                      exc_info=True)
            error_callback and error_callback(exc)
            self._set_current_channel(None)
            # NOTE(sileht): number of retry exceeded and the connection
            # is still broken
            msg = _('Unable to connect to AMQP server on '
                    '%(hostname)s:%(port)d after %(retry)d '
                    'tries: %(err_str)s') % {
                        'hostname': self.connection.hostname,
                        'port': self.connection.port,
                        'err_str': exc,
                        'retry': retry}
            LOG.error(msg)
            raise exceptions.MessageDeliveryFailure(msg)
        except Exception as exc:
            error_callback and error_callback(exc)
            raise

    def _set_current_channel(self, new_channel):
        """Change the channel to use.

        NOTE(sileht): Must be called within the connection lock
        """
        if self.channel is not None and new_channel != self.channel:
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
                self._set_current_channel(self.connection.channel())
            except recoverable_errors:
                self._set_current_channel(None)
                self.ensure_connection()
        self.consumers = []
        self.consumer_num = itertools.count(1)

    def _heartbeat_supported_and_enabled(self):
        if self.driver_conf.heartbeat_timeout_threshold <= 0:
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
                self.driver_conf.heartbeat_timeout_threshold):
            self.connection.heartbeat_check(
                rate=self.driver_conf.heartbeat_rate)

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

    def declare_consumer(self, consumer_cls, topic, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """

        def _connect_error(exc):
            log_info = {'topic': topic, 'err_str': exc}
            LOG.error(_("Failed to declare consumer for topic '%(topic)s': "
                      "%(err_str)s"), log_info)

        def _declare_consumer():
            consumer = consumer_cls(self.driver_conf, self.channel, topic,
                                    callback, six.next(self.consumer_num))
            self.consumers.append(consumer)
            return consumer

        with self._connection_lock:
            return self.ensure(_declare_consumer,
                               error_callback=_connect_error)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers.

        NOTE(sileht): Must be called within the connection lock
        """

        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()

        def _raise_timeout(exc):
            LOG.debug('Timed out waiting for RPC response: %s', exc)
            raise rpc_common.Timeout()

        def _recoverable_error_callback(exc):
            self.do_consume = True
            timer.check_return(_raise_timeout, exc)

        def _error_callback(exc):
            _recoverable_error_callback(exc)
            LOG.error(_('Failed to consume message from queue: %s'),
                      exc)

        def _consume():
            if self.do_consume:
                queues_head = self.consumers[:-1]  # not fanout.
                queues_tail = self.consumers[-1]  # fanout
                for queue in queues_head:
                    queue.consume(nowait=True)
                queues_tail.consume(nowait=False)
                self.do_consume = False

            poll_timeout = (self._poll_timeout if timeout is None
                            else min(timeout, self._poll_timeout))
            while True:
                if self._consume_loop_stopped:
                    self._consume_loop_stopped = False
                    raise StopIteration

                if self._heartbeat_supported_and_enabled():
                    self._heartbeat_check()

                try:
                    return self.connection.drain_events(timeout=poll_timeout)
                except socket.timeout as exc:
                    poll_timeout = timer.check_return(
                        _raise_timeout, exc, maximum=self._poll_timeout)

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(
                _consume,
                recoverable_error_callback=_recoverable_error_callback,
                error_callback=_error_callback)

    def publisher_send(self, publisher, msg, timeout=None, retry=None):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            log_info = {'topic': publisher.exchange_name, 'err_str': exc}
            LOG.error(_("Failed to publish message to topic "
                        "'%(topic)s': %(err_str)s"), log_info)
            LOG.debug('Exception', exc_info=exc)

        def _publish():
            publisher.send(self, msg, timeout)

        with self._connection_lock:
            self.ensure(_publish, retry=retry, error_callback=_error_callback)

    def declare_direct_consumer(self, topic, callback):
        """Create a 'direct' queue.
        In nova's use, this is generally a msg_id queue used for
        responses for call/multicall
        """
        self.declare_consumer(DirectConsumer, topic, callback)

    def declare_topic_consumer(self, exchange_name, topic, callback=None,
                               queue_name=None):
        """Create a 'topic' consumer."""
        self.declare_consumer(functools.partial(TopicConsumer,
                                                name=queue_name,
                                                exchange_name=exchange_name,
                                                ),
                              topic, callback)

    def declare_fanout_consumer(self, topic, callback):
        """Create a 'fanout' consumer."""
        self.declare_consumer(FanoutConsumer, topic, callback)

    def direct_send(self, msg_id, msg):
        """Send a 'direct' message."""

        p = RetryOnMissingExchangePublisher(self.driver_conf,
                                            exchange_name=msg_id,
                                            routing_key=msg_id,
                                            type='direct',
                                            durable=False,
                                            auto_delete=True)

        self.publisher_send(p, msg)

    def topic_send(self, exchange_name, topic, msg, timeout=None, retry=None):
        """Send a 'topic' message."""
        p = Publisher(self.driver_conf,
                      exchange_name=exchange_name,
                      routing_key=topic,
                      type='topic',
                      durable=self.driver_conf.amqp_durable_queues,
                      auto_delete=self.driver_conf.amqp_auto_delete)
        self.publisher_send(p, msg, timeout, retry=retry)

    def fanout_send(self, topic, msg, retry=None):
        """Send a 'fanout' message."""

        p = Publisher(self.driver_conf,
                      exchange_name='%s_fanout' % topic,
                      routing_key=None,
                      type='fanout',
                      durable=False,
                      auto_delete=True)

        self.publisher_send(p, msg, retry=retry)

    def notify_send(self, exchange_name, topic, msg, retry=None, **kwargs):
        """Send a notify message on a topic."""
        p = DeclareQueuePublisher(
            self.driver_conf,
            exchange_name=exchange_name,
            routing_key=topic,
            type='topic',
            durable=self.driver_conf.amqp_durable_queues,
            auto_delete=self.driver_conf.amqp_auto_delete)

        self.publisher_send(p, msg, timeout=None, retry=retry)

    def consume(self, limit=None, timeout=None):
        """Consume from all queues/consumers."""
        with self._connection_lock:
            it = self.iterconsume(limit=limit, timeout=timeout)
            while True:
                try:
                    six.next(it)
                except StopIteration:
                    return

    def stop_consuming(self):
        self._consume_loop_stopped = True


class RabbitDriver(amqpdriver.AMQPDriverBase):

    def __init__(self, conf, url,
                 default_exchange=None,
                 allowed_remote_exmods=None):
        opt_group = cfg.OptGroup(name='oslo_messaging_rabbit',
                                 title='RabbitMQ driver options')
        conf.register_group(opt_group)
        conf.register_opts(rabbit_opts, group=opt_group)
        conf.register_opts(rpc_amqp.amqp_opts, group=opt_group)

        connection_pool = rpc_amqp.ConnectionPool(
            conf, conf.oslo_messaging_rabbit.rpc_conn_pool_size,
            url, Connection)

        super(RabbitDriver, self).__init__(conf, url,
                                           connection_pool,
                                           default_exchange,
                                           allowed_remote_exmods)

    def require_features(self, requeue=True):
        pass
