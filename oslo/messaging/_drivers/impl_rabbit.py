# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import functools
import itertools
import logging
import Queue
import socket
import ssl
import threading
import time
import uuid

import eventlet
import greenlet
import kombu
import kombu.connection
import kombu.entity
import kombu.messaging
from oslo.config import cfg

from oslo.messaging._drivers import amqp as rpc_amqp
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common as rpc_common
from oslo.messaging import _urls as urls
from oslo.messaging.openstack.common import excutils
from oslo.messaging.openstack.common import network_utils
from oslo.messaging.openstack.common import sslutils

# FIXME(markmc): remove this
_ = lambda s: s

# FIXME(markmc): these were toplevel in openstack.common.rpc
rabbit_opts = [
    cfg.IntOpt('rpc_conn_pool_size',
               default=30,
               help='Size of RPC connection pool'),
    cfg.BoolOpt('fake_rabbit',
                default=False,
                help='If passed, use a fake RabbitMQ provider'),
]

kombu_opts = [
    cfg.StrOpt('kombu_ssl_version',
               default='',
               help='SSL version to use (valid only if SSL enabled). '
                    'valid values are TLSv1, SSLv23 and SSLv3. SSLv2 may '
                    'be available on some distributions'
               ),
    cfg.StrOpt('kombu_ssl_keyfile',
               default='',
               help='SSL key file (valid only if SSL enabled)'),
    cfg.StrOpt('kombu_ssl_certfile',
               default='',
               help='SSL cert file (valid only if SSL enabled)'),
    cfg.StrOpt('kombu_ssl_ca_certs',
               default='',
               help=('SSL certification authority file '
                     '(valid only if SSL enabled)')),
    cfg.StrOpt('rabbit_host',
               default='localhost',
               help='The RabbitMQ broker address where a single node is used'),
    cfg.IntOpt('rabbit_port',
               default=5672,
               help='The RabbitMQ broker port where a single node is used'),
    cfg.ListOpt('rabbit_hosts',
                default=['$rabbit_host:$rabbit_port'],
                help='RabbitMQ HA cluster host:port pairs'),
    cfg.BoolOpt('rabbit_use_ssl',
                default=False,
                help='connect over SSL for RabbitMQ'),
    cfg.StrOpt('rabbit_userid',
               default='guest',
               help='the RabbitMQ userid'),
    cfg.StrOpt('rabbit_password',
               default='guest',
               help='the RabbitMQ password',
               secret=True),
    cfg.StrOpt('rabbit_virtual_host',
               default='/',
               help='the RabbitMQ virtual host'),
    cfg.IntOpt('rabbit_retry_interval',
               default=1,
               help='how frequently to retry connecting with RabbitMQ'),
    cfg.IntOpt('rabbit_retry_backoff',
               default=2,
               help='how long to backoff for between retries when connecting '
                    'to RabbitMQ'),
    cfg.IntOpt('rabbit_max_retries',
               default=0,
               help='maximum retries with trying to connect to RabbitMQ '
                    '(the default of 0 implies an infinite retry count)'),
    cfg.BoolOpt('rabbit_ha_queues',
                default=False,
                help='use H/A queues in RabbitMQ (x-ha-policy: all).'
                     'You need to wipe RabbitMQ database when '
                     'changing this option.'),

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
        self.tag = str(tag)
        self.kwargs = kwargs
        self.queue = None
        self.ack_on_error = kwargs.get('ack_on_error', True)
        self.reconnect(channel)

    def reconnect(self, channel):
        """Re-declare the queue after a rabbit reconnect."""
        self.channel = channel
        self.kwargs['channel'] = channel
        self.queue = kombu.entity.Queue(**self.kwargs)
        self.queue.declare()

    def _callback_handler(self, message, callback):
        """Call callback with deserialized message.

        Messages that are processed without exception are ack'ed.

        If the message processing generates an exception, it will be
        ack'ed if ack_on_error=True. Otherwise it will be .reject()'ed.
        Rejection is better than waiting for the message to timeout.
        Rejected messages are immediately requeued.
        """

        ack_msg = False
        try:
            msg = rpc_common.deserialize_msg(message.payload)
            callback(msg)
            ack_msg = True
        except Exception:
            if self.ack_on_error:
                ack_msg = True
                LOG.exception(_("Failed to process message"
                                " ... skipping it."))
            else:
                LOG.exception(_("Failed to process message"
                                " ... will requeue."))
        finally:
            if ack_msg:
                message.ack()
            else:
                message.reject()

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

        def _callback(raw_message):
            message = self.channel.message_to_python(raw_message)
            self._callback_handler(message, callback)

        self.queue.consume(*args, callback=_callback, **options)

    def cancel(self):
        """Cancel the consuming from the queue, if it has started."""
        try:
            self.queue.cancel(self.tag)
        except KeyError as e:
            # NOTE(comstud): Kludge to get around a amqplib bug
            if str(e) != "u'%s'" % self.tag:
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

    def __init__(self, conf, channel, topic, callback, tag, name=None,
                 exchange_name=None, **kwargs):
        """Init a 'topic' queue.

        :param channel: the amqp channel to use
        :param topic: the topic to listen on
        :paramtype topic: str
        :param callback: the callback to call when messages are received
        :param tag: a unique ID for the consumer on the channel
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
        exchange_name = exchange_name or rpc_amqp.get_control_exchange(conf)
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
    """Base Publisher class."""

    def __init__(self, channel, exchange_name, routing_key, **kwargs):
        """Init the Publisher class with the exchange_name, routing_key,
        and other options
        """
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.kwargs = kwargs
        self.reconnect(channel)

    def reconnect(self, channel):
        """Re-establish the Producer after a rabbit reconnection."""
        self.exchange = kombu.entity.Exchange(name=self.exchange_name,
                                              **self.kwargs)
        self.producer = kombu.messaging.Producer(exchange=self.exchange,
                                                 channel=channel,
                                                 routing_key=self.routing_key)

    def send(self, msg, timeout=None):
        """Send a message."""
        if timeout:
            #
            # AMQP TTL is in milliseconds when set in the header.
            #
            self.producer.publish(msg, headers={'ttl': (timeout * 1000)})
        else:
            self.producer.publish(msg)


class DirectPublisher(Publisher):
    """Publisher class for 'direct'."""
    def __init__(self, conf, channel, msg_id, **kwargs):
        """init a 'direct' publisher.

        Kombu options may be passed as keyword args to override defaults
        """

        options = {'durable': False,
                   'auto_delete': True,
                   'exclusive': False}
        options.update(kwargs)
        super(DirectPublisher, self).__init__(channel, msg_id, msg_id,
                                              type='direct', **options)


class TopicPublisher(Publisher):
    """Publisher class for 'topic'."""
    def __init__(self, conf, channel, topic, **kwargs):
        """init a 'topic' publisher.

        Kombu options may be passed as keyword args to override defaults
        """
        options = {'durable': conf.amqp_durable_queues,
                   'auto_delete': conf.amqp_auto_delete,
                   'exclusive': False}
        options.update(kwargs)
        exchange_name = rpc_amqp.get_control_exchange(conf)
        super(TopicPublisher, self).__init__(channel,
                                             exchange_name,
                                             topic,
                                             type='topic',
                                             **options)


class FanoutPublisher(Publisher):
    """Publisher class for 'fanout'."""
    def __init__(self, conf, channel, topic, **kwargs):
        """init a 'fanout' publisher.

        Kombu options may be passed as keyword args to override defaults
        """
        options = {'durable': False,
                   'auto_delete': True,
                   'exclusive': False}
        options.update(kwargs)
        super(FanoutPublisher, self).__init__(channel, '%s_fanout' % topic,
                                              None, type='fanout', **options)


class NotifyPublisher(TopicPublisher):
    """Publisher class for 'notify'."""

    def __init__(self, conf, channel, topic, **kwargs):
        self.durable = kwargs.pop('durable', conf.amqp_durable_queues)
        self.queue_arguments = _get_queue_arguments(conf)
        super(NotifyPublisher, self).__init__(conf, channel, topic, **kwargs)

    def reconnect(self, channel):
        super(NotifyPublisher, self).reconnect(channel)

        # NOTE(jerdfelt): Normally the consumer would create the queue, but
        # we do this to ensure that messages don't get dropped if the
        # consumer is started after we do
        queue = kombu.entity.Queue(channel=channel,
                                   exchange=self.exchange,
                                   durable=self.durable,
                                   name=self.routing_key,
                                   routing_key=self.routing_key,
                                   queue_arguments=self.queue_arguments)
        queue.declare()


class Connection(object):
    """Connection object."""

    pool = None

    def __init__(self, conf, server_params=None):
        self.consumers = []
        self.consumer_thread = None
        self.proxy_callbacks = []
        self.conf = conf
        self.max_retries = self.conf.rabbit_max_retries
        # Try forever?
        if self.max_retries <= 0:
            self.max_retries = None
        self.interval_start = self.conf.rabbit_retry_interval
        self.interval_stepping = self.conf.rabbit_retry_backoff
        # max retry-interval = 30 seconds
        self.interval_max = 30
        self.memory_transport = False

        if server_params is None:
            server_params = {}
        # Keys to translate from server_params to kombu params
        server_params_to_kombu_params = {'username': 'userid'}

        ssl_params = self._fetch_ssl_params()
        params_list = []
        for adr in self.conf.rabbit_hosts:
            hostname, port = network_utils.parse_host_port(
                adr, default_port=self.conf.rabbit_port)

            params = {
                'hostname': hostname,
                'port': port,
                'userid': self.conf.rabbit_userid,
                'password': self.conf.rabbit_password,
                'virtual_host': self.conf.rabbit_virtual_host,
            }

            for sp_key, value in server_params.iteritems():
                p_key = server_params_to_kombu_params.get(sp_key, sp_key)
                params[p_key] = value

            if self.conf.fake_rabbit:
                params['transport'] = 'memory'
            if self.conf.rabbit_use_ssl:
                params['ssl'] = ssl_params

            params_list.append(params)

        self.params_list = params_list

        self.memory_transport = self.conf.fake_rabbit

        self.connection = None
        self.do_consume = None
        self.reconnect()

    def _fetch_ssl_params(self):
        """Handles fetching what ssl params should be used for the connection
        (if any).
        """
        ssl_params = dict()

        # http://docs.python.org/library/ssl.html - ssl.wrap_socket
        if self.conf.kombu_ssl_version:
            ssl_params['ssl_version'] = sslutils.validate_ssl_version(
                self.conf.kombu_ssl_version)
        if self.conf.kombu_ssl_keyfile:
            ssl_params['keyfile'] = self.conf.kombu_ssl_keyfile
        if self.conf.kombu_ssl_certfile:
            ssl_params['certfile'] = self.conf.kombu_ssl_certfile
        if self.conf.kombu_ssl_ca_certs:
            ssl_params['ca_certs'] = self.conf.kombu_ssl_ca_certs
            # We might want to allow variations in the
            # future with this?
            ssl_params['cert_reqs'] = ssl.CERT_REQUIRED

        if not ssl_params:
            # Just have the default behavior
            return True
        else:
            # Return the extended behavior
            return ssl_params

    def _connect(self, params):
        """Connect to rabbit.  Re-establish any queues that may have
        been declared before if we are reconnecting.  Exceptions should
        be handled by the caller.
        """
        if self.connection:
            LOG.info(_("Reconnecting to AMQP server on "
                     "%(hostname)s:%(port)d") % params)
            try:
                self.connection.release()
            except self.connection_errors:
                pass
            # Setting this in case the next statement fails, though
            # it shouldn't be doing any network operations, yet.
            self.connection = None
        self.connection = kombu.connection.BrokerConnection(**params)
        self.connection_errors = self.connection.connection_errors
        if self.memory_transport:
            # Kludge to speed up tests.
            self.connection.transport.polling_interval = 0.0
        self.do_consume = True
        self.consumer_num = itertools.count(1)
        self.connection.connect()
        self.channel = self.connection.channel()
        # work around 'memory' transport bug in 1.1.3
        if self.memory_transport:
            self.channel._new_queue('ae.undeliver')
        for consumer in self.consumers:
            consumer.reconnect(self.channel)
        LOG.info(_('Connected to AMQP server on %(hostname)s:%(port)d') %
                 params)

    def reconnect(self):
        """Handles reconnecting and re-establishing queues.
        Will retry up to self.max_retries number of times.
        self.max_retries = 0 means to retry forever.
        Sleep between tries, starting at self.interval_start
        seconds, backing off self.interval_stepping number of seconds
        each attempt.
        """

        attempt = 0
        while True:
            params = self.params_list[attempt % len(self.params_list)]
            attempt += 1
            try:
                self._connect(params)
                return
            except (IOError, self.connection_errors) as e:
                pass
            except Exception as e:
                # NOTE(comstud): Unfortunately it's possible for amqplib
                # to return an error not covered by its transport
                # connection_errors in the case of a timeout waiting for
                # a protocol response.  (See paste link in LP888621)
                # So, we check all exceptions for 'timeout' in them
                # and try to reconnect in this case.
                if 'timeout' not in str(e):
                    raise

            log_info = {}
            log_info['err_str'] = str(e)
            log_info['max_retries'] = self.max_retries
            log_info.update(params)

            if self.max_retries and attempt == self.max_retries:
                msg = _('Unable to connect to AMQP server on '
                        '%(hostname)s:%(port)d after %(max_retries)d '
                        'tries: %(err_str)s') % log_info
                LOG.error(msg)
                raise rpc_common.RPCException(msg)

            if attempt == 1:
                sleep_time = self.interval_start or 1
            elif attempt > 1:
                sleep_time += self.interval_stepping
            if self.interval_max:
                sleep_time = min(sleep_time, self.interval_max)

            log_info['sleep_time'] = sleep_time
            LOG.error(_('AMQP server on %(hostname)s:%(port)d is '
                        'unreachable: %(err_str)s. Trying again in '
                        '%(sleep_time)d seconds.') % log_info)
            time.sleep(sleep_time)

    def ensure(self, error_callback, method, *args, **kwargs):
        while True:
            try:
                return method(*args, **kwargs)
            except (self.connection_errors, socket.timeout, IOError) as e:
                if error_callback:
                    error_callback(e)
            except Exception as e:
                # NOTE(comstud): Unfortunately it's possible for amqplib
                # to return an error not covered by its transport
                # connection_errors in the case of a timeout waiting for
                # a protocol response.  (See paste link in LP888621)
                # So, we check all exceptions for 'timeout' in them
                # and try to reconnect in this case.
                if 'timeout' not in str(e):
                    raise
                if error_callback:
                    error_callback(e)
            self.reconnect()

    def get_channel(self):
        """Convenience call for bin/clear_rabbit_queues."""
        return self.channel

    def close(self):
        """Close/release this connection."""
        self.cancel_consumer_thread()
        self.wait_on_proxy_callbacks()
        self.connection.release()
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        self.cancel_consumer_thread()
        self.wait_on_proxy_callbacks()
        self.channel.close()
        self.channel = self.connection.channel()
        # work around 'memory' transport bug in 1.1.3
        if self.memory_transport:
            self.channel._new_queue('ae.undeliver')
        self.consumers = []

    def declare_consumer(self, consumer_cls, topic, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """

        def _connect_error(exc):
            log_info = {'topic': topic, 'err_str': str(exc)}
            LOG.error(_("Failed to declare consumer for topic '%(topic)s': "
                      "%(err_str)s") % log_info)

        def _declare_consumer():
            consumer = consumer_cls(self.conf, self.channel, topic, callback,
                                    self.consumer_num.next())
            self.consumers.append(consumer)
            return consumer

        return self.ensure(_connect_error, _declare_consumer)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers."""

        def _error_callback(exc):
            if isinstance(exc, socket.timeout):
                LOG.debug(_('Timed out waiting for RPC response: %s') %
                          str(exc))
                raise rpc_common.Timeout()
            else:
                LOG.exception(_('Failed to consume message from queue: %s') %
                              str(exc))
                self.do_consume = True

        def _consume():
            if self.do_consume:
                queues_head = self.consumers[:-1]  # not fanout.
                queues_tail = self.consumers[-1]  # fanout
                for queue in queues_head:
                    queue.consume(nowait=True)
                queues_tail.consume(nowait=False)
                self.do_consume = False
            return self.connection.drain_events(timeout=timeout)

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(_error_callback, _consume)

    def cancel_consumer_thread(self):
        """Cancel a consumer thread."""
        if self.consumer_thread is not None:
            self.consumer_thread.kill()
            try:
                self.consumer_thread.wait()
            except greenlet.GreenletExit:
                pass
            self.consumer_thread = None

    def wait_on_proxy_callbacks(self):
        """Wait for all proxy callback threads to exit."""
        for proxy_cb in self.proxy_callbacks:
            proxy_cb.wait()

    def publisher_send(self, cls, topic, msg, timeout=None, **kwargs):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            log_info = {'topic': topic, 'err_str': str(exc)}
            LOG.exception(_("Failed to publish message to topic "
                          "'%(topic)s': %(err_str)s") % log_info)

        def _publish():
            publisher = cls(self.conf, self.channel, topic, **kwargs)
            publisher.send(msg, timeout)

        self.ensure(_error_callback, _publish)

    def declare_direct_consumer(self, topic, callback):
        """Create a 'direct' queue.
        In nova's use, this is generally a msg_id queue used for
        responses for call/multicall
        """
        self.declare_consumer(DirectConsumer, topic, callback)

    def declare_topic_consumer(self, topic, callback=None, queue_name=None,
                               exchange_name=None, ack_on_error=True):
        """Create a 'topic' consumer."""
        self.declare_consumer(functools.partial(TopicConsumer,
                                                name=queue_name,
                                                exchange_name=exchange_name,
                                                ack_on_error=ack_on_error,
                                                ),
                              topic, callback)

    def declare_fanout_consumer(self, topic, callback):
        """Create a 'fanout' consumer."""
        self.declare_consumer(FanoutConsumer, topic, callback)

    def direct_send(self, msg_id, msg):
        """Send a 'direct' message."""
        self.publisher_send(DirectPublisher, msg_id, msg)

    def topic_send(self, topic, msg, timeout=None):
        """Send a 'topic' message."""
        self.publisher_send(TopicPublisher, topic, msg, timeout)

    def fanout_send(self, topic, msg):
        """Send a 'fanout' message."""
        self.publisher_send(FanoutPublisher, topic, msg)

    def notify_send(self, topic, msg, **kwargs):
        """Send a notify message on a topic."""
        self.publisher_send(NotifyPublisher, topic, msg, None, **kwargs)

    def consume(self, limit=None):
        """Consume from all queues/consumers."""
        it = self.iterconsume(limit=limit)
        while True:
            try:
                it.next()
            except StopIteration:
                return

    def consume_in_thread(self):
        """Consumer from all queues/consumers in a greenthread."""
        @excutils.forever_retry_uncaught_exceptions
        def _consumer_thread():
            try:
                self.consume()
            except greenlet.GreenletExit:
                return
        if self.consumer_thread is None:
            self.consumer_thread = eventlet.spawn(_consumer_thread)
        return self.consumer_thread

    def create_consumer(self, topic, proxy, fanout=False):
        """Create a consumer that calls a method in a proxy object."""
        proxy_cb = rpc_amqp.ProxyCallback(
            self.conf, proxy,
            rpc_amqp.get_connection_pool(self.conf, Connection))
        self.proxy_callbacks.append(proxy_cb)

        if fanout:
            self.declare_fanout_consumer(topic, proxy_cb)
        else:
            self.declare_topic_consumer(topic, proxy_cb)

    def create_worker(self, topic, proxy, pool_name):
        """Create a worker that calls a method in a proxy object."""
        proxy_cb = rpc_amqp.ProxyCallback(
            self.conf, proxy,
            rpc_amqp.get_connection_pool(self.conf, Connection))
        self.proxy_callbacks.append(proxy_cb)
        self.declare_topic_consumer(topic, proxy_cb, pool_name)

    def join_consumer_pool(self, callback, pool_name, topic,
                           exchange_name=None, ack_on_error=True):
        """Register as a member of a group of consumers for a given topic from
        the specified exchange.

        Exactly one member of a given pool will receive each message.

        A message will be delivered to multiple pools, if more than
        one is created.
        """
        callback_wrapper = rpc_amqp.CallbackWrapper(
            conf=self.conf,
            callback=callback,
            connection_pool=rpc_amqp.get_connection_pool(self.conf,
                                                         Connection),
        )
        self.proxy_callbacks.append(callback_wrapper)
        self.declare_topic_consumer(
            queue_name=pool_name,
            topic=topic,
            exchange_name=exchange_name,
            callback=callback_wrapper,
            ack_on_error=ack_on_error,
        )


def create_connection(conf, new=True):
    """Create a connection."""
    return rpc_amqp.create_connection(
        conf, new,
        rpc_amqp.get_connection_pool(conf, Connection))


def multicall(conf, context, topic, msg, timeout=None):
    """Make a call that returns multiple times."""
    return rpc_amqp.multicall(
        conf, context, topic, msg, timeout,
        rpc_amqp.get_connection_pool(conf, Connection))


def call(conf, context, topic, msg, timeout=None):
    """Sends a message on a topic and wait for a response."""
    return rpc_amqp.call(
        conf, context, topic, msg, timeout,
        rpc_amqp.get_connection_pool(conf, Connection))


def cast(conf, context, topic, msg):
    """Sends a message on a topic without waiting for a response."""
    return rpc_amqp.cast(
        conf, context, topic, msg,
        rpc_amqp.get_connection_pool(conf, Connection))


def fanout_cast(conf, context, topic, msg):
    """Sends a message on a fanout exchange without waiting for a response."""
    return rpc_amqp.fanout_cast(
        conf, context, topic, msg,
        rpc_amqp.get_connection_pool(conf, Connection))


def cast_to_server(conf, context, server_params, topic, msg):
    """Sends a message on a topic to a specific server."""
    return rpc_amqp.cast_to_server(
        conf, context, server_params, topic, msg,
        rpc_amqp.get_connection_pool(conf, Connection))


def fanout_cast_to_server(conf, context, server_params, topic, msg):
    """Sends a message on a fanout exchange to a specific server."""
    return rpc_amqp.fanout_cast_to_server(
        conf, context, server_params, topic, msg,
        rpc_amqp.get_connection_pool(conf, Connection))


def notify(conf, context, topic, msg, envelope):
    """Sends a notification event on a topic."""
    return rpc_amqp.notify(
        conf, context, topic, msg,
        rpc_amqp.get_connection_pool(conf, Connection),
        envelope)


def cleanup():
    return rpc_amqp.cleanup(Connection.pool)


class RabbitIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, ctxt, message, msg_id, reply_q):
        super(RabbitIncomingMessage, self).__init__(listener, ctxt, message)

        self.msg_id = msg_id
        self.reply_q = reply_q

    def _send_reply(self, conn, reply=None, failure=None, ending=False):
        # FIXME(markmc): is the reply format really driver specific?
        msg = {'result': reply, 'failure': failure}

        # FIXME(markmc): given that we're not supporting multicall ...
        if ending:
            msg['ending'] = True

        rpc_amqp._add_unique_id(msg)

        # If a reply_q exists, add the msg_id to the reply and pass the
        # reply_q to direct_send() to use it as the response queue.
        # Otherwise use the msg_id for backward compatibilty.
        if self.reply_q:
            msg['_msg_id'] = self.msg_id
            conn.direct_send(self.reply_q, rpc_common.serialize_msg(msg))
        else:
            conn.direct_send(self.msg_id, rpc_common.serialize_msg(msg))

    def reply(self, reply=None, failure=None):
        LOG.info("reply")
        with self.listener.driver._get_connection() as conn:
            self._send_reply(conn, reply, failure)
            self._send_reply(conn, ending=True)

    def done(self):
        LOG.info("done")
        # FIXME(markmc): I'm not sure we need this method ... we've already
        # acked the message at this point


class RabbitListener(base.Listener):

    def __init__(self, driver, target, conn):
        super(RabbitListener, self).__init__(driver, target)
        self.conn = conn
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.incoming = []

    def __call__(self, message):
        # FIXME(markmc): del local.store.context

        # FIXME(markmc): logging isn't driver specific
        rpc_common._safe_log(LOG.debug, _('received %s'), message)

        self.msg_id_cache.check_duplicate_message(message)
        ctxt = rpc_amqp.unpack_context(self.conf, message)

        self.incoming.append(RabbitIncomingMessage(self,
                                                   ctxt.to_dict(),
                                                   message,
                                                   ctxt.msg_id,
                                                   ctxt.reply_q))

    def poll(self):
        while True:
            if self.incoming:
                return self.incoming.pop(0)

            # FIXME(markmc): timeout?
            self.conn.consume(limit=1)


class ReplyWaiters(object):

    def __init__(self):
        self._queues = {}
        self._wrn_threshhold = 10

    def get(self, msg_id):
        return self._queues.get(msg_id)

    def put(self, msg_id, message_data):
        queue = self._queues.get(msg_id)
        if not queue:
            LOG.warn(_('No calling threads waiting for msg_id : %(msg_id)s'
                       ', message : %(data)s'), {'msg_id': msg_id,
                                                 'data': message_data})
            LOG.warn(_('_queues: %s') % str(self._queues))
        else:
            queue.put(message_data)

    def wake_all(self, except_id):
        for msg_id in self._queues:
            if msg_id == except_id:
                continue
            self.put(msg_id, None)

    def add(self, msg_id, queue):
        self._queues[msg_id] = queue
        if len(self._queues) > self._wrn_threshhold:
            LOG.warn(_('Number of call queues is greater than warning '
                       'threshhold: %d. There could be a leak.') %
                     self._wrn_threshhold)
            self._wrn_threshhold *= 2

    def remove(self, msg_id):
        del self._queues[msg_id]


class RabbitWaiter(object):

    def __init__(self, conf, reply_q, conn):
        self.conf = conf
        self.conn = conn
        self.reply_q = reply_q

        self.conn_lock = threading.Lock()
        self.incoming = []
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.waiters = ReplyWaiters()

        conn.declare_direct_consumer(reply_q, self)

    def __call__(self, message):
        self.incoming.append(message)

    def listen(self, msg_id):
        queue = Queue.Queue()
        self.waiters.add(msg_id, queue)

    def unlisten(self, msg_id):
        self.waiters.remove(msg_id)

    def _process_reply(self, data):
        result = None
        ending = False
        self.msg_id_cache.check_duplicate_message(data)
        if data['failure']:
            failure = data['failure']
            result = rpc_common.deserialize_remote_exception(self.conf,
                                                             failure)
        elif data.get('ending', False):
            ending = True
        else:
            result = data['result']
        return result, ending

    def _poll_connection(self, msg_id):
        while True:
            while self.incoming:
                message_data = self.incoming.pop(0)
                if message_data.pop('_msg_id', None) == msg_id:
                    return self._process_reply(message_data)

                self.waiters.put(msg_id, message_data)

            # FIXME(markmc): timeout?
            self.conn.consume(limit=1)

    def _poll_queue(self, msg_id):
        while True:
            # FIXME(markmc): timeout?
            message = self.waiters.get(msg_id)
            if message is None:
                return None, None, True  # lock was released

            reply, ending = self._process_reply(message)
            return reply, ending, False

    def wait(self, msg_id):
        # NOTE(markmc): multiple threads may call this
        # First thread calls consume, when it gets its reply
        # it wakes up other threads and they call consume
        # If a thread gets a message destined for another
        # thread, it wakes up the other thread
        final_reply = None
        while True:
            if self.conn_lock.acquire(blocking=False):
                try:
                    reply, ending = self._poll_connection(msg_id)
                    if reply:
                        final_reply = reply
                    elif ending:
                        return final_reply
                finally:
                    self.conn_lock.release()
                    self.waiters.wake_all(msg_id)
            else:
                reply, ending, trylock = self._poll_queue(msg_id)
                if trylock:
                    continue
                if reply:
                    final_reply = reply
                elif ending:
                    return final_reply


class RabbitDriver(base.BaseDriver):

    def __init__(self, conf, url=None, default_exchange=None):
        super(RabbitDriver, self).__init__(conf, url, default_exchange)

        self.conf.register_opts(kombu_opts)
        self.conf.register_opts(rabbit_opts)
        self.conf.register_opts(rpc_amqp.amqp_opts)

        self._default_exchange = urls.exchange_from_url(url, default_exchange)

        # FIXME(markmc): temp hack
        if self._default_exchange:
            self.conf.set_override('control_exchange', self._default_exchange)

        # FIXME(markmc): get connection params from URL in addition to conf
        # FIXME(markmc): close connections
        self._connection_pool = rpc_amqp.get_connection_pool(self.conf,
                                                             Connection)

        self._reply_q_lock = threading.Lock()
        self._reply_q = None
        self._reply_q_conn = None
        self._waiter = None

    def _get_connection(self, pooled=True):
        return rpc_amqp.ConnectionContext(self.conf,
                                          self._connection_pool,
                                          pooled=pooled)

    def _get_reply_q(self):
        with self._reply_q_lock:
            if self._reply_q is not None:
                return self._reply_q

            reply_q = 'reply_' + uuid.uuid4().hex

            conn = self._get_connection(pooled=False)

            self._waiter = RabbitWaiter(self.conf, reply_q, conn)

            self._reply_q = reply_q
            self._reply_q_conn = conn

        return self._reply_q

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):

        # FIXME(markmc): remove this temporary hack
        class Context(object):
            def __init__(self, d):
                self.d = d

            def to_dict(self):
                return self.d

        context = Context(ctxt)
        msg = message

        msg_id = uuid.uuid4().hex
        msg.update({'_msg_id': msg_id})
        LOG.debug(_('MSG_ID is %s') % (msg_id))
        rpc_amqp._add_unique_id(msg)
        rpc_amqp.pack_context(msg, context)

        msg.update({'_reply_q': self._get_reply_q()})

        # FIXME(markmc): handle envelope param
        msg = rpc_common.serialize_msg(msg)

        if wait_for_reply:
            self._waiter.listen(msg_id)

        try:
            with self._get_connection() as conn:
                # FIXME(markmc): check that target.topic is set
                if target.fanout:
                    conn.fanout_send(target.topic, msg)
                else:
                    topic = target.topic
                    if target.server:
                        topic = '%s.%s' % (target.topic, target.server)
                    conn.topic_send(topic, msg, timeout=timeout)

            if wait_for_reply:
                # FIXME(markmc): timeout?
                return self._waiter.wait(msg_id)
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def listen(self, target):
        # FIXME(markmc): check that topic.target and topic.server is set

        conn = self._get_connection(pooled=False)

        listener = RabbitListener(self, target, conn)

        conn.declare_topic_consumer(target.topic, listener)
        conn.declare_topic_consumer('%s.%s' % (target.topic, target.server),
                                    listener)
        conn.declare_fanout_consumer(target.topic, listener)

        return listener
