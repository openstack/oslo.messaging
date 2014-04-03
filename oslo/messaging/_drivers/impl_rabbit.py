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
import socket
import ssl
import time
import uuid

import kombu
import kombu.connection
import kombu.entity
import kombu.messaging
from oslo.config import cfg
import six

from oslo.messaging._drivers import amqp as rpc_amqp
from oslo.messaging._drivers import amqpdriver
from oslo.messaging._drivers import common as rpc_common
from oslo.messaging.openstack.common import network_utils

# FIXME(markmc): remove this
_ = lambda s: s

rabbit_opts = [
    cfg.StrOpt('kombu_ssl_version',
               default='',
               help='SSL version to use (valid only if SSL enabled). '
                    'valid values are TLSv1, SSLv23 and SSLv3. SSLv2 may '
                    'be available on some distributions.'
               ),
    cfg.StrOpt('kombu_ssl_keyfile',
               default='',
               help='SSL key file (valid only if SSL enabled).'),
    cfg.StrOpt('kombu_ssl_certfile',
               default='',
               help='SSL cert file (valid only if SSL enabled).'),
    cfg.StrOpt('kombu_ssl_ca_certs',
               default='',
               help='SSL certification authority file '
                    '(valid only if SSL enabled).'),
    cfg.FloatOpt('kombu_reconnect_delay',
                 default=1.0,
                 help='How long to wait before reconnecting in response to an '
                      'AMQP consumer cancel notification.'),
    cfg.StrOpt('rabbit_host',
               default='localhost',
               help='The RabbitMQ broker address where a single node is '
                    'used.'),
    cfg.IntOpt('rabbit_port',
               default=5672,
               help='The RabbitMQ broker port where a single node is used.'),
    cfg.ListOpt('rabbit_hosts',
                default=['$rabbit_host:$rabbit_port'],
                help='RabbitMQ HA cluster host:port pairs.'),
    cfg.BoolOpt('rabbit_use_ssl',
                default=False,
                help='Connect over SSL for RabbitMQ.'),
    cfg.StrOpt('rabbit_userid',
               default='guest',
               help='The RabbitMQ userid.'),
    cfg.StrOpt('rabbit_password',
               default='guest',
               help='The RabbitMQ password.',
               secret=True),
    cfg.StrOpt('rabbit_login_method',
               default='AMQPLAIN',
               help='the RabbitMQ login method'),
    cfg.StrOpt('rabbit_virtual_host',
               default='/',
               help='The RabbitMQ virtual host.'),
    cfg.IntOpt('rabbit_retry_interval',
               default=1,
               help='How frequently to retry connecting with RabbitMQ.'),
    cfg.IntOpt('rabbit_retry_backoff',
               default=2,
               help='How long to backoff for between retries when connecting '
                    'to RabbitMQ.'),
    cfg.IntOpt('rabbit_max_retries',
               default=0,
               help='Maximum number of RabbitMQ connection retries. '
                    'Default is 0 (infinite retry count).'),
    cfg.BoolOpt('rabbit_ha_queues',
                default=False,
                help='Use HA queues in RabbitMQ (x-ha-policy: all). '
                     'If you change this option, you must wipe the '
                     'RabbitMQ database.'),

    # FIXME(markmc): this was toplevel in openstack.common.rpc
    cfg.BoolOpt('fake_rabbit',
                default=False,
                help='If passed, use a fake RabbitMQ provider.'),
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
        self.tag = str(tag)
        self.kwargs = kwargs
        self.queue = None
        self.reconnect(channel)

    def reconnect(self, channel):
        """Re-declare the queue after a rabbit reconnect."""
        self.channel = channel
        self.kwargs['channel'] = channel
        self.queue = kombu.entity.Queue(**self.kwargs)
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
        """Init a 'direct' publisher.

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
        """Init a 'topic' publisher.

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
        """Init a 'fanout' publisher.

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
                'login_method': self.conf.rabbit_login_method,
                'virtual_host': self.conf.rabbit_virtual_host,
            }

            for sp_key, value in six.iteritems(server_params):
                p_key = server_params_to_kombu_params.get(sp_key, sp_key)
                params[p_key] = value

            if self.conf.fake_rabbit:
                params['transport'] = 'memory'
            if self.conf.rabbit_use_ssl:
                params['ssl'] = ssl_params

            params_list.append(params)

        self.params_list = itertools.cycle(params_list)

        self.memory_transport = self.conf.fake_rabbit

        self.connection = None
        self.do_consume = None
        self.reconnect()

    # FIXME(markmc): use oslo sslutils when it is available as a library
    _SSL_PROTOCOLS = {
        "tlsv1": ssl.PROTOCOL_TLSv1,
        "sslv23": ssl.PROTOCOL_SSLv23,
        "sslv3": ssl.PROTOCOL_SSLv3
    }

    try:
        _SSL_PROTOCOLS["sslv2"] = ssl.PROTOCOL_SSLv2
    except AttributeError:
        pass

    @classmethod
    def validate_ssl_version(cls, version):
        key = version.lower()
        try:
            return cls._SSL_PROTOCOLS[key]
        except KeyError:
            raise RuntimeError(_("Invalid SSL version : %s") % version)

    def _fetch_ssl_params(self):
        """Handles fetching what ssl params should be used for the connection
        (if any).
        """
        ssl_params = dict()

        # http://docs.python.org/library/ssl.html - ssl.wrap_socket
        if self.conf.kombu_ssl_version:
            ssl_params['ssl_version'] = self.validate_ssl_version(
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

        # Return the extended behavior or just have the default behavior
        return ssl_params or True

    def _connect(self, params):
        """Connect to rabbit.  Re-establish any queues that may have
        been declared before if we are reconnecting.  Exceptions should
        be handled by the caller.
        """
        if self.connection:
            LOG.info(_("Reconnecting to AMQP server on "
                     "%(hostname)s:%(port)d") % params)
            try:
                # XXX(nic): when reconnecting to a RabbitMQ cluster
                # with mirrored queues in use, the attempt to release the
                # connection can hang "indefinitely" somewhere deep down
                # in Kombu.  Blocking the thread for a bit prior to
                # release seems to kludge around the problem where it is
                # otherwise reproduceable.
                if self.conf.kombu_reconnect_delay > 0:
                    LOG.info(_("Delaying reconnect for %1.1f seconds...") %
                             self.conf.kombu_reconnect_delay)
                    time.sleep(self.conf.kombu_reconnect_delay)

                self.connection.release()
            except self.connection_errors:
                pass
            # Setting this in case the next statement fails, though
            # it shouldn't be doing any network operations, yet.
            self.connection = None
        self.connection = kombu.connection.BrokerConnection(**params)
        self.connection_errors = self.connection.connection_errors
        self.channel_errors = self.connection.channel_errors
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
            params = six.next(self.params_list)
            attempt += 1
            try:
                self._connect(params)
                return
            except IOError as e:
                pass
            except self.connection_errors as e:
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
            except self.connection_errors as e:
                if error_callback:
                    error_callback(e)
            except self.channel_errors as e:
                if error_callback:
                    error_callback(e)
            except (socket.timeout, IOError) as e:
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
        self.connection.release()
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
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
                                    six.next(self.consumer_num))
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
                               exchange_name=None):
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

    def consume(self, limit=None, timeout=None):
        """Consume from all queues/consumers."""
        it = self.iterconsume(limit=limit, timeout=timeout)
        while True:
            try:
                six.next(it)
            except StopIteration:
                return


class RabbitDriver(amqpdriver.AMQPDriverBase):

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=[]):
        conf.register_opts(rabbit_opts)
        conf.register_opts(rpc_amqp.amqp_opts)

        connection_pool = rpc_amqp.get_connection_pool(conf, Connection)

        super(RabbitDriver, self).__init__(conf, url,
                                           connection_pool,
                                           default_exchange,
                                           allowed_remote_exmods)

    def require_features(self, requeue=True):
        pass
