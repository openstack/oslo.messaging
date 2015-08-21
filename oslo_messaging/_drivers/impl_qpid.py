#    Copyright 2011 OpenStack Foundation
#    Copyright 2011 - 2012, Red Hat, Inc.
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
import os
import random
import time
import warnings

from oslo_config import cfg
from oslo_serialization import jsonutils
from oslo_utils import importutils
from oslo_utils import netutils
import six

from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._i18n import _
from oslo_messaging._i18n import _LE
from oslo_messaging._i18n import _LI
from oslo_messaging import exceptions

qpid_codec = importutils.try_import("qpid.codec010")
qpid_messaging = importutils.try_import("qpid.messaging")
qpid_exceptions = importutils.try_import("qpid.messaging.exceptions")

LOG = logging.getLogger(__name__)

qpid_opts = [
    cfg.StrOpt('qpid_hostname',
               default='localhost',
               deprecated_group='DEFAULT',
               help='Qpid broker hostname.'),
    cfg.IntOpt('qpid_port',
               default=5672,
               deprecated_group='DEFAULT',
               help='Qpid broker port.'),
    cfg.ListOpt('qpid_hosts',
                default=['$qpid_hostname:$qpid_port'],
                deprecated_group='DEFAULT',
                help='Qpid HA cluster host:port pairs.'),
    cfg.StrOpt('qpid_username',
               default='',
               deprecated_group='DEFAULT',
               help='Username for Qpid connection.'),
    cfg.StrOpt('qpid_password',
               default='',
               deprecated_group='DEFAULT',
               help='Password for Qpid connection.',
               secret=True),
    cfg.StrOpt('qpid_sasl_mechanisms',
               default='',
               deprecated_group='DEFAULT',
               help='Space separated list of SASL mechanisms to use for '
                    'auth.'),
    cfg.IntOpt('qpid_heartbeat',
               default=60,
               deprecated_group='DEFAULT',
               help='Seconds between connection keepalive heartbeats.'),
    cfg.StrOpt('qpid_protocol',
               default='tcp',
               deprecated_group='DEFAULT',
               help="Transport to use, either 'tcp' or 'ssl'."),
    cfg.BoolOpt('qpid_tcp_nodelay',
                default=True,
                deprecated_group='DEFAULT',
                help='Whether to disable the Nagle algorithm.'),
    cfg.IntOpt('qpid_receiver_capacity',
               default=1,
               deprecated_group='DEFAULT',
               help='The number of prefetched messages held by receiver.'),
    # NOTE(russellb) If any additional versions are added (beyond 1 and 2),
    # this file could probably use some additional refactoring so that the
    # differences between each version are split into different classes.
    cfg.IntOpt('qpid_topology_version',
               default=1,
               deprecated_group='DEFAULT',
               help="The qpid topology version to use.  Version 1 is what "
                    "was originally used by impl_qpid.  Version 2 includes "
                    "some backwards-incompatible changes that allow broker "
                    "federation to work.  Users should update to version 2 "
                    "when they are able to take everything down, as it "
                    "requires a clean break."),
]

JSON_CONTENT_TYPE = 'application/json; charset=utf8'


def raise_invalid_topology_version(conf):
    msg = (_("Invalid value for qpid_topology_version: %d") %
           conf.qpid_topology_version)
    LOG.error(msg)
    raise Exception(msg)


class QpidMessage(dict):
    def __init__(self, session, raw_message):
        super(QpidMessage, self).__init__(
            rpc_common.deserialize_msg(raw_message.content))
        self._raw_message = raw_message
        self._session = session

    def acknowledge(self):
        self._session.acknowledge(self._raw_message)

    def requeue(self):
        pass


class ConsumerBase(object):
    """Consumer base class."""

    def __init__(self, conf, session, callback, node_name, node_opts,
                 link_name, link_opts):
        """Declare a queue on an amqp session.

        'session' is the amqp session to use
        'callback' is the callback to call when messages are received
        'node_name' is the first part of the Qpid address string, before ';'
        'node_opts' will be applied to the "x-declare" section of "node"
                    in the address string.
        'link_name' goes into the "name" field of the "link" in the address
                    string
        'link_opts' will be applied to the "x-declare" section of "link"
                    in the address string.
        """
        self.callback = callback
        self.receiver = None
        self.rcv_capacity = conf.qpid_receiver_capacity
        self.session = None

        if conf.qpid_topology_version == 1:
            addr_opts = {
                "create": "always",
                "node": {
                    "type": "topic",
                    "x-declare": {
                        "durable": True,
                        "auto-delete": True,
                    },
                },
                "link": {
                    "durable": True,
                    "x-declare": {
                        "durable": False,
                        "auto-delete": True,
                        "exclusive": False,
                    },
                },
            }
            addr_opts["node"]["x-declare"].update(node_opts)
        elif conf.qpid_topology_version == 2:
            addr_opts = {
                "link": {
                    "x-declare": {
                        "auto-delete": True,
                        "exclusive": False,
                    },
                },
            }
        else:
            raise_invalid_topology_version(conf)

        addr_opts["link"]["x-declare"].update(link_opts)
        if link_name:
            addr_opts["link"]["name"] = link_name

        self.address = "%s ; %s" % (node_name, jsonutils.dumps(addr_opts))

        self.connect(session)

    def connect(self, session):
        """Declare the receiver on connect."""
        self._declare_receiver(session)

    def reconnect(self, session):
        """Re-declare the receiver after a Qpid reconnect."""
        self._declare_receiver(session)

    def _declare_receiver(self, session):
        self.session = session
        self.receiver = session.receiver(self.address)
        self.receiver.capacity = self.rcv_capacity

    def _unpack_json_msg(self, msg):
        """Load the JSON data in msg if msg.content_type indicates that it
           is necessary.  Put the loaded data back into msg.content and
           update msg.content_type appropriately.

        A Qpid Message containing a dict will have a content_type of
        'amqp/map', whereas one containing a string that needs to be converted
        back from JSON will have a content_type of JSON_CONTENT_TYPE.

        :param msg: a Qpid Message object
        :returns: None
        """
        if msg.content_type == JSON_CONTENT_TYPE:
            msg.content = jsonutils.loads(msg.content)
            msg.content_type = 'amqp/map'

    def consume(self):
        """Fetch the message and pass it to the callback object."""
        message = self.receiver.fetch()
        try:
            self._unpack_json_msg(message)
            self.callback(QpidMessage(self.session, message))
        except Exception:
            LOG.exception(_LE("Failed to process message... skipping it."))
            self.session.acknowledge(message)

    def get_receiver(self):
        return self.receiver

    def get_node_name(self):
        return self.address.split(';')[0]


class DirectConsumer(ConsumerBase):
    """Queue/consumer class for 'direct'."""

    def __init__(self, conf, session, msg_id, callback):
        """Init a 'direct' queue.

        'session' is the amqp session to use
        'msg_id' is the msg_id to listen on
        'callback' is the callback to call when messages are received
        """

        link_opts = {
            "exclusive": True,
            "durable": conf.amqp_durable_queues,
        }

        if conf.qpid_topology_version == 1:
            node_name = "%s/%s" % (msg_id, msg_id)
            node_opts = {"type": "direct"}
            link_name = msg_id
        elif conf.qpid_topology_version == 2:
            node_name = "amq.direct/%s" % msg_id
            node_opts = {}
            link_name = msg_id
        else:
            raise_invalid_topology_version(conf)

        super(DirectConsumer, self).__init__(conf, session, callback,
                                             node_name, node_opts, link_name,
                                             link_opts)


class TopicConsumer(ConsumerBase):
    """Consumer class for 'topic'."""

    def __init__(self, conf, session, topic, callback, exchange_name,
                 name=None):
        """Init a 'topic' queue.

        :param session: the amqp session to use
        :param topic: is the topic to listen on
        :paramtype topic: str
        :param callback: the callback to call when messages are received
        :param name: optional queue name, defaults to topic
        """

        link_opts = {
            "auto-delete": conf.amqp_auto_delete,
            "durable": conf.amqp_durable_queues,
        }

        if conf.qpid_topology_version == 1:
            node_name = "%s/%s" % (exchange_name, topic)
        elif conf.qpid_topology_version == 2:
            node_name = "amq.topic/topic/%s/%s" % (exchange_name, topic)
        else:
            raise_invalid_topology_version(conf)

        super(TopicConsumer, self).__init__(conf, session, callback, node_name,
                                            {}, name or topic, link_opts)


class FanoutConsumer(ConsumerBase):
    """Consumer class for 'fanout'."""

    def __init__(self, conf, session, topic, callback):
        """Init a 'fanout' queue.

        'session' is the amqp session to use
        'topic' is the topic to listen on
        'callback' is the callback to call when messages are received
        """
        self.conf = conf

        link_opts = {"exclusive": True}

        if conf.qpid_topology_version == 1:
            node_name = "%s_fanout" % topic
            node_opts = {"durable": False, "type": "fanout"}
        elif conf.qpid_topology_version == 2:
            node_name = "amq.topic/fanout/%s" % topic
            node_opts = {}
        else:
            raise_invalid_topology_version(conf)

        super(FanoutConsumer, self).__init__(conf, session, callback,
                                             node_name, node_opts, None,
                                             link_opts)


class Publisher(object):
    """Base Publisher class."""

    def __init__(self, conf, session, node_name, node_opts=None):
        """Init the Publisher class with the exchange_name, routing_key,
        and other options
        """
        self.sender = None
        self.session = session

        if conf.qpid_topology_version == 1:
            addr_opts = {
                "create": "always",
                "node": {
                    "type": "topic",
                    "x-declare": {
                        "durable": False,
                        # auto-delete isn't implemented for exchanges in qpid,
                        # but put in here anyway
                        "auto-delete": True,
                    },
                },
            }
            if node_opts:
                addr_opts["node"]["x-declare"].update(node_opts)

            self.address = "%s ; %s" % (node_name, jsonutils.dumps(addr_opts))
        elif conf.qpid_topology_version == 2:
            self.address = node_name
        else:
            raise_invalid_topology_version(conf)

        self.reconnect(session)

    def reconnect(self, session):
        """Re-establish the Sender after a reconnection."""
        self.sender = session.sender(self.address)

    def _pack_json_msg(self, msg):
        """Qpid cannot serialize dicts containing strings longer than 65535
           characters.  This function dumps the message content to a JSON
           string, which Qpid is able to handle.

        :param msg: May be either a Qpid Message object or a bare dict.
        :returns: A Qpid Message with its content field JSON encoded.
        """
        try:
            msg.content = jsonutils.dumps(msg.content)
        except AttributeError:
            # Need to have a Qpid message so we can set the content_type.
            msg = qpid_messaging.Message(jsonutils.dumps(msg))
        msg.content_type = JSON_CONTENT_TYPE
        return msg

    def send(self, msg):
        """Send a message."""
        try:
            # Check if Qpid can encode the message
            check_msg = msg
            if not hasattr(check_msg, 'content_type'):
                check_msg = qpid_messaging.Message(msg)
            content_type = check_msg.content_type
            enc, dec = qpid_messaging.message.get_codec(content_type)
            enc(check_msg.content)
        except qpid_codec.CodecException:
            # This means the message couldn't be serialized as a dict.
            msg = self._pack_json_msg(msg)
        self.sender.send(msg)


class DirectPublisher(Publisher):
    """Publisher class for 'direct'."""
    def __init__(self, conf, session, topic):
        """Init a 'direct' publisher."""

        if conf.qpid_topology_version == 1:
            node_name = "%s/%s" % (topic, topic)
            node_opts = {"type": "direct"}
        elif conf.qpid_topology_version == 2:
            node_name = "amq.direct/%s" % topic
            node_opts = {}
        else:
            raise_invalid_topology_version(conf)

        super(DirectPublisher, self).__init__(conf, session, node_name,
                                              node_opts)


class TopicPublisher(Publisher):
    """Publisher class for 'topic'."""
    def __init__(self, conf, session, exchange_name, topic):
        """Init a 'topic' publisher.
        """
        if conf.qpid_topology_version == 1:
            node_name = "%s/%s" % (exchange_name, topic)
        elif conf.qpid_topology_version == 2:
            node_name = "amq.topic/topic/%s/%s" % (exchange_name, topic)
        else:
            raise_invalid_topology_version(conf)

        super(TopicPublisher, self).__init__(conf, session, node_name)


class FanoutPublisher(Publisher):
    """Publisher class for 'fanout'."""
    def __init__(self, conf, session, topic):
        """Init a 'fanout' publisher.
        """

        if conf.qpid_topology_version == 1:
            node_name = "%s_fanout" % topic
            node_opts = {"type": "fanout"}
        elif conf.qpid_topology_version == 2:
            node_name = "amq.topic/fanout/%s" % topic
            node_opts = {}
        else:
            raise_invalid_topology_version(conf)

        super(FanoutPublisher, self).__init__(conf, session, node_name,
                                              node_opts)


class NotifyPublisher(Publisher):
    """Publisher class for notifications."""
    def __init__(self, conf, session, exchange_name, topic):
        """Init a 'topic' publisher.
        """
        node_opts = {"durable": True}

        if conf.qpid_topology_version == 1:
            node_name = "%s/%s" % (exchange_name, topic)
        elif conf.qpid_topology_version == 2:
            node_name = "amq.topic/topic/%s/%s" % (exchange_name, topic)
        else:
            raise_invalid_topology_version(conf)

        super(NotifyPublisher, self).__init__(conf, session, node_name,
                                              node_opts)


class Connection(object):
    """Connection object."""

    pools = {}

    def __init__(self, conf, url, purpose):
        if not qpid_messaging:
            raise ImportError("Failed to import qpid.messaging")

        self.connection = None
        self.session = None
        self.consumers = {}
        self.conf = conf
        self.driver_conf = conf.oslo_messaging_qpid

        self._consume_loop_stopped = False

        self.brokers_params = []
        if url.hosts:
            for host in url.hosts:
                params = {
                    'username': host.username or '',
                    'password': host.password or '',
                }
                if host.port is not None:
                    params['host'] = '%s:%d' % (host.hostname, host.port)
                else:
                    params['host'] = host.hostname
                self.brokers_params.append(params)
        else:
            # Old configuration format
            for adr in self.driver_conf.qpid_hosts:
                hostname, port = netutils.parse_host_port(
                    adr, default_port=5672)

                if ':' in hostname:
                    hostname = '[' + hostname + ']'

                params = {
                    'host': '%s:%d' % (hostname, port),
                    'username': self.driver_conf.qpid_username,
                    'password': self.driver_conf.qpid_password,
                }
                self.brokers_params.append(params)

        random.shuffle(self.brokers_params)
        self.brokers = itertools.cycle(self.brokers_params)

        self._initial_pid = os.getpid()
        self.reconnect()

    def _connect(self, broker):
        # Create the connection - this does not open the connection
        self.connection = qpid_messaging.Connection(broker['host'])

        # Check if flags are set and if so set them for the connection
        # before we call open
        self.connection.username = broker['username']
        self.connection.password = broker['password']

        self.connection.sasl_mechanisms = self.driver_conf.qpid_sasl_mechanisms
        # Reconnection is done by self.reconnect()
        self.connection.reconnect = False
        self.connection.heartbeat = self.driver_conf.qpid_heartbeat
        self.connection.transport = self.driver_conf.qpid_protocol
        self.connection.tcp_nodelay = self.driver_conf.qpid_tcp_nodelay
        self.connection.open()

    def _register_consumer(self, consumer):
        self.consumers[six.text_type(consumer.get_receiver())] = consumer

    def _lookup_consumer(self, receiver):
        return self.consumers[six.text_type(receiver)]

    def _disconnect(self):
        # Close the session if necessary
        if self.connection is not None and self.connection.opened():
            try:
                self.connection.close()
            except qpid_exceptions.MessagingError:
                pass
        self.connection = None

    def reconnect(self, retry=None):
        """Handles reconnecting and re-establishing sessions and queues.
        Will retry up to retry number of times.
        retry = None or -1 means to retry forever
        retry = 0 means no retry
        retry = N means N retries
        """
        delay = 1
        attempt = 0
        loop_forever = False
        if retry is None or retry < 0:
            loop_forever = True

        while True:
            self._disconnect()

            attempt += 1
            broker = six.next(self.brokers)
            try:
                self._connect(broker)
            except qpid_exceptions.MessagingError as e:
                msg_dict = dict(e=e,
                                delay=delay,
                                retry=retry,
                                broker=broker)
                if not loop_forever and attempt > retry:
                    msg = _('Unable to connect to AMQP server on '
                            '%(broker)s after %(retry)d '
                            'tries: %(e)s') % msg_dict
                    LOG.error(msg)
                    raise exceptions.MessageDeliveryFailure(msg)
                else:
                    msg = _LE("Unable to connect to AMQP server on "
                              "%(broker)s: %(e)s. Sleeping %(delay)s seconds")
                    LOG.error(msg, msg_dict)
                    time.sleep(delay)
                    delay = min(delay + 1, 5)
            else:
                LOG.info(_LI('Connected to AMQP server on %s'), broker['host'])
                break

        self.session = self.connection.session()

        if self.consumers:
            consumers = self.consumers
            self.consumers = {}

            for consumer in six.itervalues(consumers):
                consumer.reconnect(self.session)
                self._register_consumer(consumer)

            LOG.debug("Re-established AMQP queues")

    def ensure(self, error_callback, method, retry=None):

        current_pid = os.getpid()
        if self._initial_pid != current_pid:
            # NOTE(sileht):
            # to get the same level of fork support that rabbit driver have
            # (ie: allow fork before the first connection established)
            # we could use the kombu workaround:
            # https://github.com/celery/kombu/blob/master/kombu/transport/
            # qpid_patches.py#L67
            LOG.warn("Process forked! "
                     "This can result in unpredictable behavior. "
                     "See: http://docs.openstack.org/developer/"
                     "oslo_messaging/transport.html")
            self._initial_pid = current_pid

        while True:
            try:
                return method()
            except (qpid_exceptions.Empty,
                    qpid_exceptions.MessagingError) as e:
                if error_callback:
                    error_callback(e)
                self.reconnect(retry=retry)

    def close(self):
        """Close/release this connection."""
        try:
            self.connection.close()
        except Exception:
            # NOTE(dripton) Logging exceptions that happen during cleanup just
            # causes confusion; there's really nothing useful we can do with
            # them.
            pass
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        self.session.close()
        self.session = self.connection.session()
        self.consumers = {}

    def declare_consumer(self, consumer_cls, topic, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """
        def _connect_error(exc):
            log_info = {'topic': topic, 'err_str': exc}
            LOG.error(_LE("Failed to declare consumer for topic '%(topic)s': "
                          "%(err_str)s"), log_info)

        def _declare_consumer():
            consumer = consumer_cls(self.driver_conf, self.session, topic,
                                    callback)
            self._register_consumer(consumer)
            return consumer

        return self.ensure(_connect_error, _declare_consumer)

    def consume(self, timeout=None):
        """Consume from all queues/consumers."""

        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()

        def _raise_timeout(exc):
            LOG.debug('Timed out waiting for RPC response: %s', exc)
            raise rpc_common.Timeout()

        def _error_callback(exc):
            timer.check_return(_raise_timeout, exc)
            LOG.exception(_LE('Failed to consume message from queue: %s'), exc)

        def _consume():
            # NOTE(sileht):
            # maximum value chosen according the best practice from kombu:
            # http://kombu.readthedocs.org/en/latest/reference/kombu.common.html#kombu.common.eventloop
            poll_timeout = 1 if timeout is None else min(timeout, 1)

            while True:
                if self._consume_loop_stopped:
                    self._consume_loop_stopped = False
                    return

                try:
                    nxt_receiver = self.session.next_receiver(
                        timeout=poll_timeout)
                except qpid_exceptions.Empty as exc:
                    poll_timeout = timer.check_return(_raise_timeout, exc,
                                                      maximum=1)
                else:
                    break

            try:
                self._lookup_consumer(nxt_receiver).consume()
            except Exception:
                LOG.exception(_LE("Error processing message. "
                                  "Skipping it."))

        self.ensure(_error_callback, _consume)

    def publisher_send(self, cls, topic, msg, retry=None, **kwargs):
        """Send to a publisher based on the publisher class."""

        def _connect_error(exc):
            log_info = {'topic': topic, 'err_str': exc}
            LOG.exception(_LE("Failed to publish message to topic "
                          "'%(topic)s': %(err_str)s"), log_info)

        def _publisher_send():
            publisher = cls(self.driver_conf, self.session, topic=topic,
                            **kwargs)
            publisher.send(msg)

        return self.ensure(_connect_error, _publisher_send, retry=retry)

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
        self.publisher_send(DirectPublisher, topic=msg_id, msg=msg)

    def topic_send(self, exchange_name, topic, msg, timeout=None, retry=None):
        """Send a 'topic' message."""
        #
        # We want to create a message with attributes, for example a TTL. We
        # don't really need to keep 'msg' in its JSON format any longer
        # so let's create an actual Qpid message here and get some
        # value-add on the go.
        #
        # WARNING: Request timeout happens to be in the same units as
        # Qpid's TTL (seconds). If this changes in the future, then this
        # will need to be altered accordingly.
        #
        qpid_message = qpid_messaging.Message(content=msg, ttl=timeout)
        self.publisher_send(TopicPublisher, topic=topic, msg=qpid_message,
                            exchange_name=exchange_name, retry=retry)

    def fanout_send(self, topic, msg, retry=None):
        """Send a 'fanout' message."""
        self.publisher_send(FanoutPublisher, topic=topic, msg=msg, retry=retry)

    def notify_send(self, exchange_name, topic, msg, retry=None, **kwargs):
        """Send a notify message on a topic."""
        self.publisher_send(NotifyPublisher, topic=topic, msg=msg,
                            exchange_name=exchange_name, retry=retry)

    def stop_consuming(self):
        self._consume_loop_stopped = True


class QpidDriver(amqpdriver.AMQPDriverBase):
    """qpidd Driver

    .. deprecated:: 1.16 (Liberty)
    """

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=None):

        warnings.warn(_('The Qpid driver has been deprecated. '
                        'The driver is planned to be removed during the '
                        '`Mitaka` development cycle.'),
                      DeprecationWarning, stacklevel=2)

        opt_group = cfg.OptGroup(name='oslo_messaging_qpid',
                                 title='QPID driver options')
        conf.register_group(opt_group)
        conf.register_opts(qpid_opts, group=opt_group)
        conf.register_opts(rpc_amqp.amqp_opts, group=opt_group)
        conf.register_opts(base.base_opts, group=opt_group)

        connection_pool = rpc_amqp.ConnectionPool(
            conf, conf.oslo_messaging_qpid.rpc_conn_pool_size,
            url, Connection)

        super(QpidDriver, self).__init__(
            conf, url,
            connection_pool,
            default_exchange,
            allowed_remote_exmods,
            conf.oslo_messaging_qpid.send_single_reply,
        )
