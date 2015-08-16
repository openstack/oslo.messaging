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
import threading
import uuid
import sys
import time

from oslo_serialization import jsonutils

import pika
from pika import adapters as pika_adapters
from pika import credentials as pika_credentials
from pika import spec as pika_spec
from pika import exceptions as pika_exceptions
import pika_pool

from oslo_config import cfg

from oslo_messaging._drivers import common

import logging
import six

LOG = logging.getLogger(__name__)

pika_opts = [
    cfg.IntOpt('channel_max', default=None, deprecated_group='DEFAULT',
               help='Maximum number of channels to allow'),
    cfg.IntOpt('frame_max', default=None, deprecated_group='DEFAULT',
               help='The maximum byte size for an AMQP frame'),
    cfg.IntOpt('heartbeat_interval', default=None,
               deprecated_group='DEFAULT',
               help='How often to send heartbeats'),
    cfg.BoolOpt('ssl', default=None, deprecated_group='DEFAULT',
                help='Enable SSL'),
    cfg.DictOpt('ssl_options', default=None, deprecated_group='DEFAULT',
                help='Arguments passed to ssl.wrap_socket'),
    cfg.IntOpt('connection_attempts', default=None,
               deprecated_group='DEFAULT',
               help='Maximum number of retry attempts'),
    cfg.FloatOpt('retry_delay', default=None, deprecated_group='DEFAULT',
                 help='Time to wait in seconds, before the next'),
    cfg.FloatOpt('socket_timeout', default=None,
                 deprecated_group='DEFAULT',
                 help='Use for high latency networks'),
    cfg.StrOpt('locale', default=None, deprecated_group='DEFAULT',
               help='Set the locale value'),
    cfg.BoolOpt('backpressure_detection', default=None,
                deprecated_group='DEFAULT',
                help='Toggle backpressure detection')
]

pika_pool_opts = [
     cfg.IntOpt('pool_max_size', default=10, deprecated_group='DEFAULT',
                help="Maximum number of connections to keep queued."),
     cfg.IntOpt('pool_max_overflow', default=10,
                deprecated_group='DEFAULT',
                help="Maximum number of connections to create above "
                     "`pool_max_size`."),
     cfg.IntOpt('pool_timeout', default=30,
                deprecated_group='DEFAULT',
                help="Default number of seconds to wait for a connections to "
                     "available"),
     cfg.IntOpt('pool_recycle', default=None,
                deprecated_group='DEFAULT',
                help="Lifetime of a connection (since creation) in seconds "
                     "or None for no recycling. Expired connections are "
                     "closed on acquire."),
     cfg.IntOpt('pool_stale', default=None,
                deprecated_group='DEFAULT',
                help="Threshold at which inactive (since release) connections "
                     "are considered stale in seconds or None for no "
                     "staleness. Stale connections are closed on acquire.")
]

notification_opts = [
     cfg.BoolOpt('notification_persistence', default=True,
                 deprecated_group='DEFAULT',
                 help="Persist notification messages.")
]

rpc_opts = [
     cfg.IntOpt('rpc_queue_expiration', default=60, deprecated_group='DEFAULT',
                help="Time to live for rpc queues without consumers in "
                     "seconds."),
     cfg.StrOpt('rpc_reply_exchange', default="rpc_reply_exchange",
                deprecated_group='DEFAULT',
                help="Exchange name for for sending and receiving replies")
]

extra_messaging_opts = [
     cfg.StrOpt('default_durable_exchange', default="openstack_durable",
                deprecated_group='DEFAULT',
                help="Default name for durable exchange")
]


def _is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('thread')


class PikaIncomingMessage(object):

    def __init__(self, pika_engine, channel, method, properties, body, no_ack):
        self._pika_engine = pika_engine
        self._no_ack = no_ack
        self._channel = channel
        self.delivery_tag = method.delivery_tag

        self.content_type = getattr(properties, "content_type",
                                    "application/json")
        self.content_encoding = getattr(properties, "content_encoding",
                                        "utf-8")

        self.expiration = properties.expiration

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        message_dict = common.deserialize_msg(
            jsonutils.loads(body, encoding=self.content_encoding)
        )

        self.unique_id = message_dict.pop('_unique_id')
        self.msg_id = message_dict.pop('_msg_id', None)
        self.reply_q = message_dict.pop('_reply_q', None)


        context_dict = {}

        for key in list(message_dict.keys()):
            key = six.text_type(key)
            if key.startswith('_context_'):
                value = message_dict.pop(key)
                context_dict[key[9:]] = value

        self.message = message_dict
        self.ctxt = context_dict

    def reply(self, reply=None, failure=None, log_failure=True):
        if not (self.msg_id and self.reply_q):
            return

        if failure:
            failure = common.serialize_remote_exception(failure, log_failure)

        msg = {
            'result': reply,
            'failure': failure,
            '_unique_id': uuid.uuid4().hex,
            '_msg_id': self.msg_id,
            'ending': True
        }

        self._pika_engine.publish(
            exchange=self._pika_engine.rpc_reply_exchange,
            routing_key=self.reply_q,
            body=jsonutils.dumps(
                common.serialize_msg(msg),
                encoding=self.content_encoding
            ),
            properties=pika_spec.BasicProperties(
                content_encoding=self.content_encoding,
                content_type=self.content_type,
                expiration=self.expiration
            ),
            confirm=False
        )
        LOG.debug(
            "Message [id:'{}'] replied to '{}'.".format(
                self.msg_id, self.reply_q
            )
        )

    def acknowledge(self):
        if not self._no_ack:
            try:
                self._channel.basic_ack(delivery_tag=self.delivery_tag)
            except Exception:
                LOG.exception("Unable to acknowledge the message")
                raise common.RPCException("Unable to acknowledge the message")

    def requeue(self):
        if not self._no_ack:
            return self._channel.basic_nack(delivery_tag=self.delivery_tag,
                                            requeue=True)


class PikaOutgoingMessage(object):

    def __init__(self, pika_engine, message, context,
                 content_type="application/json", content_encoding="utf-8"):
        self._pika_engine = pika_engine

        self.content_type = content_type
        self.content_encoding = content_encoding

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        self.message = message
        self.context = context

        self.unique_id = uuid.uuid4().hex
        self.msg_id = None

    def send(self, exchange, rouiting_key=None, wait_for_reply=False,
             confirm=True, mandatory=False, persistent=False, timeout=None):
        msg = self.message.copy()

        if wait_for_reply:
            self.msg_id = uuid.uuid4().hex
            msg['_msg_id'] = self.msg_id
            LOG.debug('MSG_ID is %s', self.msg_id)

            msg['_reply_q'] = self._pika_engine.get_reply_q()

            reply_received = threading.Event()
            reply = [None]

            def on_reply(message):
                reply[0] = message
                reply_received.set()

            self._pika_engine.register_reply_callback(
                msg_id=self.msg_id, on_reply=on_reply, timeout=timeout
            )

        msg['_unique_id'] = self.unique_id

        for key, value in self.context.iteritems():
            key = six.text_type(key)
            msg['_context_' + key] = value

        properties = pika_spec.BasicProperties(
            content_encoding=self.content_encoding,
            content_type=self.content_type,
            delivery_mode=2 if persistent else 1
        )

        if timeout:
            properties.expiration = str(timeout * 1000)

        res = self._pika_engine.publish(
            exchange=exchange, routing_key=rouiting_key,
            body=jsonutils.dumps(
                common.serialize_msg(msg),
                encoding=self.content_encoding
            ),
            properties=properties,
            confirm=confirm,
            mandatory=mandatory
        )

        if not res:
            raise RoutingException(
                "Unable to route the message. Probably no bind queue exists"
            )

        if wait_for_reply:
            if not reply_received.wait(timeout):
                raise common.Timeout()
            return reply[0]

        return None


class PikaListener(object):
    def __init__(self, pika_engine, no_ack=True, prefetch_count=1):
        self._pika_engine = pika_engine

        self._connection = None
        self._channel = None
        self._lock = threading.Lock()

        self._prefetch_count = prefetch_count
        self._no_ack = no_ack

        self._started = False

        self._message_queue = collections.deque()

        self.start()

    def _reconnect(self):
        self._connection = self._pika_engine.create_connection()
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self._prefetch_count)

        self._on_reconnected()

    def _on_reconnected(self):
        raise NotImplementedError(
            "It is base class. Please declare consumers here"
        )

    def _start_consuming(self, queue):
        self._channel.basic_consume(self._on_message_callback,
                                    queue, no_ack=self._no_ack)

    def _on_message_callback(self, unused, method, properties, body):
            self._message_queue.append(
                (self._channel, method, properties, body)
            )

    def _cleanup(self):
        if self._channel:
            try:
                self._channel.close()
            except Exception as ex:
                if not pika_pool.Connection.is_connection_invalidated(ex):
                    LOG.exception("Unexpected error during closing channel")
            self._channel = None

        if self._connection:
            try:
                self._connection.close()
            except Exception as ex:
                if not pika_pool.Connection.is_connection_invalidated(ex):
                    LOG.exception("Unexpected error during closing connection")
            self._connection = None

    def poll(self, timeout=None):
        start = time.time()
        while not self._message_queue:
            if not self._started:
                return None
            with self._lock:
                if self._channel is None:
                    self._reconnect()
                try:
                    self._connection.process_data_events()
                except pika_pool.Connection.connectivity_errors:
                    self._cleanup()
            if timeout and time.time() - start > timeout:
                return None

        return self._message_queue.popleft()

    def start(self):
        self._started = True

    def stop(self):
        self._started = False
        with self._lock:
            self._cleanup()

    def cleanup(self):
        with self._lock:
            self._cleanup()


class RpcServicePikaListener(PikaListener):
    def __init__(self, pika_engine, target, no_ack=True, prefetch_count=1):
        super(RpcServicePikaListener, self).__init__(
            pika_engine, no_ack=no_ack, prefetch_count=prefetch_count)
        self._target = target

    def _on_reconnected(self):
        exchange = (self._target.exchange or
                    self._pika_engine.default_exchange)
        queue = '{}'.format(self._target.topic)
        server_queue = '{}.{}'.format(queue, self._target.server)

        fanout_exchange = '{}_fanout'.format(self._target.topic)

        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding(
            exchange, queue, 'direct', queue_expiration=queue_expiration
        )
        self._pika_engine.declare_queue_binding(
            exchange, server_queue, 'direct', queue_expiration=queue_expiration
        )
        self._pika_engine.declare_queue_binding(
            fanout_exchange, server_queue, 'fanout',
            queue_expiration=queue_expiration
        )

        self._start_consuming(queue)
        self._start_consuming(server_queue)

    def poll(self, timeout=None):
        msg = super(RpcServicePikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class RpcReplyPikaListener(PikaListener):
    def __init__(self, pika_engine, exchange, queue, no_ack=True,
                 prefetch_count=1):
        super(RpcReplyPikaListener, self).__init__(
            pika_engine, no_ack, prefetch_count
        )
        self.exchange = exchange
        self.queue = queue
        self._reconnect()

    def _on_reconnected(self):
        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding(
            self.exchange, self.queue, 'direct',
            queue_expiration=queue_expiration
        )
        self._start_consuming(self.queue)

    def poll(self, timeout=None):
        msg = super(RpcReplyPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class NotificationPikaListener(PikaListener):
    def __init__(self, pika_engine, targets_and_priorities,
                 queue_name=None, prefetch_count=100):
        super(NotificationPikaListener, self).__init__(
            pika_engine, no_ack=False, prefetch_count=prefetch_count
        )
        self._targets_and_priorities = targets_and_priorities
        self.queue_name = queue_name

    def _on_reconnected(self):
        queues_to_consume = set()
        for target, priority in self._targets_and_priorities:
            routing_key = '%s.%s' % (target.topic, priority)
            queue = self.queue_name or routing_key
            self._pika_engine.declare_queue_binding(
                exchange=(
                    target.exchange or
                    self._pika_engine.default_notification_exchange
                ),
                exchange_type='direct',
                queue = queue,
                routing_key=routing_key,
                queue_expiration=None,
                queue_auto_delete=False,
                durable=self._pika_engine.notification_persistence
            )
            queues_to_consume.add(queue)

        for queue_to_consume in queues_to_consume:
            self._start_consuming(queue_to_consume)

    def poll(self, timeout=None):
        msg = super(NotificationPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class PooledConnectionWithConfirmations(pika_pool.Connection):
    @property
    def channel(self):
        if self.fairy.channel is None:
            self.fairy.channel = self.fairy.cxn.channel()
            self.fairy.channel.confirm_delivery()
        return self.fairy.channel


class ExchangeNotFoundException(Exception):
    pass


class RoutingException(Exception):
    pass


class PikaEngine(object):
    def __init__(self, conf, url, default_exchange=None):
        self.conf = conf

        self.default_exchange = default_exchange
        self.rpc_reply_exchange = (
            conf.oslo_messaging_pika.rpc_reply_exchange
        )
        self.default_notification_exchange = (
            conf.oslo_messaging_pika.default_durable_exchange if
            conf.oslo_messaging_pika.notification_persistence else
            default_exchange
        )
        self.notification_persistence = (
            conf.oslo_messaging_pika.notification_persistence
        )

        self._reply_listener = None
        self._reply_queue = None
        self._on_reply_callback_list = []

        self._run = True
        self._pooler_thread = threading.Thread(target=self._poller)
        self._pooler_thread.daemon = True


        self._pika_next_connection_num = 0


        common_pika_params = {
            'virtual_host': url.virtual_host,
            'channel_max': self.conf.oslo_messaging_pika.channel_max,
            'frame_max': self.conf.oslo_messaging_pika.frame_max,
            'heartbeat_interval':
                self.conf.oslo_messaging_pika.heartbeat_interval,
            'ssl': self.conf.oslo_messaging_pika.ssl,
            'ssl_options': self.conf.oslo_messaging_pika.ssl_options,
            'connection_attempts':
                self.conf.oslo_messaging_pika.connection_attempts,
            'retry_delay': self.conf.oslo_messaging_pika.retry_delay,
            'socket_timeout': self.conf.oslo_messaging_pika.socket_timeout,
            'locale': self.conf.oslo_messaging_pika.locale,
            'backpressure_detection': (
                self.conf.oslo_messaging_pika.backpressure_detection
            )
        }

        self._pika_params_list = []
        self._create_connection_lock = threading.Lock()

        for transport_host in url.hosts:
            pika_params = pika.ConnectionParameters(
                host=transport_host.hostname,
                port=transport_host.port,
                credentials=pika_credentials.PlainCredentials(
                    transport_host.username, transport_host.password
                ),
                **common_pika_params
            )
            self._pika_params_list.append(pika_params)

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

    def create_connection(self):
        host_num = len(self._pika_params_list)
        retries = host_num
        while retries > 0:
            with self._create_connection_lock:
                try:
                    return self.create_host_connection(
                        self._pika_next_connection_num
                    )
                except pika_pool.Connection.connectivity_errors:
                    retries -= 1
                    continue
                finally:
                    self._pika_next_connection_num += 1
                    self._pika_next_connection_num %= host_num
        raise

    def create_host_connection(self, host_index):
        """
        Create new connection to host #host_index
        :return: New connection
        """
        return pika_adapters.BlockingConnection(
            self._pika_params_list[host_index]
        )

    def declare_queue_binding(self, exchange, queue, routing_key=None,
                              exchange_type='direct', queue_expiration=None,
                              queue_auto_delete=False, durable=False):
        with self.connection_pool.acquire() as conn:
            conn.channel.exchange_declare(
                exchange, exchange_type, auto_delete=True, durable=durable
            )
            arguments = {}

            if queue_expiration > 0:
                arguments['x-expires'] = queue_expiration * 1000

            conn.channel.queue_declare(queue, auto_delete=queue_auto_delete,
                                       durable=durable, arguments=arguments)

            conn.channel.queue_bind(queue, exchange, routing_key)

    def publish(self, exchange, routing_key, body, properties,
                confirm=True, mandatory=False):
        pool = (self.connection_with_confirmation_pool if confirm else
                self.connection_pool)

        # in case of connection error we will check all possible connections
        # from pool and then will try to reconnect to all configured hosts
        retries = (self.conf.oslo_messaging_pika.pool_max_size +
                   len(self._pika_params_list))

        while retries > 0:
            try:
                with pool.acquire() as conn:
                    return conn.channel.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=properties,
                        mandatory=mandatory
                    )
            except pika_pool.Connection.connectivity_errors as e:
                if (isinstance(e, pika_exceptions.ChannelClosed)
                        and e.args and e.args[0] == 404):
                    raise ExchangeNotFoundException(e.args[1])

                LOG.exception(
                    "Error during replying on message. Trying to send it via "
                    "another connection."
                )
            retries -= 1
        raise

    def get_reply_q(self):
        if self._reply_queue is None:
            self._reply_queue = "{}.{}.{}".format(
                self.conf.project, self.conf.prog, uuid.uuid4().hex
            )

            self._reply_listener = RpcReplyPikaListener(
                pika_engine=self,
                exchange=self.rpc_reply_exchange,
                queue=self._reply_queue
            )

            self._pooler_thread.start()

        return self._reply_queue

    def _poller(self):
        while self._run:
            try:
                message = self._reply_listener.poll()
                if message is None:
                    continue
                i = 0
                curtime = time.time()
                while i < len(self._on_reply_callback_list):
                    msg_id, callback, expiration = (
                        self._on_reply_callback_list[i]
                    )
                    if expiration and expiration < curtime:
                        del self._on_reply_callback_list[i]
                    if msg_id == message.msg_id:
                        del self._on_reply_callback_list[i]
                        callback(message)
                    else:
                        i += 1
            except Exception:
                LOG.exception("Exception during reply polling")

    def register_reply_callback(self, msg_id, on_reply, timeout):
        self._on_reply_callback_list.append(
            (msg_id, on_reply, time.time() + timeout if timeout else None)
        )

    def cleanup(self):
        self._run = False
        if self._pooler_thread:
            self._pooler_thread.join()

        self._reply_listener.stop()
        self._reply_listener.cleanup()


class PikaDriver(object):
    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        if 'eventlet.patcher' in sys.modules:
            import eventlet.patcher
            if eventlet.patcher.is_monkey_patched('select'):
                import select

                try:
                    del select.poll
                except AttributeError:
                    pass

                try:
                    del select.epoll
                except AttributeError:
                    pass

        opt_group = cfg.OptGroup(name='oslo_messaging_pika',
                                 title='Pika driver options')
        conf.register_group(opt_group)
        conf.register_opts(pika_opts, group=opt_group)
        conf.register_opts(pika_pool_opts, group=opt_group)
        conf.register_opts(rpc_opts, group=opt_group)
        conf.register_opts(notification_opts, group=opt_group)
        conf.register_opts(extra_messaging_opts, group=opt_group)

        self.conf = conf
        self._allowed_remote_exmods = allowed_remote_exmods

        self._pika_engine = PikaEngine(conf, url, default_exchange)

    def require_features(self, requeue=False):
        pass

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):

        msg = PikaOutgoingMessage(self._pika_engine, message, ctxt)

        if target.fanout:
            msg.send(
                exchange='{}_fanout'.format(target.topic),
                timeout=timeout
            )
        else:
            queue = target.topic
            if target.server:
                queue = '{}.{}'.format(queue, target.server)

            reply = msg.send(
                exchange=target.exchange or self._pika_engine.default_exchange,
                rouiting_key=queue,
                wait_for_reply=wait_for_reply,
                timeout=timeout
            )

            if reply is None:
                return None

            if reply.message['failure']:
                ex = common.deserialize_remote_exception(
                    reply.message['failure'], self._allowed_remote_exmods
                )
                raise ex
            else:
                return reply.message['result']

    def send_notification(self, target, ctxt, message, version, retry=None):
        msg = PikaOutgoingMessage(self._pika_engine, message, ctxt)

        retryCount = 1 if retry else 2

        while retryCount > 0:
            try:
                msg.send(
                    exchange=(
                        target.exchange or
                        self._pika_engine.default_notification_exchange
                    ),
                    rouiting_key=target.topic,
                    mandatory=True,
                    persistent=self._pika_engine.notification_persistence
                )
                break
            except (ExchangeNotFoundException, RoutingException):
                self._pika_engine.declare_queue_binding(
                    exchange=(target.exchange or
                              self._pika_engine.default_notification_exchange),
                    queue=target.topic,
                    routing_key=target.topic,
                    exchange_type='direct',
                    queue_expiration=False,
                    queue_auto_delete=False,
                    durable=self._pika_engine.notification_persistence
                )
            except Exception:
                LOG.exception(
                    "Error during sending notification. Trying again."
                )
                retryCount -= 1

    def listen(self, target):
        return RpcServicePikaListener(self._pika_engine, target)

    def listen_for_notifications(self, targets_and_priorities, pool):
        return NotificationPikaListener(self._pika_engine,
                                        targets_and_priorities, pool)

    def cleanup(self):
        self._pika_engine.cleanup()