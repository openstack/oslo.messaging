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

import retrying

import sys

from oslo_config import cfg
from oslo_log import log as logging

from oslo_messaging._drivers import common
from oslo_messaging._drivers.pika_driver import pika_engine
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_listener
from oslo_messaging._drivers.pika_driver import pika_message

from oslo_messaging import exceptions

LOG = logging.getLogger(__name__)

pika_opts = [
    cfg.IntOpt('channel_max', default=None,
               help='Maximum number of channels to allow'),
    cfg.IntOpt('frame_max', default=None,
               help='The maximum byte size for an AMQP frame'),
    cfg.IntOpt('heartbeat_interval', default=1,
               help="How often to send heartbeats for consumer's connections"),
    cfg.BoolOpt('ssl', default=None,
                help='Enable SSL'),
    cfg.DictOpt('ssl_options', default=None,
                help='Arguments passed to ssl.wrap_socket'),
    cfg.FloatOpt('socket_timeout', default=0.25,
                 help="Set socket timeout in seconds for connection's socket"),
    cfg.FloatOpt('tcp_user_timeout', default=0.25,
                 help="Set TCP_USER_TIMEOUT in seconds for connection's "
                      "socket"),
    cfg.FloatOpt('host_connection_reconnect_delay', default=5,
                 help="Set delay for reconnection to some host which has "
                      "connection error")
]

pika_pool_opts = [
    cfg.IntOpt('pool_max_size', default=10,
               help="Maximum number of connections to keep queued."),
    cfg.IntOpt('pool_max_overflow', default=0,
               help="Maximum number of connections to create above "
                    "`pool_max_size`."),
    cfg.IntOpt('pool_timeout', default=30,
               help="Default number of seconds to wait for a connections to "
                    "available"),
    cfg.IntOpt('pool_recycle', default=600,
               help="Lifetime of a connection (since creation) in seconds "
                    "or None for no recycling. Expired connections are "
                    "closed on acquire."),
    cfg.IntOpt('pool_stale', default=60,
               help="Threshold at which inactive (since release) connections "
                    "are considered stale in seconds or None for no "
                    "staleness. Stale connections are closed on acquire.")
]

notification_opts = [
    cfg.BoolOpt('notification_persistence', default=False,
                help="Persist notification messages."),
    cfg.StrOpt('default_notification_exchange',
               default="${control_exchange}_notification",
               help="Exchange name for for sending notifications"),
    cfg.IntOpt(
        'default_notification_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending notification, -1 means infinite retry."
    ),
    cfg.FloatOpt(
        'notification_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending notification message"
    )
]

rpc_opts = [
    cfg.IntOpt('rpc_queue_expiration', default=60,
               help="Time to live for rpc queues without consumers in "
                    "seconds."),
    cfg.StrOpt('default_rpc_exchange', default="${control_exchange}_rpc",
               help="Exchange name for for sending RPC messages"),
    cfg.StrOpt('rpc_reply_exchange', default="${control_exchange}_rpc_reply",
               help="Exchange name for for receiving RPC replies"),

    cfg.BoolOpt('rpc_listener_ack', default=True,
                help="Disable to increase performance. If disabled - some "
                     "messages may be lost in case of connectivity problem. "
                     "If enabled - may cause not needed message redelivery "
                     "and rpc request could be processed more then one time"),
    cfg.BoolOpt('rpc_reply_listener_ack', default=True,
                help="Disable to increase performance. If disabled - some "
                     "replies may be lost in case of connectivity problem."),
    cfg.IntOpt(
        'rpc_listener_prefetch_count', default=10,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc listener. Works only if rpc_listener_ack == True"
    ),
    cfg.IntOpt(
        'rpc_reply_listener_prefetch_count', default=10,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc reply listener. Works only if rpc_reply_listener_ack == "
             "True"
    ),
    cfg.IntOpt(
        'rpc_reply_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending reply. -1 means infinite retry during rpc_timeout"
    ),
    cfg.FloatOpt(
        'rpc_reply_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending reply."
    ),
    cfg.IntOpt(
        'default_rpc_retry_attempts', default=-1,
        help="Reconnecting retry count in case of connectivity problem during "
             "sending RPC message, -1 means infinite retry. If actual "
             "retry attempts in not 0 the rpc request could be processed more "
             "then one time"
    ),
    cfg.FloatOpt(
        'rpc_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending RPC message"
    )
]


def _is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('thread')


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

        self.conf = conf
        self._allowed_remote_exmods = allowed_remote_exmods

        self._pika_engine = pika_engine.PikaEngine(conf, url, default_exchange)

    def require_features(self, requeue=False):
        pass

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):

        if retry is None:
            retry = self._pika_engine.default_rpc_retry_attempts

        def on_exception(ex):
            if isinstance(ex, (pika_drv_exc.ConnectionException,
                               exceptions.MessageDeliveryFailure)):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = (
            None if retry == 0 else
            retrying.retry(
                stop_max_attempt_number=(None if retry == -1 else retry),
                retry_on_exception=on_exception,
                wait_fixed=self._pika_engine.rpc_retry_delay * 1000,
            )
        )

        msg = pika_message.PikaOutgoingMessage(self._pika_engine, message,
                                               ctxt)

        if target.fanout:
            return msg.send(
                exchange='{}_fanout_{}'.format(
                    self._pika_engine.default_rpc_exchange, target.topic
                ),
                timeout=timeout, confirm=True, mandatory=False,
                retrier=retrier
            )

        queue = target.topic
        if target.server:
            queue = '{}.{}'.format(queue, target.server)

        reply = msg.send(
            exchange=target.exchange or self._pika_engine.default_rpc_exchange,
            routing_key=queue,
            wait_for_reply=wait_for_reply,
            timeout=timeout,
            confirm=True,
            mandatory=True,
            retrier=retrier
        )

        if reply is not None:
            if reply.message['failure']:
                ex = common.deserialize_remote_exception(
                    reply.message['failure'], self._allowed_remote_exmods
                )
                raise ex

            return reply.message['result']

    def send_notification(self, target, ctxt, message, version, retry=None):
        if retry is None:
            retry = self._pika_engine.default_notification_retry_attempts

        def on_exception(ex):
            if isinstance(ex, (pika_drv_exc.ExchangeNotFoundException,
                               pika_drv_exc.RoutingException)):
                LOG.warn(str(ex))
                try:
                    self._pika_engine.declare_queue_binding(
                        exchange=(
                            target.exchange or
                            self._pika_engine.default_notification_exchange
                        ),
                        queue=target.topic,
                        routing_key=target.topic,
                        exchange_type='direct',
                        queue_expiration=None,
                        queue_auto_delete=False,
                        durable=self._pika_engine.notification_persistence,
                    )
                except pika_drv_exc.ConnectionException as e:
                    LOG.warn(str(e))
                return True
            elif isinstance(ex, (pika_drv_exc.ConnectionException,
                                 pika_drv_exc.MessageRejectedException)):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = retrying.retry(
            stop_max_attempt_number=(None if retry == -1 else retry),
            retry_on_exception=on_exception,
            wait_fixed=self._pika_engine.notification_retry_delay * 1000,
        )

        msg = pika_message.PikaOutgoingMessage(self._pika_engine, message,
                                               ctxt)

        return msg.send(
            exchange=(
                target.exchange or
                self._pika_engine.default_notification_exchange
            ),
            routing_key=target.topic,
            wait_for_reply=False,
            confirm=True,
            mandatory=True,
            persistent=self._pika_engine.notification_persistence,
            retrier=retrier
        )

    def listen(self, target):
        listener = pika_listener.RpcServicePikaListener(
            self._pika_engine, target,
            no_ack=not self._pika_engine.rpc_listener_ack,
            prefetch_count=self._pika_engine.rpc_listener_prefetch_count
        )
        listener.start()
        return listener

    def listen_for_notifications(self, targets_and_priorities, pool):
        listener = pika_listener.NotificationPikaListener(
            self._pika_engine, targets_and_priorities, pool
        )
        listener.start()
        return listener

    def cleanup(self):
        self._pika_engine.cleanup()


class PikaDriverCompatibleWithRabbitDriver(PikaDriver):
    """Old RabbitMQ driver creates exchange before sending message.
    In this case if no rpc service listen this exchange message will be sent
    to /dev/null but client will know anything about it. That is strange.
    But for now we need to keep original behaviour
    """
    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        try:
            return super(PikaDriverCompatibleWithRabbitDriver, self).send(
                target=target,
                ctxt=ctxt,
                message=message,
                wait_for_reply=wait_for_reply,
                timeout=timeout,
                retry=retry
            )
        except exceptions.MessageDeliveryFailure:
            if wait_for_reply:
                raise exceptions.MessagingTimeout()
            else:
                return None
