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

from debtcollector import deprecate
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import timeutils
import pika_pool
import tenacity

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common
from oslo_messaging._drivers.pika_driver import (pika_connection_factory as
                                                 pika_drv_conn_factory)
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_engine as pika_drv_engine
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_listener as pika_drv_lstnr
from oslo_messaging._drivers.pika_driver import pika_message as pika_drv_msg
from oslo_messaging._drivers.pika_driver import pika_poller as pika_drv_poller
from oslo_messaging import exceptions

LOG = logging.getLogger(__name__)

pika_pool_opts = [
    cfg.IntOpt('pool_max_size', default=30,
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

message_opts = [
    cfg.StrOpt('default_serializer_type', default='json',
               choices=('json', 'msgpack'),
               help="Default serialization mechanism for "
                    "serializing/deserializing outgoing/incoming messages")
]

notification_opts = [
    cfg.BoolOpt('notification_persistence', default=False,
                help="Persist notification messages."),
    cfg.StrOpt('default_notification_exchange',
               default="${control_exchange}_notification",
               help="Exchange name for sending notifications"),
    cfg.IntOpt(
        'notification_listener_prefetch_count', default=100,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to notification listener."
    ),
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
               help="Exchange name for sending RPC messages"),
    cfg.StrOpt('rpc_reply_exchange', default="${control_exchange}_rpc_reply",
               help="Exchange name for receiving RPC replies"),
    cfg.IntOpt(
        'rpc_listener_prefetch_count', default=100,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc listener."
    ),
    cfg.IntOpt(
        'rpc_reply_listener_prefetch_count', default=100,
        help="Max number of not acknowledged message which RabbitMQ can send "
             "to rpc reply listener."
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
             "than one time"
    ),
    cfg.FloatOpt(
        'rpc_retry_delay', default=0.25,
        help="Reconnecting retry delay in case of connectivity problem during "
             "sending RPC message"
    )
]


class PikaDriver(base.BaseDriver):
    """Pika Driver

    **Warning**: The ``pika`` driver has been deprecated and will be removed in
    a future release.  It is recommended that all users of the ``pika`` driver
    transition to using the ``rabbit`` driver.
    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):

        deprecate("The pika driver is no longer maintained. It has been"
                  " deprecated",
                  message="It is recommended that all users of the pika driver"
                  " transition to using the rabbit driver.",
                  version="pike", removal_version="rocky")

        opt_group = cfg.OptGroup(name='oslo_messaging_pika',
                                 title='Pika driver options')
        conf.register_group(opt_group)
        conf.register_opts(pika_drv_conn_factory.pika_opts, group=opt_group)
        conf.register_opts(pika_pool_opts, group=opt_group)
        conf.register_opts(message_opts, group=opt_group)
        conf.register_opts(rpc_opts, group=opt_group)
        conf.register_opts(notification_opts, group=opt_group)
        conf = common.ConfigOptsProxy(conf, url, opt_group.name)

        self._pika_engine = pika_drv_engine.PikaEngine(
            conf, url, default_exchange, allowed_remote_exmods
        )
        self._reply_listener = pika_drv_lstnr.RpcReplyPikaListener(
            self._pika_engine
        )
        super(PikaDriver, self).__init__(conf, url, default_exchange,
                                         allowed_remote_exmods)

    def require_features(self, requeue=False):
        pass

    def _declare_rpc_exchange(self, exchange, stopwatch):
        timeout = stopwatch.leftover(return_none=True)
        with (self._pika_engine.connection_without_confirmation_pool
                .acquire(timeout=timeout)) as conn:
            try:
                self._pika_engine.declare_exchange_by_channel(
                    conn.channel,
                    self._pika_engine.get_rpc_exchange_name(
                        exchange
                    ), "direct", False
                )
            except pika_pool.Timeout as e:
                raise exceptions.MessagingTimeout(
                    "Timeout for current operation was expired. {}.".format(
                        str(e)
                    )
                )

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        with timeutils.StopWatch(duration=timeout) as stopwatch:
            if retry is None:
                retry = self._pika_engine.default_rpc_retry_attempts

            exchange = self._pika_engine.get_rpc_exchange_name(
                target.exchange
            )

            def on_exception(ex):
                if isinstance(ex, pika_drv_exc.ExchangeNotFoundException):
                    # it is desired to create exchange because if we sent to
                    # exchange which is not exists, we get ChannelClosed
                    # exception and need to reconnect
                    try:
                        self._declare_rpc_exchange(exchange, stopwatch)
                    except pika_drv_exc.ConnectionException as e:
                        LOG.warning("Problem during declaring exchange. %s", e)
                    return True
                elif isinstance(ex, (pika_drv_exc.ConnectionException,
                                     exceptions.MessageDeliveryFailure)):
                    LOG.warning("Problem during message sending. %s", ex)
                    return True
                else:
                    return False

            if retry:
                retrier = tenacity.retry(
                    stop=(tenacity.stop_never if retry == -1 else
                          tenacity.stop_after_attempt(retry)),
                    retry=tenacity.retry_if_exception(on_exception),
                    wait=tenacity.wait_fixed(self._pika_engine.rpc_retry_delay)
                )
            else:
                retrier = None

            if target.fanout:
                return self.cast_all_workers(
                    exchange, target.topic, ctxt, message, stopwatch, retrier
                )

            routing_key = self._pika_engine.get_rpc_queue_name(
                target.topic, target.server, retrier is None
            )

            msg = pika_drv_msg.RpcPikaOutgoingMessage(self._pika_engine,
                                                      message, ctxt)
            try:
                reply = msg.send(
                    exchange=exchange,
                    routing_key=routing_key,
                    reply_listener=(
                        self._reply_listener if wait_for_reply else None
                    ),
                    stopwatch=stopwatch,
                    retrier=retrier
                )
            except pika_drv_exc.ExchangeNotFoundException as ex:
                try:
                    self._declare_rpc_exchange(exchange, stopwatch)
                except pika_drv_exc.ConnectionException as e:
                    LOG.warning("Problem during declaring exchange. %s", e)
                raise ex

            if reply is not None:
                if reply.failure is not None:
                    raise reply.failure

                return reply.result

    def cast_all_workers(self, exchange, topic, ctxt, message, stopwatch,
                         retrier=None):
        msg = pika_drv_msg.PikaOutgoingMessage(self._pika_engine, message,
                                               ctxt)
        try:
            msg.send(
                exchange=exchange,
                routing_key=self._pika_engine.get_rpc_queue_name(
                    topic, "all_workers", retrier is None
                ),
                mandatory=False,
                stopwatch=stopwatch,
                retrier=retrier
            )
        except pika_drv_exc.ExchangeNotFoundException:
            try:
                self._declare_rpc_exchange(exchange, stopwatch)
            except pika_drv_exc.ConnectionException as e:
                LOG.warning("Problem during declaring exchange. %s", e)

    def _declare_notification_queue_binding(
            self, target, stopwatch=pika_drv_cmns.INFINITE_STOP_WATCH):
        if stopwatch.expired():
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired."
            )
        try:
            timeout = stopwatch.leftover(return_none=True)
            with (self._pika_engine.connection_without_confirmation_pool
                    .acquire)(timeout=timeout) as conn:
                self._pika_engine.declare_queue_binding_by_channel(
                    conn.channel,
                    exchange=(
                        target.exchange or
                        self._pika_engine.default_notification_exchange
                    ),
                    queue=target.topic,
                    routing_key=target.topic,
                    exchange_type='direct',
                    queue_expiration=None,
                    durable=self._pika_engine.notification_persistence,
                )
        except pika_pool.Timeout as e:
            raise exceptions.MessagingTimeout(
                "Timeout for current operation was expired. {}.".format(str(e))
            )

    def send_notification(self, target, ctxt, message, version, retry=None):
        if retry is None:
            retry = self._pika_engine.default_notification_retry_attempts

        def on_exception(ex):
            if isinstance(ex, (pika_drv_exc.ExchangeNotFoundException,
                               pika_drv_exc.RoutingException)):
                LOG.warning("Problem during sending notification. %s", ex)
                try:
                    self._declare_notification_queue_binding(target)
                except pika_drv_exc.ConnectionException as e:
                    LOG.warning("Problem during declaring notification queue "
                                "binding. %s", e)
                return True
            elif isinstance(ex, (pika_drv_exc.ConnectionException,
                                 pika_drv_exc.MessageRejectedException)):
                LOG.warning("Problem during sending notification. %s", ex)
                return True
            else:
                return False

        if retry:
            retrier = tenacity.retry(
                stop=(tenacity.stop_never if retry == -1 else
                      tenacity.stop_after_attempt(retry)),
                retry=tenacity.retry_if_exception(on_exception),
                wait=tenacity.wait_fixed(
                    self._pika_engine.notification_retry_delay
                )
            )
        else:
            retrier = None

        msg = pika_drv_msg.PikaOutgoingMessage(self._pika_engine, message,
                                               ctxt)
        return msg.send(
            exchange=(
                target.exchange or
                self._pika_engine.default_notification_exchange
            ),
            routing_key=target.topic,
            confirm=True,
            mandatory=True,
            persistent=self._pika_engine.notification_persistence,
            retrier=retrier
        )

    def listen(self, target, batch_size, batch_timeout):
        return pika_drv_poller.RpcServicePikaPoller(
            self._pika_engine, target, batch_size, batch_timeout,
            self._pika_engine.rpc_listener_prefetch_count
        )

    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        return pika_drv_poller.NotificationPikaPoller(
            self._pika_engine, targets_and_priorities, batch_size,
            batch_timeout,
            self._pika_engine.notification_listener_prefetch_count, pool
        )

    def cleanup(self):
        self._reply_listener.cleanup()
        self._pika_engine.cleanup()
