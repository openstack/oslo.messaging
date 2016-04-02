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

import threading
import time

from oslo_log import log as logging
from oslo_utils import timeutils
import six

from oslo_messaging._drivers import base
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_message as pika_drv_msg

LOG = logging.getLogger(__name__)


class PikaPoller(base.PollStyleListener):
    """Provides user friendly functionality for RabbitMQ message consuming,
    handles low level connectivity problems and restore connection if some
    connectivity related problem detected
    """

    def __init__(self, pika_engine, prefetch_count, incoming_message_class):
        """Initialize required fields

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        :param incoming_message_class: PikaIncomingMessage, wrapper for
            consumed RabbitMQ message
        """
        super(PikaPoller, self).__init__(prefetch_count)
        self._pika_engine = pika_engine
        self._incoming_message_class = incoming_message_class

        self._connection = None
        self._channel = None
        self._lock = threading.Lock()

        self._started = False

        self._queues_to_consume = None

        self._message_queue = []

    def _reconnect(self):
        """Performs reconnection to the broker. It is unsafe method for
        internal use only
        """
        self._connection = self._pika_engine.create_connection(
            for_listening=True
        )
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self.prefetch_size)

        if self._queues_to_consume is None:
            self._queues_to_consume = self._declare_queue_binding()

        self._start_consuming()

    def _declare_queue_binding(self):
        """Is called by recovering connection logic if target RabbitMQ
        exchange and (or) queue do not exist. Should be overridden in child
        classes

        :return Dictionary, declared_queue_name -> no_ack_mode
        """
        raise NotImplementedError(
            "It is base class. Please declare exchanges and queues here"
        )

    def _start_consuming(self):
        """Is called by recovering connection logic for starting consumption
        of configured RabbitMQ queues
        """

        assert self._queues_to_consume is not None

        try:
            for queue_info in self._queues_to_consume:
                no_ack = queue_info["no_ack"]

                on_message_callback = (
                    self._on_message_no_ack_callback if no_ack
                    else self._on_message_with_ack_callback
                )

                queue_info["consumer_tag"] = self._channel.basic_consume(
                    on_message_callback, queue_info["queue_name"],
                    no_ack=no_ack
                )
        except Exception:
            self._queues_to_consume = None
            raise

    def _stop_consuming(self):
        """Is called by poller's stop logic for stopping consumption
        of configured RabbitMQ queues
        """

        assert self._queues_to_consume is not None

        for queue_info in self._queues_to_consume:
            consumer_tag = queue_info["consumer_tag"]
            if consumer_tag is not None:
                self._channel.basic_cancel(consumer_tag)
                queue_info["consumer_tag"] = None

    def _on_message_no_ack_callback(self, unused, method, properties, body):
        """Is called by Pika when message was received from queue listened with
        no_ack=True mode
        """
        self._message_queue.append(
            self._incoming_message_class(
                self._pika_engine, None, method, properties, body
            )
        )

    def _on_message_with_ack_callback(self, unused, method, properties, body):
        """Is called by Pika when message was received from queue listened with
        no_ack=False mode
        """
        self._message_queue.append(
            self._incoming_message_class(
                self._pika_engine, self._channel, method, properties, body
            )
        )

    def _cleanup(self):
        """Cleanup allocated resources (channel, connection, etc). It is unsafe
        method for internal use only
        """
        if self._connection:
            try:
                self._connection.close()
            except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS:
                # expected errors
                pass
            except Exception:
                LOG.exception("Unexpected error during closing connection")
            finally:
                self._channel = None
                self._connection = None

        for i in six.moves.range(len(self._message_queue) - 1, -1, -1):
            message = self._message_queue[i]
            if message.need_ack():
                del self._message_queue[i]

    @base.batch_poll_helper
    def poll(self, timeout=None):
        """Main method of this class - consumes message from RabbitMQ

        :param: timeout: float, seconds, timeout for waiting new incoming
            message, None means wait forever
        :return: list of PikaIncomingMessage, RabbitMQ messages
        """

        with timeutils.StopWatch(timeout) as stop_watch:
            while True:
                with self._lock:
                    if self._message_queue:
                        return self._message_queue.pop(0)

                    if stop_watch.expired():
                        return None

                    try:
                        if self._started:
                            if self._channel is None:
                                self._reconnect()
                            # we need some time_limit here, not too small to
                            # avoid a lot of not needed iterations but not too
                            # large to release lock time to time and give a
                            # chance to perform another method waiting this
                            # lock
                            self._connection.process_data_events(
                                time_limit=0.25
                            )
                        else:
                            # consumer is stopped so we don't expect new
                            # messages, just process already sent events
                            if self._channel is not None:
                                self._connection.process_data_events(
                                    time_limit=0
                                )

                            # and return if we don't see new messages
                            if not self._message_queue:
                                return None
                    except pika_drv_exc.EstablishConnectionException as e:
                        LOG.warning(
                            "Problem during establishing connection for pika "
                            "poller %s", e, exc_info=True
                        )
                        time.sleep(
                            self._pika_engine.host_connection_reconnect_delay
                        )
                    except pika_drv_exc.ConnectionException:
                        self._cleanup()
                        raise
                    except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS:
                        self._cleanup()
                        raise

    def start(self):
        """Starts poller. Should be called before polling to allow message
        consuming
        """
        with self._lock:
            if self._started:
                return

            try:
                self._reconnect()
            except pika_drv_exc.EstablishConnectionException as exc:
                LOG.warning(
                    "Can not establish connection during pika poller's "
                    "start(). Connecting is required during first poll() "
                    "call. %s", exc, exc_info=True
                )
            except pika_drv_exc.ConnectionException as exc:
                self._cleanup()
                LOG.warning(
                    "Connectivity problem during pika poller's start(). "
                    "Reconnecting required during first poll() call. %s",
                    exc, exc_info=True
                )
            except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as exc:
                self._cleanup()
                LOG.warning(
                    "Connectivity problem during pika poller's start(). "
                    "Reconnecting required during first poll() call. %s",
                    exc, exc_info=True
                )
            self._started = True

    def stop(self):
        """Stops poller. Should be called when polling is not needed anymore to
        stop new message consuming. After that it is necessary to poll already
        prefetched messages
        """
        with self._lock:
            if not self._started:
                return

            if self._queues_to_consume and self._channel:
                try:
                    self._stop_consuming()
                except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as exc:
                    self._cleanup()
                    LOG.warning(
                        "Connectivity problem detected during consumer "
                        "cancellation. %s", exc, exc_info=True
                    )
            self._started = False

    def cleanup(self):
        """Safe version of _cleanup. Cleans up allocated resources (channel,
        connection, etc).
        """
        with self._lock:
            self._cleanup()


class RpcServicePikaPoller(PikaPoller):
    """PikaPoller implementation for polling RPC messages. Overrides base
    functionality according to RPC specific
    """
    def __init__(self, pika_engine, target, prefetch_count):
        """Adds target parameter for declaring RPC specific exchanges and
        queues

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param target: Target, oslo.messaging Target object which defines RPC
            endpoint
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        """
        self._target = target

        super(RpcServicePikaPoller, self).__init__(
            pika_engine, prefetch_count=prefetch_count,
            incoming_message_class=pika_drv_msg.RpcPikaIncomingMessage
        )

    def _declare_queue_binding(self):
        """Overrides base method and perform declaration of RabbitMQ exchanges
        and queues which correspond to oslo.messaging RPC target

        :return Dictionary, declared_queue_name -> no_ack_mode
        """
        queue_expiration = self._pika_engine.rpc_queue_expiration

        exchange = self._pika_engine.get_rpc_exchange_name(
            self._target.exchange
        )

        queues_to_consume = []

        for no_ack in [True, False]:
            queue = self._pika_engine.get_rpc_queue_name(
                self._target.topic, None, no_ack
            )
            self._pika_engine.declare_queue_binding_by_channel(
                channel=self._channel, exchange=exchange, queue=queue,
                routing_key=queue, exchange_type='direct', durable=False,
                queue_expiration=queue_expiration
            )
            queues_to_consume.append(
                {"queue_name": queue, "no_ack": no_ack, "consumer_tag": None}
            )

            if self._target.server:
                server_queue = self._pika_engine.get_rpc_queue_name(
                    self._target.topic, self._target.server, no_ack
                )
                self._pika_engine.declare_queue_binding_by_channel(
                    channel=self._channel, exchange=exchange, durable=False,
                    queue=server_queue, routing_key=server_queue,
                    exchange_type='direct', queue_expiration=queue_expiration
                )
                queues_to_consume.append(
                    {"queue_name": server_queue, "no_ack": no_ack,
                     "consumer_tag": None}
                )

                worker_queue = self._pika_engine.get_rpc_queue_name(
                    self._target.topic, self._target.server, no_ack, True
                )
                all_workers_routing_key = self._pika_engine.get_rpc_queue_name(
                    self._target.topic, "all_workers", no_ack
                )
                self._pika_engine.declare_queue_binding_by_channel(
                    channel=self._channel, exchange=exchange, durable=False,
                    queue=worker_queue, routing_key=all_workers_routing_key,
                    exchange_type='direct', queue_expiration=queue_expiration
                )
                queues_to_consume.append(
                    {"queue_name": worker_queue, "no_ack": no_ack,
                     "consumer_tag": None}
                )

        return queues_to_consume


class RpcReplyPikaPoller(PikaPoller):
    """PikaPoller implementation for polling RPC reply messages. Overrides
    base functionality according to RPC reply specific
    """
    def __init__(self, pika_engine, exchange, queue, prefetch_count):
        """Adds exchange and queue parameter for declaring exchange and queue
        used for RPC reply delivery

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param exchange: String, exchange name used for RPC reply delivery
        :param queue: String, queue name used for RPC reply delivery
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        """
        self._exchange = exchange
        self._queue = queue

        super(RpcReplyPikaPoller, self).__init__(
            pika_engine=pika_engine, prefetch_count=prefetch_count,
            incoming_message_class=pika_drv_msg.RpcReplyPikaIncomingMessage
        )

    def _declare_queue_binding(self):
        """Overrides base method and perform declaration of RabbitMQ exchange
        and queue used for RPC reply delivery

        :return Dictionary, declared_queue_name -> no_ack_mode
        """
        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=self._exchange, queue=self._queue,
            routing_key=self._queue, exchange_type='direct',
            queue_expiration=self._pika_engine.rpc_queue_expiration,
            durable=False
        )

        return [{"queue_name": self._queue, "no_ack": False,
                 "consumer_tag": None}]


class NotificationPikaPoller(PikaPoller):
    """PikaPoller implementation for polling Notification messages. Overrides
    base functionality according to Notification specific
    """
    def __init__(self, pika_engine, targets_and_priorities, prefetch_count,
                 queue_name=None):
        """Adds targets_and_priorities and queue_name parameter
        for declaring exchanges and queues used for notification delivery

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param targets_and_priorities: list of (target, priority), defines
            default queue names for corresponding notification types
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        :param queue: String, alternative queue name used for this poller
            instead of default queue name
        """
        self._targets_and_priorities = targets_and_priorities
        self._queue_name = queue_name

        super(NotificationPikaPoller, self).__init__(
            pika_engine, prefetch_count=prefetch_count,
            incoming_message_class=pika_drv_msg.PikaIncomingMessage
        )

    def _declare_queue_binding(self):
        """Overrides base method and perform declaration of RabbitMQ exchanges
        and queues used for notification delivery

        :return Dictionary, declared_queue_name -> no_ack_mode
        """
        queues_to_consume = []
        for target, priority in self._targets_and_priorities:
            routing_key = '%s.%s' % (target.topic, priority)
            queue = self._queue_name or routing_key
            self._pika_engine.declare_queue_binding_by_channel(
                channel=self._channel,
                exchange=(
                    target.exchange or
                    self._pika_engine.default_notification_exchange
                ),
                queue = queue,
                routing_key=routing_key,
                exchange_type='direct',
                queue_expiration=None,
                durable=self._pika_engine.notification_persistence,
            )
            queues_to_consume.append(
                {"queue_name": queue, "no_ack": False, "consumer_tag": None}
            )

        return queues_to_consume
