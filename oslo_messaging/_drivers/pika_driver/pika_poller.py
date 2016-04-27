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

from oslo_log import log as logging
from oslo_service import loopingcall

from oslo_messaging._drivers import base
from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_message as pika_drv_msg

LOG = logging.getLogger(__name__)


class PikaPoller(base.Listener):
    """Provides user friendly functionality for RabbitMQ message consuming,
    handles low level connectivity problems and restore connection if some
    connectivity related problem detected
    """

    def __init__(self, pika_engine, batch_size, batch_timeout, prefetch_count,
                 incoming_message_class):
        """Initialize required fields

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param batch_size: desired number of messages passed to
            single on_incoming_callback call
        :param batch_timeout: defines how long should we wait for batch_size
            messages if we already have some messages waiting for processing
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        :param incoming_message_class: PikaIncomingMessage, wrapper for
            consumed RabbitMQ message
        """
        super(PikaPoller, self).__init__(batch_size, batch_timeout,
                                         prefetch_count)
        self._pika_engine = pika_engine
        self._incoming_message_class = incoming_message_class

        self._connection = None
        self._channel = None
        self._recover_loopingcall = None
        self._lock = threading.RLock()

        self._cur_batch_buffer = None
        self._cur_batch_timeout_id = None

        self._started = False
        self._closing_connection_by_poller = False

        self._queues_to_consume = None

    def _on_connection_close(self, connection, reply_code, reply_text):
        self._deliver_cur_batch()
        if self._closing_connection_by_poller:
            return
        with self._lock:
            self._connection = None
            self._start_recover_consuming_task()

    def _on_channel_close(self, channel, reply_code, reply_text):
        if self._cur_batch_buffer:
            self._cur_batch_buffer = [
                message for message in self._cur_batch_buffer
                if not message.need_ack()
            ]
        if self._closing_connection_by_poller:
            return
        with self._lock:
            self._channel = None
            self._start_recover_consuming_task()

    def _on_consumer_cancel(self, method_frame):
        with self._lock:
            if self._queues_to_consume:
                consumer_tag = method_frame.method.consumer_tag
                for queue_info in self._queues_to_consume:
                    if queue_info["consumer_tag"] == consumer_tag:
                        queue_info["consumer_tag"] = None

            self._start_recover_consuming_task()

    def _on_message_no_ack_callback(self, unused, method, properties, body):
        """Is called by Pika when message was received from queue listened with
        no_ack=True mode
        """
        incoming_message = self._incoming_message_class(
            self._pika_engine, None, method, properties, body
        )
        self._on_incoming_message(incoming_message)

    def _on_message_with_ack_callback(self, unused, method, properties, body):
        """Is called by Pika when message was received from queue listened with
        no_ack=False mode
        """
        incoming_message = self._incoming_message_class(
            self._pika_engine, self._channel, method, properties, body
        )
        self._on_incoming_message(incoming_message)

    def _deliver_cur_batch(self):
        if self._cur_batch_timeout_id is not None:
            self._connection.remove_timeout(self._cur_batch_timeout_id)
            self._cur_batch_timeout_id = None
        if self._cur_batch_buffer:
            buf_to_send = self._cur_batch_buffer
            self._cur_batch_buffer = None
            try:
                self.on_incoming_callback(buf_to_send)
            except Exception:
                LOG.exception("Unexpected exception during incoming delivery")

    def _on_incoming_message(self, incoming_message):
        if self._cur_batch_buffer is None:
            self._cur_batch_buffer = [incoming_message]
        else:
            self._cur_batch_buffer.append(incoming_message)

        if len(self._cur_batch_buffer) >= self.batch_size:
            self._deliver_cur_batch()
            return

        if self._cur_batch_timeout_id is None:
            self._cur_batch_timeout_id = self._connection.add_timeout(
                self.batch_timeout, self._deliver_cur_batch)

    def _start_recover_consuming_task(self):
        """Start async job for checking connection to the broker."""
        if self._recover_loopingcall is None and self._started:
            self._recover_loopingcall = (
                loopingcall.DynamicLoopingCall(
                    self._try_recover_consuming
                )
            )
            LOG.info("Starting recover consuming job for listener: %s", self)
            self._recover_loopingcall.start()

    def _try_recover_consuming(self):
        with self._lock:
            try:
                if self._started:
                    self._start_or_recover_consuming()
            except pika_drv_exc.EstablishConnectionException as e:
                LOG.warning(
                    "Problem during establishing connection for pika "
                    "poller %s", e, exc_info=True
                )
                return self._pika_engine.host_connection_reconnect_delay
            except pika_drv_exc.ConnectionException as e:
                LOG.warning(
                    "Connectivity exception during starting/recovering pika "
                    "poller %s", e, exc_info=True
                )
            except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as e:
                LOG.warning(
                    "Connectivity exception during starting/recovering pika "
                    "poller %s", e, exc_info=True
                )
            except BaseException:
                # NOTE (dukhlov): I preffer to use here BaseException because
                # if this method raise such exception LoopingCall stops
                # execution Probably it should never happen and Exception
                # should be enough but in case of programmer mistake it could
                # be and it is potentially hard to catch problem if we will
                # stop background task. It is better when it continue to work
                # and write a lot of LOG with this error
                LOG.exception("Unexpected exception during "
                              "starting/recovering pika poller")
            else:
                self._recover_loopingcall = None
                LOG.info("Recover consuming job was finished for listener: %s",
                         self)
                raise loopingcall.LoopingCallDone(True)
            return 0

    def _start_or_recover_consuming(self):
        """Performs reconnection to the broker. It is unsafe method for
        internal use only
        """
        if self._connection is None or not self._connection.is_open:
            self._connection = self._pika_engine.create_connection(
                for_listening=True
            )
            self._connection.add_on_close_callback(self._on_connection_close)
            self._channel = None

        if self._channel is None or not self._channel.is_open:
            if self._queues_to_consume:
                for queue_info in self._queues_to_consume:
                    queue_info["consumer_tag"] = None

            self._channel = self._connection.channel()
            self._channel.add_on_close_callback(self._on_channel_close)
            self._channel.add_on_cancel_callback(self._on_consumer_cancel)
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
                if queue_info["consumer_tag"] is not None:
                    continue
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

    def start(self, on_incoming_callback):
        """Starts poller. Should be called before polling to allow message
        consuming

        :param on_incoming_callback: callback function to be executed when
            listener received messages. Messages should be processed and
            acked/nacked by callback
        """
        super(PikaPoller, self).start(on_incoming_callback)

        with self._lock:
            if self._started:
                return
            connected = False
            try:
                self._start_or_recover_consuming()
            except pika_drv_exc.EstablishConnectionException as exc:
                LOG.warning(
                    "Can not establish connection during pika poller's "
                    "start(). %s", exc, exc_info=True
                )
            except pika_drv_exc.ConnectionException as exc:
                LOG.warning(
                    "Connectivity problem during pika poller's start().  %s",
                    exc, exc_info=True
                )
            except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as exc:
                LOG.warning(
                    "Connectivity problem during pika poller's start(). %s",
                    exc, exc_info=True
                )
            else:
                connected = True

            self._started = True
            if not connected:
                self._start_recover_consuming_task()

    def stop(self):
        """Stops poller. Should be called when polling is not needed anymore to
        stop new message consuming. After that it is necessary to poll already
        prefetched messages
        """
        super(PikaPoller, self).stop()

        with self._lock:
            if not self._started:
                return

            if self._recover_loopingcall is not None:
                self._recover_loopingcall.stop()
                self._recover_loopingcall = None

            if (self._queues_to_consume and self._channel and
                    self._channel.is_open):
                try:
                    self._stop_consuming()
                except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as exc:
                    LOG.warning(
                        "Connectivity problem detected during consumer "
                        "cancellation. %s", exc, exc_info=True
                    )
            self._deliver_cur_batch()
            self._started = False

    def cleanup(self):
        """Cleanup allocated resources (channel, connection, etc)."""
        with self._lock:
            if self._connection and self._connection.is_open:
                try:
                    self._closing_connection_by_poller = True
                    self._connection.close()
                    self._closing_connection_by_poller = False
                except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS:
                    # expected errors
                    pass
                except Exception:
                    LOG.exception("Unexpected error during closing connection")
                finally:
                    self._channel = None
                    self._connection = None


class RpcServicePikaPoller(PikaPoller):
    """PikaPoller implementation for polling RPC messages. Overrides base
    functionality according to RPC specific
    """
    def __init__(self, pika_engine, target, batch_size, batch_timeout,
                 prefetch_count):
        """Adds target parameter for declaring RPC specific exchanges and
        queues

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param target: Target, oslo.messaging Target object which defines RPC
            endpoint
        :param batch_size: desired number of messages passed to
            single on_incoming_callback call
        :param batch_timeout: defines how long should we wait for batch_size
            messages if we already have some messages waiting for processing
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        """
        self._target = target

        super(RpcServicePikaPoller, self).__init__(
            pika_engine, batch_size, batch_timeout, prefetch_count,
            pika_drv_msg.RpcPikaIncomingMessage
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
    def __init__(self, pika_engine, exchange, queue, batch_size, batch_timeout,
                 prefetch_count):
        """Adds exchange and queue parameter for declaring exchange and queue
        used for RPC reply delivery

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param exchange: String, exchange name used for RPC reply delivery
        :param queue: String, queue name used for RPC reply delivery
        :param batch_size: desired number of messages passed to
            single on_incoming_callback call
        :param batch_timeout: defines how long should we wait for batch_size
            messages if we already have some messages waiting for processing
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        """
        self._exchange = exchange
        self._queue = queue

        super(RpcReplyPikaPoller, self).__init__(
            pika_engine, batch_size, batch_timeout, prefetch_count,
            pika_drv_msg.RpcReplyPikaIncomingMessage
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
    def __init__(self, pika_engine, targets_and_priorities,
                 batch_size, batch_timeout, prefetch_count, queue_name=None):
        """Adds targets_and_priorities and queue_name parameter
        for declaring exchanges and queues used for notification delivery

        :param pika_engine: PikaEngine, shared object with configuration and
            shared driver functionality
        :param targets_and_priorities: list of (target, priority), defines
            default queue names for corresponding notification types
        :param batch_size: desired number of messages passed to
            single on_incoming_callback call
        :param batch_timeout: defines how long should we wait for batch_size
            messages if we already have some messages waiting for processing
        :param prefetch_count: Integer, maximum count of unacknowledged
            messages which RabbitMQ broker sends to this consumer
        :param queue: String, alternative queue name used for this poller
            instead of default queue name
        """
        self._targets_and_priorities = targets_and_priorities
        self._queue_name = queue_name

        super(NotificationPikaPoller, self).__init__(
            pika_engine, batch_size, batch_timeout, prefetch_count,
            pika_drv_msg.PikaIncomingMessage
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
                queue=queue,
                routing_key=routing_key,
                exchange_type='direct',
                queue_expiration=None,
                durable=self._pika_engine.notification_persistence,
            )
            queues_to_consume.append(
                {"queue_name": queue, "no_ack": False, "consumer_tag": None}
            )

        return queues_to_consume
