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

import collections

from oslo_log import log as logging

import threading
import time

from oslo_messaging._drivers.pika_driver import pika_message
import pika_pool
import retrying

LOG = logging.getLogger(__name__)


class PikaListener(object):
    def __init__(self, pika_engine, no_ack, prefetch_count):
        self._pika_engine = pika_engine

        self._connection = None
        self._channel = None
        self._lock = threading.Lock()

        self._prefetch_count = prefetch_count
        self._no_ack = no_ack

        self._started = False

        self._message_queue = collections.deque()

    def _reconnect(self):
        self._connection = self._pika_engine.create_connection(
            for_listening=True
        )
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
        self._message_queue.append((self._channel, method, properties, body))

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
            with self._lock:
                if not self._started:
                    return None

                try:
                    if self._channel is None:
                        self._reconnect()
                    self._connection.process_data_events()
                except Exception:
                    self._cleanup()
                    raise
            if timeout and time.time() - start > timeout:
                return None

        return self._message_queue.popleft()

    def start(self):
        self._started = True

    def stop(self):
        with self._lock:
            if not self._started:
                return

            self._started = False
            self._cleanup()

    def reconnect(self):
        with self._lock:
            self._cleanup()
            try:
                self._reconnect()
            except Exception:
                self._cleanup()
                raise

    def cleanup(self):
        with self._lock:
            self._cleanup()


class RpcServicePikaListener(PikaListener):
    def __init__(self, pika_engine, target, no_ack, prefetch_count):
        self._target = target

        super(RpcServicePikaListener, self).__init__(
            pika_engine, no_ack=no_ack, prefetch_count=prefetch_count)

    def _on_reconnected(self):
        exchange = (self._target.exchange or
                    self._pika_engine.default_rpc_exchange)
        queue = '{}'.format(self._target.topic)
        server_queue = '{}.{}'.format(queue, self._target.server)

        fanout_exchange = '{}_fanout_{}'.format(
            self._pika_engine.default_rpc_exchange, self._target.topic
        )

        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=exchange, queue=queue, routing_key=queue,
            exchange_type='direct', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )
        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=exchange, queue=server_queue, routing_key=server_queue,
            exchange_type='direct', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )
        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=fanout_exchange, queue=server_queue, routing_key="",
            exchange_type='fanout', queue_expiration=queue_expiration,
            queue_auto_delete=False, durable=False
        )

        self._start_consuming(queue)
        self._start_consuming(server_queue)

    def poll(self, timeout=None):
        msg = super(RpcServicePikaListener, self).poll(timeout)
        if msg is None:
            return None
        return pika_message.PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class RpcReplyPikaListener(PikaListener):
    def __init__(self, pika_engine, exchange, queue, no_ack, prefetch_count):
        self._exchange = exchange
        self._queue = queue

        super(RpcReplyPikaListener, self).__init__(
            pika_engine, no_ack, prefetch_count
        )

    def _on_reconnected(self):
        queue_expiration = (
            self._pika_engine.conf.oslo_messaging_pika.rpc_queue_expiration
        )

        self._pika_engine.declare_queue_binding_by_channel(
            channel=self._channel,
            exchange=self._exchange, queue=self._queue,
            routing_key=self._queue, exchange_type='direct',
            queue_expiration=queue_expiration, queue_auto_delete=False,
            durable=False
        )
        self._start_consuming(self._queue)

    def start(self, timeout=None):
        super(RpcReplyPikaListener, self).start()

        def on_exception(ex):
            LOG.warn(str(ex))

            return True

        retrier = retrying.retry(
            stop_max_attempt_number=self._pika_engine.rpc_reply_retry_attempts,
            stop_max_delay=timeout * 1000,
            wait_fixed=self._pika_engine.rpc_reply_retry_delay * 1000,
            retry_on_exception=on_exception,
        )

        retrier(self.reconnect)()

    def poll(self, timeout=None):
        msg = super(RpcReplyPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return pika_message.PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )


class NotificationPikaListener(PikaListener):
    def __init__(self, pika_engine, targets_and_priorities,
                 queue_name=None, prefetch_count=100):
        self._targets_and_priorities = targets_and_priorities
        self._queue_name = queue_name

        super(NotificationPikaListener, self).__init__(
            pika_engine, no_ack=False, prefetch_count=prefetch_count
        )

    def _on_reconnected(self):
        queues_to_consume = set()
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
                queue_auto_delete=False,
                durable=self._pika_engine.notification_persistence,
            )
            queues_to_consume.add(queue)

        for queue_to_consume in queues_to_consume:
            self._start_consuming(queue_to_consume)

    def poll(self, timeout=None):
        msg = super(NotificationPikaListener, self).poll(timeout)
        if msg is None:
            return None
        return pika_message.PikaIncomingMessage(
            self._pika_engine, *msg, no_ack=self._no_ack
        )
