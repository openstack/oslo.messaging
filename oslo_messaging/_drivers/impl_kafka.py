# Copyright (C) 2015 Cisco Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import threading

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers import pool as driver_pool
from oslo_messaging._i18n import _LE
from oslo_messaging._i18n import _LW
from oslo_serialization import jsonutils

import kafka
from kafka.common import KafkaError
from oslo_config import cfg
from oslo_log import log as logging


LOG = logging.getLogger(__name__)

PURPOSE_SEND = 'send'
PURPOSE_LISTEN = 'listen'

kafka_opts = [
    cfg.StrOpt('kafka_default_host', default='localhost',
               deprecated_for_removal=True,
               deprecated_reason="Replaced by [DEFAULT]/transport_url",
               help='Default Kafka broker Host'),

    cfg.PortOpt('kafka_default_port', default=9092,
                deprecated_for_removal=True,
                deprecated_reason="Replaced by [DEFAULT]/transport_url",
                help='Default Kafka broker Port'),

    cfg.IntOpt('kafka_max_fetch_bytes', default=1024 * 1024,
               help='Max fetch bytes of Kafka consumer'),

    cfg.IntOpt('kafka_consumer_timeout', default=1.0,
               help='Default timeout(s) for Kafka consumers'),

    cfg.IntOpt('pool_size', default=10,
               help='Pool Size for Kafka Consumers'),

    cfg.IntOpt('conn_pool_min_size', default=2,
               help='The pool size limit for connections expiration policy'),

    cfg.IntOpt('conn_pool_ttl', default=1200,
               help='The time-to-live in sec of idle connections in the pool')
]

CONF = cfg.CONF


def pack_context_with_message(ctxt, msg):
    """Pack context into msg."""
    if isinstance(ctxt, dict):
        context_d = ctxt
    else:
        context_d = ctxt.to_dict()

    return {'message': msg, 'context': context_d}


def target_to_topic(target, priority=None):
    """Convert target into topic string

    :param target: Message destination target
    :type target: oslo_messaging.Target
    :param priority: Notification priority
    :type priority: string
    """
    if not priority:
        return target.topic
    return target.topic + '.' + priority


class Connection(object):

    def __init__(self, conf, url, purpose):

        driver_conf = conf.oslo_messaging_kafka

        self.conf = conf
        self.kafka_client = None
        self.producer = None
        self.consumer = None
        self.fetch_messages_max_bytes = driver_conf.kafka_max_fetch_bytes
        self.consumer_timeout = float(driver_conf.kafka_consumer_timeout)
        self.url = url
        self._parse_url()
        # TODO(Support for manual/auto_commit functionality)
        # When auto_commit is False, consumer can manually notify
        # the completion of the subscription.
        # Currently we don't support for non auto commit option
        self.auto_commit = True
        self._consume_loop_stopped = False

    def _parse_url(self):
        driver_conf = self.conf.oslo_messaging_kafka

        self.hostaddrs = []

        for host in self.url.hosts:
            if host.hostname:
                self.hostaddrs.append("%s:%s" % (
                    host.hostname,
                    host.port or driver_conf.kafka_default_port))

        if not self.hostaddrs:
            self.hostaddrs.append("%s:%s" % (driver_conf.kafka_default_host,
                                             driver_conf.kafka_default_port))

    def notify_send(self, topic, ctxt, msg, retry):
        """Send messages to Kafka broker.

        :param topic: String of the topic
        :param ctxt: context for the messages
        :param msg: messages for publishing
        :param retry: the number of retry
        """
        message = pack_context_with_message(ctxt, msg)
        self._ensure_connection()
        self._send_and_retry(message, topic, retry)

    def _send_and_retry(self, message, topic, retry):
        current_retry = 0
        if not isinstance(message, str):
            message = jsonutils.dumps(message)
        while message is not None:
            try:
                self._send(message, topic)
                message = None
            except Exception:
                LOG.warning(_LW("Failed to publish a message of topic %s"),
                            topic)
                current_retry += 1
                if retry is not None and current_retry >= retry:
                    LOG.exception(_LE("Failed to retry to send data "
                                      "with max retry times"))
                    message = None

    def _send(self, message, topic):
        self.producer.send_messages(topic, message)

    def consume(self, timeout=None):
        """Receive up to 'max_fetch_messages' messages.

        :param timeout: poll timeout in seconds
        """
        duration = (self.consumer_timeout if timeout is None else timeout)
        timer = driver_common.DecayingTimer(duration=duration)
        timer.start()

        def _raise_timeout():
            LOG.debug('Timed out waiting for Kafka response')
            raise driver_common.Timeout()

        poll_timeout = (self.consumer_timeout if timeout is None
                        else min(timeout, self.consumer_timeout))

        while True:
            if self._consume_loop_stopped:
                return
            try:
                next_timeout = poll_timeout * 1000.0
                # TODO(use configure() method instead)
                # Currently KafkaConsumer does not support for
                # the case of updating only fetch_max_wait_ms parameter
                self.consumer._config['fetch_max_wait_ms'] = next_timeout
                messages = list(self.consumer.fetch_messages())
            except Exception as e:
                LOG.exception(_LE("Failed to consume messages: %s"), e)
                messages = None

            if not messages:
                poll_timeout = timer.check_return(
                    _raise_timeout, maximum=self.consumer_timeout)
                continue

            return messages

    def stop_consuming(self):
        self._consume_loop_stopped = True

    def reset(self):
        """Reset a connection so it can be used again."""
        if self.consumer:
            self.consumer.close()
        self.consumer = None

    def close(self):
        if self.kafka_client:
            self.kafka_client.close()
        self.kafka_client = None
        if self.producer:
            self.producer.stop()
        self.consumer = None

    def commit(self):
        """Commit is used by subscribers belonging to the same group.
        After subscribing messages, commit is called to prevent
        the other subscribers which belong to the same group
        from re-subscribing the same messages.

        Currently self.auto_commit option is always True,
        so we don't need to call this function.
        """
        self.consumer.commit()

    def _ensure_connection(self):
        if self.kafka_client:
            return
        try:
            self.kafka_client = kafka.KafkaClient(
                self.hostaddrs)
            self.producer = kafka.SimpleProducer(self.kafka_client)
        except KafkaError as e:
            LOG.exception(_LE("Kafka Connection is not available: %s"), e)
            self.kafka_client = None

    def declare_topic_consumer(self, topics, group=None):
        self._ensure_connection()
        for topic in topics:
            self.kafka_client.ensure_topic_exists(topic)
        self.consumer = kafka.KafkaConsumer(
            *topics, group_id=group,
            bootstrap_servers=self.hostaddrs,
            fetch_message_max_bytes=self.fetch_messages_max_bytes)
        self._consume_loop_stopped = False


class OsloKafkaMessage(base.RpcIncomingMessage):

    def __init__(self, ctxt, message):
        super(OsloKafkaMessage, self).__init__(ctxt, message)

    def requeue(self):
        LOG.warning(_LW("requeue is not supported"))

    def reply(self, reply=None, failure=None):
        LOG.warning(_LW("reply is not supported"))


class KafkaListener(base.PollStyleListener):

    def __init__(self, conn):
        super(KafkaListener, self).__init__()
        self._stopped = threading.Event()
        self.conn = conn
        self.incoming_queue = []

    @base.batch_poll_helper
    def poll(self, timeout=None):
        while not self._stopped.is_set():
            if self.incoming_queue:
                return self.incoming_queue.pop(0)
            try:
                messages = self.conn.consume(timeout=timeout)
                for msg in messages:
                    message = msg.value
                    LOG.debug('poll got message : %s', message)
                    message = jsonutils.loads(message)
                    self.incoming_queue.append(OsloKafkaMessage(
                        ctxt=message['context'], message=message['message']))
            except driver_common.Timeout:
                return None

    def stop(self):
        self._stopped.set()
        self.conn.stop_consuming()

    def cleanup(self):
        self.conn.close()

    def commit(self):
        # TODO(Support for manually/auto commit functionality)
        # It's better to allow users to commit manually and support for
        # self.auto_commit = False option. For now, this commit function
        # is meaningless since user couldn't call this function and
        # auto_commit option is always True.
        self.conn.commit()


class KafkaDriver(base.BaseDriver):
    """Note: Current implementation of this driver is experimental.
    We will have functional and/or integrated testing enabled for this driver.
    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):

        opt_group = cfg.OptGroup(name='oslo_messaging_kafka',
                                 title='Kafka driver options')
        conf.register_group(opt_group)
        conf.register_opts(kafka_opts, group=opt_group)

        super(KafkaDriver, self).__init__(
            conf, url, default_exchange, allowed_remote_exmods)

        # the pool configuration properties
        max_size = self.conf.oslo_messaging_kafka.pool_size
        min_size = self.conf.oslo_messaging_kafka.conn_pool_min_size
        ttl = self.conf.oslo_messaging_kafka.conn_pool_ttl

        self.connection_pool = driver_pool.ConnectionPool(
            self.conf, max_size, min_size, ttl,
            self._url, Connection)
        self.listeners = []

    def cleanup(self):
        for c in self.listeners:
            c.close()
        self.listeners = []

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        raise NotImplementedError(
            'The RPC implementation for Kafka is not implemented')

    def send_notification(self, target, ctxt, message, version, retry=None):
        """Send notification to Kafka brokers

        :param target: Message destination target
        :type target: oslo_messaging.Target
        :param ctxt: Message context
        :type ctxt: dict
        :param message: Message payload to pass
        :type message: dict
        :param version: Messaging API version (currently not used)
        :type version: str
        :param retry: an optional default kafka consumer retries configuration
                      None means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        with self._get_connection(purpose=PURPOSE_SEND) as conn:
            conn.notify_send(target_to_topic(target), ctxt, message, retry)

    def listen(self, target, batch_size, batch_timeout):
        raise NotImplementedError(
            'The RPC implementation for Kafka is not implemented')

    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Listen to a specified list of targets on Kafka brokers

        :param targets_and_priorities: List of pairs (target, priority)
                                       priority is not used for kafka driver
                                       target.exchange_target.topic is used as
                                       a kafka topic
        :type targets_and_priorities: list
        :param pool: consumer group of Kafka consumers
        :type pool: string
        """
        conn = self._get_connection(purpose=PURPOSE_LISTEN)
        topics = set()
        for target, priority in targets_and_priorities:
            topics.add(target_to_topic(target, priority))

        conn.declare_topic_consumer(topics, pool)

        listener = KafkaListener(conn)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def _get_connection(self, purpose):
        return driver_common.ConnectionContext(self.connection_pool, purpose)
