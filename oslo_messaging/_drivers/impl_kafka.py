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

import logging
import threading

import confluent_kafka
from confluent_kafka import KafkaException
from oslo_serialization import jsonutils
from oslo_utils import eventletutils
from oslo_utils import importutils

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers.kafka_driver import kafka_options

if eventletutils.EVENTLET_AVAILABLE:
    tpool = importutils.try_import('eventlet.tpool')

LOG = logging.getLogger(__name__)


def unpack_message(msg):
    """Unpack context and msg."""
    context = {}
    message = None
    msg = jsonutils.loads(msg)
    message = driver_common.deserialize_msg(msg)
    context = message['_context']
    del message['_context']
    return context, message


def pack_message(ctxt, msg):
    """Pack context into msg."""
    if isinstance(ctxt, dict):
        context_d = ctxt
    else:
        context_d = ctxt.to_dict()
    msg['_context'] = context_d

    msg = driver_common.serialize_msg(msg)

    return msg


def concat(sep, items):
    return sep.join(filter(bool, items))


def target_to_topic(target, priority=None, vhost=None):
    """Convert target into topic string

    :param target: Message destination target
    :type target: oslo_messaging.Target
    :param priority: Notification priority
    :type priority: string
    :param priority: Notification vhost
    :type priority: string
    """
    return concat(".", [target.topic, priority, vhost])


class ConsumerTimeout(KafkaException):
    pass


class AssignedPartition(object):
    """This class is used by the ConsumerConnection to track the
    assigned partitions.
    """
    def __init__(self, topic, partition):
        super(AssignedPartition, self).__init__()
        self.topic = topic
        self.partition = partition
        self.skey = '%s %d' % (self.topic, self.partition)

    def to_dict(self):
        return {'topic': self.topic, 'partition': self.partition}


class Connection(object):
    """This is the base class for consumer and producer connections for
    transport attributes.
    """

    def __init__(self, conf, url):

        self.driver_conf = conf.oslo_messaging_kafka
        self.security_protocol = self.driver_conf.security_protocol
        self.sasl_mechanism = self.driver_conf.sasl_mechanism
        self.ssl_cafile = self.driver_conf.ssl_cafile
        self.url = url
        self.virtual_host = url.virtual_host
        self._parse_url()

    def _parse_url(self):
        self.hostaddrs = []
        self.username = None
        self.password = None

        for host in self.url.hosts:
            # NOTE(ansmith): connections and failover are transparently
            # managed by the client library. Credentials will be
            # selectd from first host encountered in transport_url
            if self.username is None:
                self.username = host.username
                self.password = host.password
            else:
                if self.username != host.username:
                    LOG.warning("Different transport usernames detected")

            if host.hostname:
                self.hostaddrs.append("%s:%s" % (host.hostname, host.port))

    def reset(self):
        """Reset a connection so it can be used again."""
        pass


class ConsumerConnection(Connection):
    """This is the class for kafka topic/assigned partition consumer
    """
    def __init__(self, conf, url):

        super(ConsumerConnection, self).__init__(conf, url)
        self.consumer = None
        self.consumer_timeout = self.driver_conf.kafka_consumer_timeout
        self.max_fetch_bytes = self.driver_conf.kafka_max_fetch_bytes
        self.group_id = self.driver_conf.consumer_group
        self.use_auto_commit = self.driver_conf.enable_auto_commit
        self.max_poll_records = self.driver_conf.max_poll_records
        self._consume_loop_stopped = False
        self.assignment_dict = dict()

    def find_assignment(self, topic, partition):
        """Find and return existing assignment based on topic and partition"""
        skey = '%s %d' % (topic, partition)
        return self.assignment_dict.get(skey)

    def on_assign(self, consumer, topic_partitions):
        """Rebalance on_assign callback"""
        assignment = [AssignedPartition(p.topic, p.partition)
                      for p in topic_partitions]
        self.assignment_dict = {a.skey: a for a in assignment}
        for t in topic_partitions:
            LOG.debug("Topic %s assigned to partition %d",
                      t.topic, t.partition)

    def on_revoke(self, consumer, topic_partitions):
        """Rebalance on_revoke callback"""
        self.assignment_dict = dict()
        for t in topic_partitions:
            LOG.debug("Topic %s revoked from partition %d",
                      t.topic, t.partition)

    def _poll_messages(self, timeout):
        """Consume messages, callbacks and return list of messages"""
        msglist = self.consumer.consume(self.max_poll_records,
                                        timeout)

        if ((len(self.assignment_dict) == 0) or (len(msglist) == 0)):
            raise ConsumerTimeout()

        messages = []
        for message in msglist:
            if message is None:
                break
            a = self.find_assignment(message.topic(), message.partition())
            if a is None:
                LOG.warning(("Message for %s received on unassigned "
                             "partition %d"),
                            message.topic(), message.partition())
            else:
                messages.append(message.value())

        if not self.use_auto_commit:
            self.consumer.commit(asynchronous=False)

        return messages

    def consume(self, timeout=None):
        """Receive messages.

        :param timeout: poll timeout in seconds
        """

        def _raise_timeout(exc):
            raise driver_common.Timeout(str(exc))

        timer = driver_common.DecayingTimer(duration=timeout)
        timer.start()

        poll_timeout = (self.consumer_timeout if timeout is None
                        else min(timeout, self.consumer_timeout))

        while True:
            if self._consume_loop_stopped:
                return
            try:
                if eventletutils.is_monkey_patched('thread'):
                    return tpool.execute(self._poll_messages, poll_timeout)
                return self._poll_messages(poll_timeout)
            except ConsumerTimeout as exc:
                poll_timeout = timer.check_return(
                    _raise_timeout, exc, maximum=self.consumer_timeout)
            except Exception:
                LOG.exception("Failed to consume messages")
                return

    def stop_consuming(self):
        self._consume_loop_stopped = True

    def close(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None

    def declare_topic_consumer(self, topics, group=None):
        conf = {
            'bootstrap.servers': ",".join(self.hostaddrs),
            'group.id': (group or self.group_id),
            'enable.auto.commit': self.use_auto_commit,
            'max.partition.fetch.bytes': self.max_fetch_bytes,
            'security.protocol': self.security_protocol,
            'sasl.mechanism': self.sasl_mechanism,
            'sasl.username': self.username,
            'sasl.password': self.password,
            'ssl.ca.location': self.ssl_cafile,
            'enable.partition.eof': False,
            'default.topic.config': {'auto.offset.reset': 'latest'}
        }
        LOG.debug("Subscribing to %s as %s", topics, (group or self.group_id))
        self.consumer = confluent_kafka.Consumer(conf)
        self.consumer.subscribe(topics,
                                on_assign=self.on_assign,
                                on_revoke=self.on_revoke)


class ProducerConnection(Connection):

    def __init__(self, conf, url):

        super(ProducerConnection, self).__init__(conf, url)
        self.batch_size = self.driver_conf.producer_batch_size
        self.linger_ms = self.driver_conf.producer_batch_timeout * 1000
        self.producer = None
        self.producer_lock = threading.Lock()

    def _produce_message(self, topic, message):
        while True:
            try:
                self.producer.produce(topic, message)
            except KafkaException as e:
                LOG.error("Produce message failed: %s" % str(e))
            except BufferError:
                LOG.debug("Produce message queue full, waiting for deliveries")
                self.producer.poll(0.5)
                continue
            break

        self.producer.poll(0)

    def notify_send(self, topic, ctxt, msg, retry):
        """Send messages to Kafka broker.

        :param topic: String of the topic
        :param ctxt: context for the messages
        :param msg: messages for publishing
        :param retry: the number of retry
        """
        retry = retry if retry >= 0 else None
        message = pack_message(ctxt, msg)
        message = jsonutils.dumps(message).encode('utf-8')

        try:
            self._ensure_producer()
            if eventletutils.is_monkey_patched('thread'):
                return tpool.execute(self._produce_message, topic, message)
            return self._produce_message(topic, message)
        except Exception:
            # NOTE(sileht): if something goes wrong close the producer
            # connection
            self._close_producer()
            raise

    def close(self):
        self._close_producer()

    def _close_producer(self):
        with self.producer_lock:
            if self.producer:
                try:
                    self.producer.flush()
                except KafkaException:
                    LOG.error("Flush error during producer close")
                self.producer = None

    def _ensure_producer(self):
        if self.producer:
            return
        with self.producer_lock:
            if self.producer:
                return
            conf = {
                'bootstrap.servers': ",".join(self.hostaddrs),
                'linger.ms': self.linger_ms,
                'batch.num.messages': self.batch_size,
                'security.protocol': self.security_protocol,
                'sasl.mechanism': self.sasl_mechanism,
                'sasl.username': self.username,
                'sasl.password': self.password,
                'ssl.ca.location': self.ssl_cafile
            }
            self.producer = confluent_kafka.Producer(conf)


class OsloKafkaMessage(base.RpcIncomingMessage):

    def __init__(self, ctxt, message):
        super(OsloKafkaMessage, self).__init__(ctxt, message)

    def requeue(self):
        LOG.warning("requeue is not supported")

    def reply(self, reply=None, failure=None):
        LOG.warning("reply is not supported")

    def heartbeat(self):
        LOG.warning("heartbeat is not supported")


class KafkaListener(base.PollStyleListener):

    def __init__(self, conn):
        super(KafkaListener, self).__init__()
        self._stopped = threading.Event()
        self.conn = conn
        self.incoming_queue = []

        # FIXME(sileht): We do a first poll to ensure we topics are created
        # This is a workaround mainly for functional tests, in real life
        # this is fine if topics are not created synchroneously
        self.poll(5)

    @base.batch_poll_helper
    def poll(self, timeout=None):
        while not self._stopped.is_set():
            if self.incoming_queue:
                return self.incoming_queue.pop(0)
            try:
                messages = self.conn.consume(timeout=timeout) or []
                for message in messages:
                    msg = OsloKafkaMessage(*unpack_message(message))
                    self.incoming_queue.append(msg)
            except driver_common.Timeout:
                return None

    def stop(self):
        self._stopped.set()
        self.conn.stop_consuming()

    def cleanup(self):
        self.conn.close()


class KafkaDriver(base.BaseDriver):
    """Kafka Driver

    See :doc:`kafka` for details.
    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        conf = kafka_options.register_opts(conf, url)
        super(KafkaDriver, self).__init__(
            conf, url, default_exchange, allowed_remote_exmods)

        self.listeners = []
        self.virtual_host = url.virtual_host
        self.pconn = ProducerConnection(conf, url)

    def cleanup(self):
        self.pconn.close()
        for c in self.listeners:
            c.close()
        self.listeners = []
        LOG.info("Kafka messaging driver shutdown")

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             call_monitor_timeout=None, retry=None):
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
        :param call_monitor_timeout: Maximum time the client will wait for the
            call to complete before or receive a message heartbeat indicating
            the remote side is still executing.
        :type call_monitor_timeout: float
        :param retry: an optional default kafka consumer retries configuration
                      None means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        self.pconn.notify_send(target_to_topic(target,
                                               vhost=self.virtual_host),
                               ctxt, message, retry)

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
        conn = ConsumerConnection(self.conf, self._url)
        topics = []
        for target, priority in targets_and_priorities:
            topics.append(target_to_topic(target, priority))

        conn.declare_topic_consumer(topics, pool)

        listener = KafkaListener(conn)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)
