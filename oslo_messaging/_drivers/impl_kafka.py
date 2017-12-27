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

# Following code fixes 2 issues with kafka-python and
# The current release of eventlet (0.19.0) does not actually remove
# select.poll [1]. Because of kafka-python.selectors34 selects
# PollSelector instead of SelectSelector [2]. PollSelector relies on
# select.poll, which does not work when eventlet/greenlet is used. This
# bug in evenlet is fixed in the master branch [3], but there's no
# release of eventlet that includes this fix at this point.

import json
import threading

import kafka
from kafka.client_async import selectors
import kafka.errors
from oslo_log import log as logging
from oslo_utils import eventletutils
import tenacity

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers.kafka_driver import kafka_options
from oslo_messaging._i18n import _LE
from oslo_messaging._i18n import _LW
from oslo_serialization import jsonutils

import logging as l
l.basicConfig(level=l.INFO)
l.getLogger("kafka").setLevel(l.WARN)
l.getLogger("stevedore").setLevel(l.WARN)

if eventletutils.is_monkey_patched('select'):
    # monkeypatch the vendored SelectSelector._select like eventlet does
    # https://github.com/eventlet/eventlet/blob/master/eventlet/green/selectors.py#L32
    from eventlet.green import select
    selectors.SelectSelector._select = staticmethod(select.select)

    # Force to use the select selectors
    KAFKA_SELECTOR = selectors.SelectSelector
else:
    KAFKA_SELECTOR = selectors.DefaultSelector

LOG = logging.getLogger(__name__)


def unpack_message(msg):
    context = {}
    message = None
    msg = json.loads(msg)
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


def retry_on_retriable_kafka_error(exc):
    return (isinstance(exc, kafka.errors.KafkaError) and exc.retriable)


def with_reconnect(retries=None):
    def decorator(func):
        @tenacity.retry(
            retry=tenacity.retry_if_exception(retry_on_retriable_kafka_error),
            wait=tenacity.wait_fixed(1),
            stop=tenacity.stop_after_attempt(retries),
            reraise=True
        )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


class Connection(object):

    def __init__(self, conf, url):

        self.conf = conf
        self.url = url
        self.virtual_host = url.virtual_host
        self._parse_url()

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

    def reset(self):
        """Reset a connection so it can be used again."""
        pass


class ConsumerConnection(Connection):

    def __init__(self, conf, url):

        super(ConsumerConnection, self).__init__(conf, url)
        driver_conf = self.conf.oslo_messaging_kafka
        self.consumer = None
        self.consumer_timeout = driver_conf.kafka_consumer_timeout
        self.max_fetch_bytes = driver_conf.kafka_max_fetch_bytes
        self.group_id = driver_conf.consumer_group
        self._consume_loop_stopped = False

    @with_reconnect()
    def _poll_messages(self, timeout):
        messages = self.consumer.poll(timeout * 1000.0)
        messages = [record.value
                    for records in messages.values() if records
                    for record in records]
        if not messages:
            # NOTE(sileht): really ? you return payload but no messages...
            # simulate timeout to consume message again
            raise kafka.errors.ConsumerTimeout()
        return messages

    def consume(self, timeout=None):
        """Receive up to 'max_fetch_messages' messages.

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
                return self._poll_messages(poll_timeout)
            except kafka.errors.ConsumerTimeout as exc:
                poll_timeout = timer.check_return(
                    _raise_timeout, exc, maximum=self.consumer_timeout)
            except Exception:
                LOG.exception(_LE("Failed to consume messages"))
                return

    def stop_consuming(self):
        self._consume_loop_stopped = True

    def close(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None

    @with_reconnect()
    def declare_topic_consumer(self, topics, group=None):
        # TODO(Support for manual/auto_commit functionality)
        # When auto_commit is False, consumer can manually notify
        # the completion of the subscription.
        # Currently we don't support for non auto commit option
        self.consumer = kafka.KafkaConsumer(
            *topics, group_id=(group or self.group_id),
            bootstrap_servers=self.hostaddrs,
            max_partition_fetch_bytes=self.max_fetch_bytes,
            selector=KAFKA_SELECTOR
        )


class ProducerConnection(Connection):

    def __init__(self, conf, url):

        super(ProducerConnection, self).__init__(conf, url)
        driver_conf = self.conf.oslo_messaging_kafka
        self.batch_size = driver_conf.producer_batch_size
        self.linger_ms = driver_conf.producer_batch_timeout * 1000
        self.producer = None
        self.producer_lock = threading.Lock()

    def notify_send(self, topic, ctxt, msg, retry):
        """Send messages to Kafka broker.

        :param topic: String of the topic
        :param ctxt: context for the messages
        :param msg: messages for publishing
        :param retry: the number of retry
        """
        retry = retry if retry >= 0 else None
        message = pack_message(ctxt, msg)
        message = jsonutils.dumps(message)

        @with_reconnect(retries=retry)
        def wrapped_with_reconnect():
            self._ensure_producer()
            # NOTE(sileht): This returns a future, we can use get()
            # if we want to block like other driver
            future = self.producer.send(topic, message)
            future.get()

        try:
            wrapped_with_reconnect()
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
                self.producer.close()
                self.producer = None

    def _ensure_producer(self):
        if self.producer:
            return
        with self.producer_lock:
            if self.producer:
                return
            self.producer = kafka.KafkaProducer(
                bootstrap_servers=self.hostaddrs,
                linger_ms=self.linger_ms,
                batch_size=self.batch_size,
                selector=KAFKA_SELECTOR)


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
    """Note: Current implementation of this driver is experimental.
    We will have functional and/or integrated testing enabled for this driver.
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
        topics = set()
        for target, priority in targets_and_priorities:
            topics.add(target_to_topic(target, priority))

        conn.declare_topic_consumer(topics, pool)

        listener = KafkaListener(conn)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)
