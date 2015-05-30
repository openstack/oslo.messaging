
# Copyright 2013 Red Hat, Inc.
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

__all__ = ['AMQPDriverBase']

import logging
import threading
import uuid

from six import moves

import oslo_messaging
from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._i18n import _
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)


class AMQPIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, ctxt, message, unique_id, msg_id, reply_q):
        super(AMQPIncomingMessage, self).__init__(listener, ctxt,
                                                  dict(message))

        self.unique_id = unique_id
        self.msg_id = msg_id
        self.reply_q = reply_q
        self.acknowledge_callback = message.acknowledge
        self.requeue_callback = message.requeue

    def _send_reply(self, conn, reply=None, failure=None,
                    ending=False, log_failure=True):
        if failure:
            failure = rpc_common.serialize_remote_exception(failure,
                                                            log_failure)

        msg = {'result': reply, 'failure': failure}
        if ending:
            msg['ending'] = True

        rpc_amqp._add_unique_id(msg)

        # If a reply_q exists, add the msg_id to the reply and pass the
        # reply_q to direct_send() to use it as the response queue.
        # Otherwise use the msg_id for backward compatibility.
        if self.reply_q:
            msg['_msg_id'] = self.msg_id
            conn.direct_send(self.reply_q, rpc_common.serialize_msg(msg))
        else:
            conn.direct_send(self.msg_id, rpc_common.serialize_msg(msg))

    def reply(self, reply=None, failure=None, log_failure=True):
        if not self.msg_id:
            # NOTE(Alexei_987) not sending reply, if msg_id is empty
            #    because reply should not be expected by caller side
            return
        with self.listener.driver._get_connection(
                rpc_amqp.PURPOSE_SEND) as conn:
            if self.listener.driver.send_single_reply:
                self._send_reply(conn, reply, failure, log_failure=log_failure,
                                 ending=True)
            else:
                self._send_reply(conn, reply, failure, log_failure=log_failure)
                self._send_reply(conn, ending=True)

    def acknowledge(self):
        self.listener.msg_id_cache.add(self.unique_id)
        self.acknowledge_callback()

    def requeue(self):
        # NOTE(sileht): In case of the connection is lost between receiving the
        # message and requeing it, this requeue call fail
        # but because the message is not acknowledged and not added to the
        # msg_id_cache, the message will be reconsumed, the only difference is
        # the message stay at the beginning of the queue instead of moving to
        # the end.
        self.requeue_callback()


class AMQPListener(base.Listener):

    def __init__(self, driver, conn):
        super(AMQPListener, self).__init__(driver)
        self.conn = conn
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.incoming = []
        self._stopped = threading.Event()

    def __call__(self, message):
        ctxt = rpc_amqp.unpack_context(self.conf, message)

        # FIXME(sileht): Don't log the message until strutils is more
        # efficient, (rpc_amqp.unpack_context already log the context)
        # LOG.debug(u'received: %s',
        #           strutils.mask_password(six.text_type(dict(message))))

        unique_id = self.msg_id_cache.check_duplicate_message(message)

        self.incoming.append(AMQPIncomingMessage(self,
                                                 ctxt.to_dict(),
                                                 message,
                                                 unique_id,
                                                 ctxt.msg_id,
                                                 ctxt.reply_q))

    def poll(self, timeout=None):
        while not self._stopped.is_set():
            if self.incoming:
                return self.incoming.pop(0)
            try:
                self.conn.consume(timeout=timeout)
            except rpc_common.Timeout:
                return None

    def stop(self):
        self._stopped.set()
        self.conn.stop_consuming()

    def cleanup(self):
        # Closes listener connection
        self.conn.close()


class ReplyWaiters(object):

    WAKE_UP = object()

    def __init__(self):
        self._queues = {}
        self._wrn_threshold = 10

    def get(self, msg_id, timeout):
        try:
            return self._queues[msg_id].get(block=True, timeout=timeout)
        except moves.queue.Empty:
            raise oslo_messaging.MessagingTimeout(
                'Timed out waiting for a reply '
                'to message ID %s' % msg_id)

    def put(self, msg_id, message_data):
        queue = self._queues.get(msg_id)
        if not queue:
            LOG.info(_LI('No calling threads waiting for msg_id : %s'), msg_id)
            LOG.debug(' queues: %(queues)s, message: %(message)s',
                      {'queues': len(self._queues), 'message': message_data})
        else:
            queue.put(message_data)

    def add(self, msg_id):
        self._queues[msg_id] = moves.queue.Queue()
        if len(self._queues) > self._wrn_threshold:
            LOG.warn('Number of call queues is greater than warning '
                     'threshold: %d. There could be a leak. Increasing'
                     ' threshold to: %d', self._wrn_threshold,
                     self._wrn_threshold * 2)
            self._wrn_threshold *= 2

    def remove(self, msg_id):
        del self._queues[msg_id]


class ReplyWaiter(object):
    def __init__(self, reply_q, conn, allowed_remote_exmods):
        self.conn = conn
        self.allowed_remote_exmods = allowed_remote_exmods
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.waiters = ReplyWaiters()

        self.conn.declare_direct_consumer(reply_q, self)

        self._thread_exit_event = threading.Event()
        self._thread = threading.Thread(target=self.poll)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        if self._thread:
            self._thread_exit_event.set()
            self.conn.stop_consuming()
            self._thread.join()
            self._thread = None

    def poll(self):
        while not self._thread_exit_event.is_set():
            try:
                self.conn.consume()
            except Exception:
                LOG.exception("Failed to process incoming message, "
                              "retrying...")

    def __call__(self, message):
        message.acknowledge()
        incoming_msg_id = message.pop('_msg_id', None)
        self.waiters.put(incoming_msg_id, message)

    def listen(self, msg_id):
        self.waiters.add(msg_id)

    def unlisten(self, msg_id):
        self.waiters.remove(msg_id)

    @staticmethod
    def _raise_timeout_exception(msg_id):
        raise oslo_messaging.MessagingTimeout(
            _('Timed out waiting for a reply to message ID %s.') % msg_id)

    def _process_reply(self, data):
        self.msg_id_cache.check_duplicate_message(data)
        if data['failure']:
            failure = data['failure']
            result = rpc_common.deserialize_remote_exception(
                failure, self.allowed_remote_exmods)
        else:
            result = data.get('result', None)

        ending = data.get('ending', False)
        return result, ending

    def wait(self, msg_id, timeout):
        # NOTE(sileht): for each msg_id we receive two amqp message
        # first one with the payload, a second one to ensure the other
        # have finish to send the payload
        # NOTE(viktors): We are going to remove this behavior in the N
        # release, but we need to keep backward compatibility, so we should
        # support both cases for now.
        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()
        final_reply = None
        ending = False
        while not ending:
            timeout = timer.check_return(self._raise_timeout_exception, msg_id)
            try:
                message = self.waiters.get(msg_id, timeout=timeout)
            except moves.queue.Empty:
                self._raise_timeout_exception(msg_id)

            reply, ending = self._process_reply(message)
            if reply is not None:
                # NOTE(viktors): This can be either first _send_reply() with an
                # empty `result` field or a second _send_reply() with
                # ending=True and no `result` field.
                final_reply = reply
        return final_reply


class AMQPDriverBase(base.BaseDriver):

    def __init__(self, conf, url, connection_pool,
                 default_exchange=None, allowed_remote_exmods=None,
                 send_single_reply=False):
        super(AMQPDriverBase, self).__init__(conf, url, default_exchange,
                                             allowed_remote_exmods)

        self._default_exchange = default_exchange

        self._connection_pool = connection_pool

        self._reply_q_lock = threading.Lock()
        self._reply_q = None
        self._reply_q_conn = None
        self._waiter = None

        self.send_single_reply = send_single_reply

    def _get_exchange(self, target):
        return target.exchange or self._default_exchange

    def _get_connection(self, purpose=rpc_amqp.PURPOSE_SEND):
        return rpc_amqp.ConnectionContext(self._connection_pool,
                                          purpose=purpose)

    def _get_reply_q(self):
        with self._reply_q_lock:
            if self._reply_q is not None:
                return self._reply_q

            reply_q = 'reply_' + uuid.uuid4().hex

            conn = self._get_connection(rpc_amqp.PURPOSE_LISTEN)

            self._waiter = ReplyWaiter(reply_q, conn,
                                       self._allowed_remote_exmods)

            self._reply_q = reply_q
            self._reply_q_conn = conn

        return self._reply_q

    def _send(self, target, ctxt, message,
              wait_for_reply=None, timeout=None,
              envelope=True, notify=False, retry=None):

        # FIXME(markmc): remove this temporary hack
        class Context(object):
            def __init__(self, d):
                self.d = d

            def to_dict(self):
                return self.d

        context = Context(ctxt)
        msg = message

        if wait_for_reply:
            msg_id = uuid.uuid4().hex
            msg.update({'_msg_id': msg_id})
            LOG.debug('MSG_ID is %s', msg_id)
            msg.update({'_reply_q': self._get_reply_q()})

        rpc_amqp._add_unique_id(msg)
        rpc_amqp.pack_context(msg, context)

        if envelope:
            msg = rpc_common.serialize_msg(msg)

        if wait_for_reply:
            self._waiter.listen(msg_id)

        try:
            with self._get_connection(rpc_amqp.PURPOSE_SEND) as conn:
                if notify:
                    conn.notify_send(self._get_exchange(target),
                                     target.topic, msg, retry=retry)
                elif target.fanout:
                    conn.fanout_send(target.topic, msg, retry=retry)
                else:
                    topic = target.topic
                    if target.server:
                        topic = '%s.%s' % (target.topic, target.server)
                    conn.topic_send(exchange_name=self._get_exchange(target),
                                    topic=topic, msg=msg, timeout=timeout,
                                    retry=retry)

            if wait_for_reply:
                result = self._waiter.wait(msg_id, timeout)
                if isinstance(result, Exception):
                    raise result
                return result
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        return self._send(target, ctxt, message, wait_for_reply, timeout,
                          retry=retry)

    def send_notification(self, target, ctxt, message, version, retry=None):
        return self._send(target, ctxt, message,
                          envelope=(version == 2.0), notify=True, retry=retry)

    def listen(self, target):
        conn = self._get_connection(rpc_amqp.PURPOSE_LISTEN)

        listener = AMQPListener(self, conn)

        conn.declare_topic_consumer(exchange_name=self._get_exchange(target),
                                    topic=target.topic,
                                    callback=listener)
        conn.declare_topic_consumer(exchange_name=self._get_exchange(target),
                                    topic='%s.%s' % (target.topic,
                                                     target.server),
                                    callback=listener)
        conn.declare_fanout_consumer(target.topic, listener)

        return listener

    def listen_for_notifications(self, targets_and_priorities, pool):
        conn = self._get_connection(rpc_amqp.PURPOSE_LISTEN)

        listener = AMQPListener(self, conn)
        for target, priority in targets_and_priorities:
            conn.declare_topic_consumer(
                exchange_name=self._get_exchange(target),
                topic='%s.%s' % (target.topic, priority),
                callback=listener, queue_name=pool)
        return listener

    def cleanup(self):
        if self._connection_pool:
            self._connection_pool.empty()
        self._connection_pool = None

        with self._reply_q_lock:
            if self._reply_q is not None:
                self._waiter.stop()
                self._reply_q_conn.close()
                self._reply_q_conn = None
                self._reply_q = None
                self._waiter = None
