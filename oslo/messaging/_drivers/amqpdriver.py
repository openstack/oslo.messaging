
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

from oslo import messaging
from oslo.messaging._drivers import amqp as rpc_amqp
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common as rpc_common

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
        with self.listener.driver._get_connection() as conn:
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

    def __call__(self, message):
        # FIXME(markmc): logging isn't driver specific
        rpc_common._safe_log(LOG.debug, 'received %s', dict(message))

        unique_id = self.msg_id_cache.check_duplicate_message(message)
        ctxt = rpc_amqp.unpack_context(self.conf, message)

        self.incoming.append(AMQPIncomingMessage(self,
                                                 ctxt.to_dict(),
                                                 message,
                                                 unique_id,
                                                 ctxt.msg_id,
                                                 ctxt.reply_q))

    def poll(self):
        while True:
            if self.incoming:
                return self.incoming.pop(0)
            self.conn.consume(limit=1)


class ReplyWaiters(object):

    WAKE_UP = object()

    def __init__(self):
        self._queues = {}
        self._wrn_threshold = 10

    def get(self, msg_id, timeout):
        try:
            return self._queues[msg_id].get(block=True, timeout=timeout)
        except moves.queue.Empty:
            raise messaging.MessagingTimeout('Timed out waiting for a reply '
                                             'to message ID %s' % msg_id)

    def check(self, msg_id):
        try:
            return self._queues[msg_id].get(block=False)
        except moves.queue.Empty:
            return None

    def put(self, msg_id, message_data):
        queue = self._queues.get(msg_id)
        if not queue:
            LOG.warn('No calling threads waiting for msg_id : %(msg_id)s'
                     ', message : %(data)s', {'msg_id': msg_id,
                                              'data': message_data})
            LOG.warn('_queues: %s' % str(self._queues))
        else:
            queue.put(message_data)

    def wake_all(self, except_id):
        msg_ids = [i for i in self._queues.keys() if i != except_id]
        for msg_id in msg_ids:
            self.put(msg_id, self.WAKE_UP)

    def add(self, msg_id, queue):
        self._queues[msg_id] = queue
        if len(self._queues) > self._wrn_threshold:
            LOG.warn('Number of call queues is greater than warning '
                     'threshold: %d. There could be a leak.' %
                     self._wrn_threshold)
            self._wrn_threshold *= 2

    def remove(self, msg_id):
        del self._queues[msg_id]


class ReplyWaiter(object):

    def __init__(self, conf, reply_q, conn, allowed_remote_exmods):
        self.conf = conf
        self.conn = conn
        self.reply_q = reply_q
        self.allowed_remote_exmods = allowed_remote_exmods

        self.conn_lock = threading.Lock()
        self.incoming = []
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.waiters = ReplyWaiters()

        conn.declare_direct_consumer(reply_q, self)

    def __call__(self, message):
        message.acknowledge()
        self.incoming.append(message)

    def listen(self, msg_id):
        queue = moves.queue.Queue()
        self.waiters.add(msg_id, queue)

    def unlisten(self, msg_id):
        self.waiters.remove(msg_id)

    def _process_reply(self, data):
        result = None
        ending = False
        self.msg_id_cache.check_duplicate_message(data)
        if data['failure']:
            failure = data['failure']
            result = rpc_common.deserialize_remote_exception(
                failure, self.allowed_remote_exmods)
        elif data.get('ending', False):
            ending = True
        else:
            result = data['result']
        return result, ending

    def _poll_connection(self, msg_id, timeout):
        while True:
            while self.incoming:
                message_data = self.incoming.pop(0)

                incoming_msg_id = message_data.pop('_msg_id', None)
                if incoming_msg_id == msg_id:
                    return self._process_reply(message_data)

                self.waiters.put(incoming_msg_id, message_data)

            try:
                self.conn.consume(limit=1, timeout=timeout)
            except rpc_common.Timeout:
                raise messaging.MessagingTimeout('Timed out waiting for a '
                                                 'reply to message ID %s'
                                                 % msg_id)

    def _poll_queue(self, msg_id, timeout):
        message = self.waiters.get(msg_id, timeout)
        if message is self.waiters.WAKE_UP:
            return None, None, True  # lock was released

        reply, ending = self._process_reply(message)
        return reply, ending, False

    def _check_queue(self, msg_id):
        while True:
            message = self.waiters.check(msg_id)
            if message is self.waiters.WAKE_UP:
                continue
            if message is None:
                return None, None, True  # queue is empty

            reply, ending = self._process_reply(message)
            return reply, ending, False

    def wait(self, msg_id, timeout):
        #
        # NOTE(markmc): we're waiting for a reply for msg_id to come in for on
        # the reply_q, but there may be other threads also waiting for replies
        # to other msg_ids
        #
        # Only one thread can be consuming from the queue using this connection
        # and we don't want to hold open a connection per thread, so instead we
        # have the first thread take responsibility for passing replies not
        # intended for itself to the appropriate thread.
        #
        final_reply = None
        while True:
            if self.conn_lock.acquire(False):
                # Ok, we're the thread responsible for polling the connection
                try:
                    # Check the queue to see if a previous lock-holding thread
                    # queued up a reply already
                    while True:
                        reply, ending, empty = self._check_queue(msg_id)
                        if empty:
                            break
                        if not ending:
                            final_reply = reply
                        else:
                            return final_reply

                    # Now actually poll the connection
                    while True:
                        reply, ending = self._poll_connection(msg_id, timeout)
                        if not ending:
                            final_reply = reply
                        else:
                            return final_reply
                finally:
                    self.conn_lock.release()
                    # We've got our reply, tell the other threads to wake up
                    # so that one of them will take over the responsibility for
                    # polling the connection
                    self.waiters.wake_all(msg_id)
            else:
                # We're going to wait for the first thread to pass us our reply
                reply, ending, trylock = self._poll_queue(msg_id, timeout)
                if trylock:
                    # The first thread got its reply, let's try and take over
                    # the responsibility for polling
                    continue
                if not ending:
                    final_reply = reply
                else:
                    return final_reply


class AMQPDriverBase(base.BaseDriver):

    def __init__(self, conf, url, connection_pool,
                 default_exchange=None, allowed_remote_exmods=[]):
        super(AMQPDriverBase, self).__init__(conf, url, default_exchange,
                                             allowed_remote_exmods)

        self._default_exchange = default_exchange

        # FIXME(markmc): temp hack
        if self._default_exchange:
            self.conf.set_override('control_exchange', self._default_exchange)

        self._connection_pool = connection_pool

        self._reply_q_lock = threading.Lock()
        self._reply_q = None
        self._reply_q_conn = None
        self._waiter = None

    def _get_connection(self, pooled=True):
        return rpc_amqp.ConnectionContext(self.conf,
                                          self._url,
                                          self._connection_pool,
                                          pooled=pooled)

    def _get_reply_q(self):
        with self._reply_q_lock:
            if self._reply_q is not None:
                return self._reply_q

            reply_q = 'reply_' + uuid.uuid4().hex

            conn = self._get_connection(pooled=False)

            self._waiter = ReplyWaiter(self.conf, reply_q, conn,
                                       self._allowed_remote_exmods)

            self._reply_q = reply_q
            self._reply_q_conn = conn

        return self._reply_q

    def _send(self, target, ctxt, message,
              wait_for_reply=None, timeout=None,
              envelope=True, notify=False):

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
            LOG.debug('MSG_ID is %s' % (msg_id))
            msg.update({'_reply_q': self._get_reply_q()})

        rpc_amqp._add_unique_id(msg)
        rpc_amqp.pack_context(msg, context)

        if envelope:
            msg = rpc_common.serialize_msg(msg)

        if wait_for_reply:
            self._waiter.listen(msg_id)

        try:
            with self._get_connection() as conn:
                if notify:
                    conn.notify_send(target.topic, msg)
                elif target.fanout:
                    conn.fanout_send(target.topic, msg)
                else:
                    topic = target.topic
                    if target.server:
                        topic = '%s.%s' % (target.topic, target.server)
                    conn.topic_send(topic, msg, timeout=timeout)

            if wait_for_reply:
                result = self._waiter.wait(msg_id, timeout)
                if isinstance(result, Exception):
                    raise result
                return result
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None):
        return self._send(target, ctxt, message, wait_for_reply, timeout)

    def send_notification(self, target, ctxt, message, version):
        return self._send(target, ctxt, message,
                          envelope=(version == 2.0), notify=True)

    def listen(self, target):
        conn = self._get_connection(pooled=False)

        listener = AMQPListener(self, conn)

        conn.declare_topic_consumer(target.topic, listener)
        conn.declare_topic_consumer('%s.%s' % (target.topic, target.server),
                                    listener)
        conn.declare_fanout_consumer(target.topic, listener)

        return listener

    def listen_for_notifications(self, targets_and_priorities):
        conn = self._get_connection(pooled=False)

        listener = AMQPListener(self, conn)
        for target, priority in targets_and_priorities:
            conn.declare_topic_consumer('%s.%s' % (target.topic, priority),
                                        callback=listener,
                                        exchange_name=target.exchange)
        return listener

    def cleanup(self):
        if self._connection_pool:
            self._connection_pool.empty()
        self._connection_pool = None
