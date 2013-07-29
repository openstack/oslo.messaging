
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
import Queue
import threading
import uuid

from oslo.messaging._drivers import amqp as rpc_amqp
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common as rpc_common
from oslo.messaging import _urls as urls

LOG = logging.getLogger(__name__)


class AMQPIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, ctxt, message, msg_id, reply_q):
        super(AMQPIncomingMessage, self).__init__(listener, ctxt, message)

        self.msg_id = msg_id
        self.reply_q = reply_q

    def _send_reply(self, conn, reply=None, failure=None, ending=False):
        # FIXME(markmc): is the reply format really driver specific?
        msg = {'result': reply, 'failure': failure}

        # FIXME(markmc): given that we're not supporting multicall ...
        if ending:
            msg['ending'] = True

        rpc_amqp._add_unique_id(msg)

        # If a reply_q exists, add the msg_id to the reply and pass the
        # reply_q to direct_send() to use it as the response queue.
        # Otherwise use the msg_id for backward compatibilty.
        if self.reply_q:
            msg['_msg_id'] = self.msg_id
            conn.direct_send(self.reply_q, rpc_common.serialize_msg(msg))
        else:
            conn.direct_send(self.msg_id, rpc_common.serialize_msg(msg))

    def reply(self, reply=None, failure=None):
        LOG.info("reply")
        with self.listener.driver._get_connection() as conn:
            self._send_reply(conn, reply, failure)
            self._send_reply(conn, ending=True)

    def done(self):
        LOG.info("done")
        # FIXME(markmc): I'm not sure we need this method ... we've already
        # acked the message at this point


class AMQPListener(base.Listener):

    def __init__(self, driver, target, conn):
        super(AMQPListener, self).__init__(driver, target)
        self.conn = conn
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.incoming = []

    def __call__(self, message):
        # FIXME(markmc): del local.store.context

        # FIXME(markmc): logging isn't driver specific
        rpc_common._safe_log(LOG.debug, 'received %s', message)

        self.msg_id_cache.check_duplicate_message(message)
        ctxt = rpc_amqp.unpack_context(self.conf, message)

        self.incoming.append(AMQPIncomingMessage(self,
                                                 ctxt.to_dict(),
                                                 message,
                                                 ctxt.msg_id,
                                                 ctxt.reply_q))

    def poll(self):
        while True:
            if self.incoming:
                return self.incoming.pop(0)

            # FIXME(markmc): timeout?
            self.conn.consume(limit=1)


class ReplyWaiters(object):

    def __init__(self):
        self._queues = {}
        self._wrn_threshhold = 10

    def get(self, msg_id):
        return self._queues.get(msg_id)

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
        for msg_id in self._queues:
            if msg_id == except_id:
                continue
            self.put(msg_id, None)

    def add(self, msg_id, queue):
        self._queues[msg_id] = queue
        if len(self._queues) > self._wrn_threshhold:
            LOG.warn('Number of call queues is greater than warning '
                     'threshhold: %d. There could be a leak.' %
                     self._wrn_threshhold)
            self._wrn_threshhold *= 2

    def remove(self, msg_id):
        del self._queues[msg_id]


class ReplyWaiter(object):

    def __init__(self, conf, reply_q, conn):
        self.conf = conf
        self.conn = conn
        self.reply_q = reply_q

        self.conn_lock = threading.Lock()
        self.incoming = []
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.waiters = ReplyWaiters()

        conn.declare_direct_consumer(reply_q, self)

    def __call__(self, message):
        self.incoming.append(message)

    def listen(self, msg_id):
        queue = Queue.Queue()
        self.waiters.add(msg_id, queue)

    def unlisten(self, msg_id):
        self.waiters.remove(msg_id)

    def _process_reply(self, data):
        result = None
        ending = False
        self.msg_id_cache.check_duplicate_message(data)
        if data['failure']:
            failure = data['failure']
            result = rpc_common.deserialize_remote_exception(self.conf,
                                                             failure)
        elif data.get('ending', False):
            ending = True
        else:
            result = data['result']
        return result, ending

    def _poll_connection(self, msg_id):
        while True:
            while self.incoming:
                message_data = self.incoming.pop(0)
                if message_data.pop('_msg_id', None) == msg_id:
                    return self._process_reply(message_data)

                self.waiters.put(msg_id, message_data)

            # FIXME(markmc): timeout?
            self.conn.consume(limit=1)

    def _poll_queue(self, msg_id):
        while True:
            # FIXME(markmc): timeout?
            message = self.waiters.get(msg_id)
            if message is None:
                return None, None, True  # lock was released

            reply, ending = self._process_reply(message)
            return reply, ending, False

    def wait(self, msg_id):
        # NOTE(markmc): multiple threads may call this
        # First thread calls consume, when it gets its reply
        # it wakes up other threads and they call consume
        # If a thread gets a message destined for another
        # thread, it wakes up the other thread
        final_reply = None
        while True:
            if self.conn_lock.acquire(blocking=False):
                try:
                    reply, ending = self._poll_connection(msg_id)
                    if reply:
                        final_reply = reply
                    elif ending:
                        return final_reply
                finally:
                    self.conn_lock.release()
                    self.waiters.wake_all(msg_id)
            else:
                reply, ending, trylock = self._poll_queue(msg_id)
                if trylock:
                    continue
                if reply:
                    final_reply = reply
                elif ending:
                    return final_reply


class AMQPDriverBase(base.BaseDriver):

    def __init__(self, conf, connection_pool, url=None, default_exchange=None):
        super(AMQPDriverBase, self).__init__(conf, url, default_exchange)

        self._default_exchange = urls.exchange_from_url(url, default_exchange)

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
                                          self._connection_pool,
                                          pooled=pooled)

    def _get_reply_q(self):
        with self._reply_q_lock:
            if self._reply_q is not None:
                return self._reply_q

            reply_q = 'reply_' + uuid.uuid4().hex

            conn = self._get_connection(pooled=False)

            self._waiter = ReplyWaiter(self.conf, reply_q, conn)

            self._reply_q = reply_q
            self._reply_q_conn = conn

        return self._reply_q

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):

        # FIXME(markmc): remove this temporary hack
        class Context(object):
            def __init__(self, d):
                self.d = d

            def to_dict(self):
                return self.d

        context = Context(ctxt)
        msg = message

        msg_id = uuid.uuid4().hex
        msg.update({'_msg_id': msg_id})
        LOG.debug('MSG_ID is %s' % (msg_id))
        rpc_amqp._add_unique_id(msg)
        rpc_amqp.pack_context(msg, context)

        msg.update({'_reply_q': self._get_reply_q()})

        # FIXME(markmc): handle envelope param
        msg = rpc_common.serialize_msg(msg)

        if wait_for_reply:
            self._waiter.listen(msg_id)

        try:
            with self._get_connection() as conn:
                # FIXME(markmc): check that target.topic is set
                if target.fanout:
                    conn.fanout_send(target.topic, msg)
                else:
                    topic = target.topic
                    if target.server:
                        topic = '%s.%s' % (target.topic, target.server)
                    conn.topic_send(topic, msg, timeout=timeout)

            if wait_for_reply:
                # FIXME(markmc): timeout?
                return self._waiter.wait(msg_id)
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def listen(self, target):
        # FIXME(markmc): check that topic.target and topic.server is set

        conn = self._get_connection(pooled=False)

        listener = AMQPListener(self, target, conn)

        conn.declare_topic_consumer(target.topic, listener)
        conn.declare_topic_consumer('%s.%s' % (target.topic, target.server),
                                    listener)
        conn.declare_fanout_consumer(target.topic, listener)

        return listener
