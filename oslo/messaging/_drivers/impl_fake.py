# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack Foundation
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

import json
import Queue
import threading
import time

from oslo import messaging
from oslo.messaging._drivers import base
from oslo.messaging import _urls as urls


class InvalidTarget(base.TransportDriverError, ValueError):

    def __init__(self, msg, target):
        msg = msg + ":" + str(target)
        super(InvalidTarget, self).__init__(msg)
        self.target = target


class FakeIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, ctxt, message, reply_q):
        super(FakeIncomingMessage, self).__init__(listener, ctxt, message)
        self._reply_q = reply_q

    def reply(self, reply=None, failure=None):
        # FIXME: handle failure
        if self._reply_q:
            self._reply_q.put(reply)

    def done(self):
        pass


class FakeListener(base.Listener):

    def __init__(self, driver, target, exchange):
        super(FakeListener, self).__init__(driver, target)
        self._exchange = exchange

    def poll(self):
        while True:
            (ctxt, message, reply_q) = self._exchange.poll(self.target)
            if message is not None:
                return FakeIncomingMessage(self, ctxt, message, reply_q)
            time.sleep(.05)


class FakeExchange(object):

    def __init__(self, name):
        self.name = name
        self._queues_lock = threading.Lock()
        self._topic_queues = {}
        self._server_queues = {}

    def _get_topic_queue(self, topic):
        return self._topic_queues.setdefault(topic, [])

    def _get_server_queue(self, topic, server):
        return self._server_queues.setdefault((topic, server), [])

    def deliver_message(self, topic, ctxt, message,
                        server=None, fanout=False, reply_q=None):
        with self._queues_lock:
            if fanout:
                queues = [q for t, q in self._server_queues.items()
                          if t[0] == topic]
            elif server is not None:
                queues = [self._get_server_queue(topic, server)]
            else:
                queues = [self._get_topic_queue(topic)]
            for queue in queues:
                queue.append((ctxt, message, reply_q))

    def poll(self, target):
        with self._queues_lock:
            queue = self._get_server_queue(target.topic, target.server)
            if not queue:
                queue = self._get_topic_queue(target.topic)
            return queue.pop(0) if queue else (None, None, None)


class FakeDriver(base.BaseDriver):

    def __init__(self, conf, url=None, default_exchange=None):
        super(FakeDriver, self).__init__(conf, url, default_exchange)

        self._default_exchange = urls.exchange_from_url(url, default_exchange)

        self._exchanges_lock = threading.Lock()
        self._exchanges = {}

    @staticmethod
    def _check_serialize(message):
        """Make sure a message intended for rpc can be serialized.

        We specifically want to use json, not our own jsonutils because
        jsonutils has some extra logic to automatically convert objects to
        primitive types so that they can be serialized.  We want to catch all
        cases where non-primitive types make it into this code and treat it as
        an error.
        """
        json.dumps(message)

    def _get_exchange(self, name):
        while self._exchanges_lock:
            return self._exchanges.setdefault(name, FakeExchange(name))

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        if not target.topic:
            raise InvalidTarget('A topic is required to send', target)

        # FIXME(markmc): preconditions to enforce:
        #  - timeout and not wait_for_reply
        #  - target.fanout and (wait_for_reply or timeout)

        self._check_serialize(message)

        exchange = self._get_exchange(target.exchange or
                                      self._default_exchange)

        reply_q = None
        if wait_for_reply:
            reply_q = Queue.Queue()

        exchange.deliver_message(target.topic, ctxt, message,
                                 server=target.server,
                                 fanout=target.fanout,
                                 reply_q=reply_q)

        if wait_for_reply:
            try:
                return reply_q.get(timeout=timeout)
            except Queue.Empty:
                raise messaging.MessagingTimeout(
                    'No reply on topic %s' % target.topic)

        return None

    def listen(self, target):
        if not (target.topic and target.server):
            raise InvalidTarget('Topic and server are required to listen',
                                target)

        exchange = self._get_exchange(target.exchange or
                                      self._default_exchange)

        return FakeListener(self, target, exchange)
