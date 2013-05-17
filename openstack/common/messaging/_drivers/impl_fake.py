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
import time

from openstack.common.gettextutils import _
from openstack.common.messaging import exceptions
from openstack.common.messaging._drivers import base
from openstack.common.messaging import _utils as utils


class InvalidTarget(ValueError):

    def __init__(self, msg, target):
        self.msg = msg
        self.target = target

    def __str__(self):
        return self.msg + ":" + str(self.target)


class FakeIncomingMessage(base.IncomingMessage):

    def reply(self, reply=None, failure=None):
        self.listener._deliver_reply(reply, failure)

    def done(self):
        pass


class FakeListener(base.Listener):

    def __init__(self, driver, target):
        super(FakeListener, self).__init__(driver, target)
        self._queue = Queue.Queue()
        self._reply_queue = Queue.Queue()

    def _deliver_message(self, ctxt, message, wait_for_reply=None, timeout=None):
        self._queue.put((ctxt, message))
        if wait_for_reply:
            try:
                return self._reply_queue.get(timeout=timeout)
            except Queue.Empty:
                # FIXME(markmc): timeout exception
                return None

    def _deliver_reply(self, reply=None, failure=None):
        # FIXME: handle failure
        self._reply_queue.put(reply)

    def poll(self):
        while True:
            # sleeping allows keyboard interrupts
            try:
                (ctxt, message) = self._queue.get(block=False)
                return FakeIncomingMessage(self, ctxt, message)
            except Queue.Empty:
                time.sleep(.05)


class FakeDriver(base.BaseDriver):

    def __init__(self, conf, url=None, default_exchange=None):
        super(FakeDriver, self).__init__(conf, url, default_exchange)

        self._default_exchange = utils.exchange_from_url(url, default_exchange)

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

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        if not target.topic:
            raise InvalidTarget(_('A topic is required to send'), target)

        # FIXME(markmc): preconditions to enforce:
        #  - timeout and not wait_for_reply
        #  - target.fanout and (wait_for_reply or timeout)

        self._check_serialize(message)

        exchange = target.exchange or self._default_exchange

        start_time = time.time()
        while True:
            topics = self._exchanges.get(exchange, {})
            listeners = topics.get(target.topic, [])
            if target.server:
                listeners = [l for l in listeners if l.target.server == server]

            if listeners or not wait_for_reply:
                break

            if timeout and (time.time() - start_time > timeout):
                raise exceptions.MessagingTimeout(
                    _('No listeners found for topic %s') % target.topic)

            time.sleep(.05)

        if target.fanout:
            for listener in listeners:
                ret = listener._deliver_message(ctxt, message)
                if ret:
                    return ret
            return

        # FIXME(markmc): implement round-robin delivery
        listener = listeners[0]
        return listener._deliver_message(ctxt, message, wait_for_reply, timeout)

    def listen(self, target):
        if not (target.topic and target.server):
            raise InvalidTarget(_('Topic and server are required to listen'),
                                target)

        exchange = target.exchange or self._default_exchange
        topics = self._exchanges.setdefault(exchange, {})

        if target.topic in topics:
            raise InvalidTarget(_('Already listening on this topic'), target)

        listener = FakeListener(self, target)

        listeners = topics.setdefault(target.topic, [])
        listeners.append(listener)

        return listener
