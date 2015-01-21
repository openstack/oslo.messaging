#    Copyright 2014, Red Hat, Inc.
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

"""
Driver for the 'amqp' transport.

This module provides a transport driver that speaks version 1.0 of the AMQP
messaging protocol.  The driver sends messages and creates subscriptions via
'tasks' that are performed on its behalf via the controller module.
"""

import logging
import os
import threading
import time

from oslo_serialization import jsonutils
import proton
from six import moves

import oslo_messaging
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common
from oslo_messaging._drivers.protocols.amqp import controller
from oslo_messaging import target as messaging_target


LOG = logging.getLogger(__name__)


class SendTask(controller.Task):
    """A task that sends a message to a target, and optionally allows for the
    calling thread to wait for a reply.
    """
    def __init__(self, target, request, reply_expected, deadline):
        super(SendTask, self).__init__()
        self._target = target
        self._request = request
        self._deadline = deadline
        if reply_expected:
            self._reply_queue = moves.queue.Queue()
        else:
            self._reply_queue = None

    def execute(self, controller):
        """Runs on eventloop thread - sends request."""
        if not self._deadline or self._deadline > time.time():
            controller.request(self._target, self._request, self._reply_queue)
        else:
            LOG.warn("Send request to %s aborted: TTL expired.", self._target)

    def get_reply(self, timeout):
        """Retrieve the reply."""
        if not self._reply_queue:
            return None
        try:
            return self._reply_queue.get(timeout=timeout)
        except moves.queue.Empty:
            raise oslo_messaging.MessagingTimeout(
                'Timed out waiting for a reply')


class ListenTask(controller.Task):
    """A task that creates a subscription to the given target.  Messages
    arriving from the target are given to the listener.
    """
    def __init__(self, target, listener, notifications=False):
        """Create a subscription to the target."""
        super(ListenTask, self).__init__()
        self._target = target
        self._listener = listener
        self._notifications = notifications

    def execute(self, controller):
        """Run on the eventloop thread - subscribes to target. Inbound messages
        are queued to the listener's incoming queue.
        """
        if self._notifications:
            controller.subscribe_notifications(self._target,
                                               self._listener.incoming)
        else:
            controller.subscribe(self._target, self._listener.incoming)


class ReplyTask(controller.Task):
    """A task that sends 'response' message to address."""
    def __init__(self, address, response, log_failure):
        super(ReplyTask, self).__init__()
        self._address = address
        self._response = response
        self._log_failure = log_failure

    def execute(self, controller):
        """Run on the eventloop thread - send the response message."""
        controller.response(self._address, self._response)


def marshal_response(reply=None, failure=None):
    # TODO(grs): do replies have a context?
    msg = proton.Message()
    if failure:
        failure = common.serialize_remote_exception(failure)
        data = {"failure": failure}
    else:
        data = {"response": reply}
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_response(message, allowed):
    # TODO(kgiusti) This may fail to unpack and raise an exception. Need to
    # communicate this to the caller!
    data = jsonutils.loads(message.body)
    failure = data.get('failure')
    if failure is not None:
        raise common.deserialize_remote_exception(failure, allowed)
    return data.get("response")


def marshal_request(request, context, envelope):
    msg = proton.Message()
    if envelope:
        request = common.serialize_msg(request)
    data = {
        "request": request,
        "context": context
    }
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_request(message):
    data = jsonutils.loads(message.body)
    return (data.get("request"), data.get("context"))


class ProtonIncomingMessage(base.IncomingMessage):
    def __init__(self, listener, ctxt, request, message):
        super(ProtonIncomingMessage, self).__init__(listener, ctxt, request)
        self._reply_to = message.reply_to
        self._correlation_id = message.id

    def reply(self, reply=None, failure=None, log_failure=True):
        """Schedule a ReplyTask to send the reply."""
        if self._reply_to:
            response = marshal_response(reply=reply, failure=failure)
            response.correlation_id = self._correlation_id
            LOG.debug("Replying to %s", self._correlation_id)
            task = ReplyTask(self._reply_to, response, log_failure)
            self.listener.driver._ctrl.add_task(task)
        else:
            LOG.debug("Ignoring reply as no reply address available")

    def acknowledge(self):
        pass

    def requeue(self):
        pass


class ProtonListener(base.Listener):
    def __init__(self, driver):
        super(ProtonListener, self).__init__(driver)
        self.incoming = moves.queue.Queue()

    def poll(self):
        message = self.incoming.get()
        request, ctxt = unmarshal_request(message)
        LOG.debug("Returning incoming message")
        return ProtonIncomingMessage(self, ctxt, request, message)


class ProtonDriver(base.BaseDriver):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        # TODO(kgiusti) Remove once driver fully stabilizes:
        LOG.warning("Support for the 'amqp' transport is EXPERIMENTAL.")
        if proton is None or hasattr(controller, "fake_controller"):
            raise NotImplementedError("Proton AMQP C libraries not installed")

        super(ProtonDriver, self).__init__(conf, url, default_exchange,
                                           allowed_remote_exmods)
        # TODO(grs): handle authentication etc
        self._hosts = url.hosts
        self._conf = conf
        self._default_exchange = default_exchange

        # lazy connection setup - don't create the controller until
        # after the first messaging request:
        self._ctrl = None
        self._pid = None
        self._lock = threading.Lock()

    def _ensure_connect_called(func):
        """Causes a new controller to be created when the messaging service is
        first used by the current process. It is safe to push tasks to it
        whether connected or not, but those tasks won't be processed until
        connection completes.
        """
        def wrap(self, *args, **kws):
            with self._lock:
                old_pid = self._pid
                self._pid = os.getpid()

            if old_pid != self._pid:
                if self._ctrl is not None:
                    LOG.warning("Process forked after connection established!")
                    self._ctrl.shutdown(wait=False)
                # Create a Controller that connects to the messaging service:
                self._ctrl = controller.Controller(self._hosts,
                                                   self._default_exchange,
                                                   self._conf)
                self._ctrl.connect()
            return func(self, *args, **kws)
        return wrap

    @_ensure_connect_called
    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False,
             retry=None):
        """Send a message to the given target."""
        # TODO(kgiusti) need to add support for retry
        if retry is not None:
            raise NotImplementedError('"retry" not implemented by'
                                      'this transport driver')

        request = marshal_request(message, ctxt, envelope)
        expire = 0
        if timeout:
            expire = time.time() + timeout  # when the caller times out
            # amqp uses millisecond time values, timeout is seconds
            request.ttl = int(timeout * 1000)
            request.expiry_time = int(expire * 1000)
        LOG.debug("Send to %s", target)
        task = SendTask(target, request, wait_for_reply, expire)
        self._ctrl.add_task(task)
        result = None
        if wait_for_reply:
            # the following can raise MessagingTimeout if no reply received:
            reply = task.get_reply(timeout)
            # TODO(kgiusti) how to handle failure to un-marshal?  Must log, and
            # determine best way to communicate this failure back up to the
            # caller
            result = unmarshal_response(reply, self._allowed_remote_exmods)
        LOG.debug("Send to %s returning", target)
        return result

    @_ensure_connect_called
    def send_notification(self, target, ctxt, message, version,
                          retry=None):
        """Send a notification message to the given target."""
        # TODO(kgiusti) need to add support for retry
        if retry is not None:
            raise NotImplementedError('"retry" not implemented by'
                                      'this transport driver')
        return self.send(target, ctxt, message, envelope=(version == 2.0))

    @_ensure_connect_called
    def listen(self, target):
        """Construct a Listener for the given target."""
        LOG.debug("Listen to %s", target)
        listener = ProtonListener(self)
        self._ctrl.add_task(ListenTask(target, listener))
        return listener

    @_ensure_connect_called
    def listen_for_notifications(self, targets_and_priorities, pool):
        LOG.debug("Listen for notifications %s", targets_and_priorities)
        if pool:
            raise NotImplementedError('"pool" not implemented by'
                                      'this transport driver')
        listener = ProtonListener(self)
        for target, priority in targets_and_priorities:
            topic = '%s.%s' % (target.topic, priority)
            t = messaging_target.Target(topic=topic)
            self._ctrl.add_task(ListenTask(t, listener, True))
        return listener

    def cleanup(self):
        """Release all resources."""
        if self._ctrl:
            self._ctrl.shutdown()
            self._ctrl = None
        LOG.info("AMQP 1.0 messaging driver shutdown")
