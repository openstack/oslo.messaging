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

import logging
import threading
import time

from oslo_messaging._drivers.protocols.amqp import controller
from oslo_messaging import exceptions

from six import moves

LOG = logging.getLogger(__name__)


class SendTask(controller.Task):
    """A task that sends a message to a target, and optionally waits for a
    reply message.  The caller may block until the remote confirms receipt or
    the reply message has arrived.
    """
    def __init__(self, target, request, wait_for_reply, deadline):
        super(SendTask, self).__init__()
        self._target = target
        self._request = request
        self._deadline = deadline
        self._wait_for_reply = wait_for_reply
        self._results_queue = moves.queue.Queue()

    def wait(self, timeout):
        """Wait for the send to complete, and, optionally, a reply message from
        the remote.  Will raise MessagingTimeout if the send does not complete
        or no reply is received within timeout seconds. If the request has
        failed for any other reason, a MessagingException is raised."
        """
        try:
            result = self._results_queue.get(timeout=timeout)
        except moves.queue.Empty:
            if self._wait_for_reply:
                reason = "Timed out waiting for a reply."
            else:
                reason = "Timed out waiting for send to complete."
            raise exceptions.MessagingTimeout(reason)
        if result["status"] == "OK":
            return result.get("response", None)
        raise result["error"]

    def execute(self, controller):
        """Runs on eventloop thread - sends request."""
        if not self._deadline or self._deadline > time.time():
            controller.request(self._target, self._request,
                               self._results_queue, self._wait_for_reply)
        else:
            LOG.warn("Send request to %s aborted: TTL expired.", self._target)


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
    """A task that sends 'response' message to 'address'.
    """
    def __init__(self, address, response, log_failure):
        super(ReplyTask, self).__init__()
        self._address = address
        self._response = response
        self._log_failure = log_failure
        self._wakeup = threading.Event()

    def wait(self):
        """Wait for the controller to send the message.
        """
        self._wakeup.wait()

    def execute(self, controller):
        """Run on the eventloop thread - send the response message."""
        controller.response(self._address, self._response)
        self._wakeup.set()
