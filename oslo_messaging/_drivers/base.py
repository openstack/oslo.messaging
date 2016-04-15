
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

import abc
import threading

from oslo_config import cfg
from oslo_utils import excutils
from oslo_utils import timeutils
import six


from oslo_messaging import exceptions

base_opts = [
    cfg.IntOpt('rpc_conn_pool_size',
               default=30,
               deprecated_group='DEFAULT',
               help='Size of RPC connection pool.'),
]


def batch_poll_helper(func):
    """Decorator to poll messages in batch

    This decorator helps driver that polls message one by one,
    to returns a list of message.
    """
    def wrapper(in_self, timeout=None, batch_size=1, batch_timeout=None):
        incomings = []
        driver_prefetch = in_self.prefetch_size
        if driver_prefetch > 0:
            batch_size = min(batch_size, driver_prefetch)

        with timeutils.StopWatch(timeout) as timeout_watch:
            # poll first message
            msg = func(in_self, timeout=timeout_watch.leftover(True))
            if msg is not None:
                incomings.append(msg)
            if batch_size == 1 or msg is None:
                return incomings

            # update batch_timeout according to timeout for whole operation
            timeout_left = timeout_watch.leftover(True)
            if timeout_left is not None and (
                    batch_timeout is None or timeout_left < batch_timeout):
                batch_timeout = timeout_left

        with timeutils.StopWatch(batch_timeout) as batch_timeout_watch:
            # poll remained batch messages
            while len(incomings) < batch_size and msg is not None:
                msg = func(in_self, timeout=batch_timeout_watch.leftover(True))
                if msg is not None:
                    incomings.append(msg)

        return incomings
    return wrapper


class TransportDriverError(exceptions.MessagingException):
    """Base class for transport driver specific exceptions."""


@six.add_metaclass(abc.ABCMeta)
class IncomingMessage(object):

    def __init__(self, ctxt, message):
        self.ctxt = ctxt
        self.message = message

    def acknowledge(self):
        """Acknowledge the message."""

    @abc.abstractmethod
    def requeue(self):
        """Requeue the message."""


@six.add_metaclass(abc.ABCMeta)
class RpcIncomingMessage(IncomingMessage):

    @abc.abstractmethod
    def reply(self, reply=None, failure=None, log_failure=True):
        """Send a reply or failure back to the client."""


@six.add_metaclass(abc.ABCMeta)
class PollStyleListener(object):
    def __init__(self, prefetch_size=-1):
        self.prefetch_size = prefetch_size

    @abc.abstractmethod
    def poll(self, timeout=None, batch_size=1, batch_timeout=None):
        """Blocking until 'batch_size' message is pending and return
        [IncomingMessage].
        Waits for first message. Then waits for next batch_size-1 messages
        during batch window defined by batch_timeout
        This method block current thread until message comes,  stop() is
        executed by another thread or timemout is elapsed.
        """

    def stop(self):
        """Stop listener.
        Stop the listener message polling
        """
        pass

    def cleanup(self):
        """Cleanup listener.
        Close connection (socket) used by listener if any.
        As this is listener specific method, overwrite it in to derived class
        if cleanup of listener required.
        """
        pass


@six.add_metaclass(abc.ABCMeta)
class Listener(object):
    def __init__(self, batch_size, batch_timeout,
                 prefetch_size=-1):
        """Init Listener

        :param batch_size: desired number of messages passed to
            single on_incoming_callback notification
        :param batch_timeout: defines how long should we wait for batch_size
            messages if we already have some messages waiting for processing
        :param prefetch_size: defines how many massages we want to prefetch
            from backend (depend on driver type) by single request
        """
        self.on_incoming_callback = None
        self.batch_timeout = batch_timeout
        self.prefetch_size = prefetch_size
        if prefetch_size > 0:
            batch_size = min(batch_size, prefetch_size)
        self.batch_size = batch_size

    def start(self, on_incoming_callback):
        """Start listener.
        Start the listener message polling

        :param on_incoming_callback:  callback function to be executed when
            listener received messages. Messages should be processed and
            acked/nacked by callback
        """
        self.on_incoming_callback = on_incoming_callback

    def stop(self):
        """Stop listener.
        Stop the listener message polling
        """
        self.on_incoming_callback = None

    @abc.abstractmethod
    def cleanup(self):
        """Cleanup listener.
        Close connection (socket) used by listener if any.
        As this is listener specific method, overwrite it in to derived class
        if cleanup of listener required.
        """


class PollStyleListenerAdapter(Listener):
    def __init__(self, poll_style_listener, batch_size, batch_timeout):
        super(PollStyleListenerAdapter, self).__init__(
            batch_size, batch_timeout, poll_style_listener.prefetch_size
        )
        self._poll_style_listener = poll_style_listener
        self._listen_thread = threading.Thread(target=self._runner)
        self._listen_thread.daemon = True
        self._started = False

    def start(self, on_incoming_callback):
        """Start listener.
        Start the listener message polling

        :param on_incoming_callback:  callback function to be executed when
            listener received messages. Messages should be processed and
            acked/nacked by callback
        """
        super(PollStyleListenerAdapter, self).start(on_incoming_callback)
        self._started = True
        self._listen_thread.start()

    @excutils.forever_retry_uncaught_exceptions
    def _runner(self):
        while self._started:
            incoming = self._poll_style_listener.poll(
                batch_size=self.batch_size, batch_timeout=self.batch_timeout)

            if incoming:
                self.on_incoming_callback(incoming)

        # listener is stopped but we need to process all already consumed
        # messages
        while True:
            incoming = self._poll_style_listener.poll(
                batch_size=self.batch_size, batch_timeout=self.batch_timeout)

            if not incoming:
                return
            self.on_incoming_callback(incoming)

    def stop(self):
        """Stop listener.
        Stop the listener message polling
        """
        self._started = False
        self._poll_style_listener.stop()
        self._listen_thread.join()
        super(PollStyleListenerAdapter, self).stop()

    def cleanup(self):
        """Cleanup listener.
        Close connection (socket) used by listener if any.
        As this is listener specific method, overwrite it in to derived class
        if cleanup of listener required.
        """
        self._poll_style_listener.cleanup()


@six.add_metaclass(abc.ABCMeta)
class BaseDriver(object):
    prefetch_size = 0

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=None):
        self.conf = conf
        self._url = url
        self._default_exchange = default_exchange
        self._allowed_remote_exmods = allowed_remote_exmods or []

    def require_features(self, requeue=False):
        if requeue:
            raise NotImplementedError('Message requeueing not supported by '
                                      'this transport driver')

    @abc.abstractmethod
    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""

    @abc.abstractmethod
    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""

    @abc.abstractmethod
    def listen(self, target, batch_size, batch_timeout):
        """Construct a Listener for the given target."""

    @abc.abstractmethod
    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Construct a notification Listener for the given list of
        tuple of (target, priority).
        """

    @abc.abstractmethod
    def cleanup(self):
        """Release all resources."""
