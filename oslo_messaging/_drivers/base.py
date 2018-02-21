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
    cfg.IntOpt('rpc_conn_pool_size', default=30,
               deprecated_group='DEFAULT',
               help='Size of RPC connection pool.'),
    cfg.IntOpt('conn_pool_min_size', default=2,
               help='The pool size limit for connections expiration policy'),
    cfg.IntOpt('conn_pool_ttl', default=1200,
               help='The time-to-live in sec of idle connections in the pool')
]


def batch_poll_helper(func):
    """Decorator to poll messages in batch

    This decorator is used to add message batching support to a
    :py:meth:`PollStyleListener.poll` implementation that only polls for a
    single message per call.
    """
    def wrapper(in_self, timeout=None, batch_size=1, batch_timeout=None):
        incomings = []
        driver_prefetch = in_self.prefetch_size
        if driver_prefetch > 0:
            batch_size = min(batch_size, driver_prefetch)
        timeout = batch_timeout or timeout

        with timeutils.StopWatch(timeout) as watch:
            while True:
                message = func(in_self, timeout=watch.leftover(True))
                if message is not None:
                    incomings.append(message)
                if len(incomings) == batch_size or message is None:
                    break

        return incomings
    return wrapper


class TransportDriverError(exceptions.MessagingException):
    """Base class for transport driver specific exceptions."""


@six.add_metaclass(abc.ABCMeta)
class IncomingMessage(object):
    """The IncomingMessage class represents a single message received from the
    messaging backend. Instances of this class are passed to up a server's
    messaging processing logic. The backend driver must provide a concrete
    derivation of this class which provides the backend specific logic for its
    public methods.

    :param ctxt: Context metadata provided by sending application.
    :type ctxt: dict
    :param message: The message as provided by the sending application.
    :type message: dict
    """

    def __init__(self, ctxt, message):
        self.ctxt = ctxt
        self.message = message
        self.client_timeout = None

    def acknowledge(self):
        """Called by the server to acknowledge receipt of the message. When
        this is called the driver must notify the backend of the
        acknowledgment. This call should block at least until the driver has
        processed the acknowledgment request locally. It may unblock before the
        acknowledgment state has been acted upon by the backend.

        If the acknowledge operation fails this method must issue a log message
        describing the reason for the failure.

        :raises: Does not raise an exception
        """

    @abc.abstractmethod
    def requeue(self):
        """Called by the server to return the message to the backend so it may
        be made available for consumption by another server.  This call should
        block at least until the driver has processed the requeue request
        locally. It may unblock before the backend makes the requeued message
        available for consumption.

        If the requeue operation fails this method must issue a log message
        describing the reason for the failure.

        Support for this method is _optional_.  The
        :py:meth:`BaseDriver.require_features` method should indicate whether
        or not support for requeue is available.

        :raises: Does not raise an exception
        """


@six.add_metaclass(abc.ABCMeta)
class RpcIncomingMessage(IncomingMessage):
    """The RpcIncomingMessage represents an RPC request message received from
    the backend. This class must be used for RPC calls that return a value to
    the caller.
    """

    @abc.abstractmethod
    def reply(self, reply=None, failure=None):
        """Called by the server to send an RPC reply message or an exception
        back to the calling client.

        If an exception is passed via *failure* the driver must convert it to
        a form that can be sent as a message and properly converted back to the
        exception at the remote.

        The driver must provide a way to determine the destination address for
        the reply. For example the driver may use the *reply-to* field from the
        corresponding incoming message. Often a driver will also need to set a
        correlation identifier in the reply to help the remote route the reply
        to the correct RPCClient.

        The driver should provide an *at-most-once* delivery guarantee for
        reply messages. This call should block at least until the reply message
        has been handed off to the backend - there is no need to confirm that
        the reply has been delivered.

        If the reply operation fails this method must issue a log message
        describing the reason for the failure.

        See :py:meth:`BaseDriver.send` for details regarding how the received
        reply is processed.

        :param reply: reply message body
        :type reply: dict
        :param failure: an exception thrown by the RPC call
        :type failure: Exception
        :raises: Does not raise an exception
        """

    @abc.abstractmethod
    def heartbeat(self):
        """Called by the server to send an RPC heartbeat message back to
        the calling client.

        If the client (is new enough to have) passed its timeout value during
        the RPC call, this method will be called periodically by the server
        to update the client's timeout timer while a long-running call is
        executing.

        :raises: Does not raise an exception
        """


@six.add_metaclass(abc.ABCMeta)
class PollStyleListener(object):
    """A PollStyleListener is used to transfer received messages to a server
    for processing. A polling pattern is used to retrieve messages.  A
    PollStyleListener uses a separate thread to run the polling loop.  A
    :py:class:`PollStyleListenerAdapter` can be used to create a
    :py:class:`Listener` from a PollStyleListener.

    :param prefetch_size: The number of messages that should be pulled from the
        backend per receive transaction. May not be honored by all backend
        implementations.
    :type prefetch_size: int
    """

    def __init__(self, prefetch_size=-1):
        self.prefetch_size = prefetch_size

    @abc.abstractmethod
    def poll(self, timeout=None, batch_size=1, batch_timeout=None):
        """poll is called by the server to retrieve incoming messages. It
        blocks until 'batch_size' incoming messages are available, a timeout
        occurs, or the poll is interrupted by a call to the :py:meth:`stop`
        method.

        If 'batch_size' is > 1 poll must block until 'batch_size' messages are
        available or at least one message is available and batch_timeout
        expires

        :param timeout: Block up to 'timeout' seconds waiting for a message
        :type timeout: float
        :param batch_size: Block until this number of messages are received.
        :type batch_size: int
        :param batch_timeout: Time to wait in seconds for a full batch to
            arrive. A timer is started when the first message in a batch is
            received. If a full batch's worth of messages is not received when
            the timer expires then :py:meth:`poll` returns all messages
            received thus far.
        :type batch_timeout: float
        :raises: Does not raise an exception.
        :return: A list of up to batch_size IncomingMessage objects.
        """

    def stop(self):
        """Stop the listener from polling for messages. This method must cause
        the :py:meth:`poll` call to unblock and return whatever messages are
        currently available.  This method is called from a different thread
        than the poller so it must be thread-safe.
        """
        pass

    def cleanup(self):
        """Cleanup all resources held by the listener. This method should block
        until the cleanup is completed.
        """
        pass


@six.add_metaclass(abc.ABCMeta)
class Listener(object):
    """A Listener is used to transfer incoming messages from the driver to a
    server for processing.  A callback is used by the driver to transfer the
    messages.

    :param batch_size: desired number of messages passed to
        single on_incoming_callback notification
    :type batch_size: int
    :param batch_timeout: defines how long should we wait in seconds for
        batch_size messages if we already have some messages waiting for
        processing
    :type batch_timeout: float
    :param prefetch_size: defines how many messages we want to prefetch
        from the messaging backend in a single request. May not be honored by
        all backend implementations.
    :type prefetch_size: int
    """
    def __init__(self, batch_size, batch_timeout,
                 prefetch_size=-1):

        self.on_incoming_callback = None
        self.batch_timeout = batch_timeout
        self.prefetch_size = prefetch_size
        if prefetch_size > 0:
            batch_size = min(batch_size, prefetch_size)
        self.batch_size = batch_size

    def start(self, on_incoming_callback):
        """Start receiving messages. This should cause the driver to start
        receiving messages from the backend. When message(s) arrive the driver
        must invoke 'on_incoming_callback' passing it the received messages as
        a list of IncomingMessages.

        :param on_incoming_callback: callback function to be executed when
            listener receives messages.
        :type on_incoming_callback: func
        """
        self.on_incoming_callback = on_incoming_callback

    def stop(self):
        """Stop receiving messages.  The driver must no longer invoke
        the callback.
        """
        self.on_incoming_callback = None

    @abc.abstractmethod
    def cleanup(self):
        """Cleanup all resources held by the listener. This method should block
        until the cleanup is completed.
        """


class PollStyleListenerAdapter(Listener):
    """A Listener that uses a PollStyleListener for message transfer. A
    dedicated thread is created to do message polling.
    """
    def __init__(self, poll_style_listener, batch_size, batch_timeout):
        super(PollStyleListenerAdapter, self).__init__(
            batch_size, batch_timeout, poll_style_listener.prefetch_size
        )
        self._poll_style_listener = poll_style_listener
        self._listen_thread = threading.Thread(target=self._runner)
        self._listen_thread.daemon = True
        self._started = False

    def start(self, on_incoming_callback):
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
        self._started = False
        self._poll_style_listener.stop()
        self._listen_thread.join()
        super(PollStyleListenerAdapter, self).stop()

    def cleanup(self):
        self._poll_style_listener.cleanup()


@six.add_metaclass(abc.ABCMeta)
class BaseDriver(object):
    """Defines the backend driver interface. Each backend driver implementation
    must provide a concrete derivation of this class implementing the backend
    specific logic for its public methods.

    :param conf: The configuration settings provided by the user.
    :type conf: ConfigOpts
    :param url: The network address of the messaging backend(s).
    :type url: TransportURL
    :param default_exchange: The exchange to use if no exchange is specified in
        a Target.
    :type default_exchange: str
    :param allowed_remote_exmods: whitelist of those exception modules which
        are permitted to be re-raised if an exception is returned in response
        to an RPC call.
    :type allowed_remote_exmods: list
    """
    prefetch_size = 0

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=None):
        self.conf = conf
        self._url = url
        self._default_exchange = default_exchange
        self._allowed_remote_exmods = allowed_remote_exmods or []

    def require_features(self, requeue=False):
        """The driver must raise a 'NotImplementedError' if any of the feature
        flags passed as True are not supported.
        """
        if requeue:
            raise NotImplementedError('Message requeueing not supported by '
                                      'this transport driver')

    @abc.abstractmethod
    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, call_monitor_timeout=None,
             retry=None):
        """Send a message to the given target and optionally wait for a reply.
        This method is used by the RPC client when sending RPC requests to a
        server.

        The driver must use the *topic*, *exchange*, and *server* (if present)
        attributes of the *target* to construct the backend-native message
        address. The message address must match the format used by
        subscription(s) created by the :py:meth:`BaseDriver.listen` method.

        If the *target's* *fanout* attribute is set, a copy of the message must
        be sent to all subscriptions using the *exchange* and *topic*
        values. If *fanout* is not set, then only one subscriber should receive
        the message.  In the case of multiple subscribers to the same address,
        only one copy of the message is delivered. In this case the driver
        should implement a delivery pattern that distributes messages in a
        balanced fashion across the multiple subscribers.

        This method must block the caller until one of the following events
        occur:

        * the send operation completes successfully
        * *timeout* seconds elapse (if specified)
        * *retry* count is reached (if specified)

        The *wait_for_reply* parameter determines whether or not the caller
        expects a response to the RPC request. If True, this method must block
        until a response message is received.  This method then returns the
        response message to the caller.  The driver must implement a mechanism
        for routing incoming responses back to their corresponding send
        request. How this is done may vary based on the type of messaging
        backend, but typically it involves having the driver create an internal
        subscription for reply messages and setting the request message's
        *reply-to* header to the subscription address.  The driver may also
        need to supply a correlation identifier for mapping the response back
        to the sender.  See :py:meth:`RpcIncomingMessage.reply`

        If *wait_for_reply* is False this method will block until the message
        has been handed off to the backend - there is no need to confirm that
        the message has been delivered. Once the handoff completes this method
        returns.

        The driver may attempt to retry sending the message should a
        recoverable error occur that prevents the message from being passed to
        the backend. The *retry* parameter specifies how many attempts to
        re-send the message the driver may make before raising a
        :py:exc:`MessageDeliveryFailure` exception. A value of None or -1 means
        unlimited retries. 0 means no retry is attempted. N means attempt at
        most N retries before failing. **Note well:** the driver MUST guarantee
        that the message is not duplicated by the retry process.

        :param target: The message's destination address
        :type target: Target
        :param ctxt: Context metadata provided by sending application which
            is transferred along with the message.
        :type ctxt: dict
        :param message: message provided by the caller
        :type message: dict
        :param wait_for_reply: If True block until a reply message is received.
        :type wait_for_reply: bool
        :param timeout: Maximum time in seconds to block waiting for the send
            operation to complete. Should this expire the :py:meth:`send` must
            raise a :py:exc:`MessagingTimeout` exception
        :type timeout: float
        :param call_monitor_timeout: Maximum time the client will wait for the
            call to complete or receive a message heartbeat indicating the
            remote side is still executing.
        :type call_monitor_timeout: float
        :param retry: maximum message send attempts permitted
        :type retry: int
        :returns: A reply message or None if no reply expected
        :raises: :py:exc:`MessagingException`, any exception thrown by the
            remote server when executing the RPC call.
        """

    @abc.abstractmethod
    def send_notification(self, target, ctxt, message, version, retry):
        """Send a notification message to the given target. This method is used
        by the Notifier to send notification messages to a Listener.

        Notifications use a *store and forward* delivery pattern. The driver
        must allow for delivery in the case where the intended recipient is
        not present at the time the notification is published. Typically this
        requires a messaging backend that has the ability to store messages
        until a consumer is present.

        Therefore this method must block at least until the backend accepts
        ownership of the message.  This method does not guarantee that the
        message has or will be processed by the intended recipient.

        The driver must use the *topic* and *exchange* attributes of the
        *target* to construct the backend-native message address. The message
        address must match the format used by subscription(s) created by the
        :py:meth:`BaseDriver.listen_for_notifications` method. Only one copy of
        the message is delivered in the case of multiple subscribers to the
        same address. In this case the driver should implement a delivery
        pattern that distributes messages in a balanced fashion across the
        multiple subscribers.

        There is an exception to the single delivery semantics described above:
        the *pool* parameter to the
        :py:meth:`BaseDriver.listen_for_notifications` method may be used to
        set up shared subscriptions.  See
        :py:meth:`BaseDriver.listen_for_notifications` for details.

        This method must also honor the *retry* parameter. See
        :py:meth:`BaseDriver.send` for details regarding implementing the
        *retry* process.

        *version* indicates whether or not the message should be encapsulated
        in an envelope.  A value < 2.0 should not envelope the message. See
        :py:func:`common.serialize_msg` for more detail.

        :param target: The message's destination address
        :type target: Target
        :param ctxt: Context metadata provided by sending application which
            is transferred along with the message.
        :type ctxt: dict
        :param message: message provided by the caller
        :type message: dict
        :param version: determines the envelope for the message
        :type version: float
        :param retry: maximum message send attempts permitted
        :type retry: int
        :returns: None
        :raises: :py:exc:`MessagingException`
        """

    @abc.abstractmethod
    def listen(self, target, batch_size, batch_timeout):
        """Construct a listener for the given target.  The listener may be
        either a :py:class:`Listener` or :py:class:`PollStyleListener`
        depending on the driver's preference.  This method is used by the RPC
        server.

        The driver must create subscriptions to the address provided in
        *target*. These subscriptions must then be associated with a
        :py:class:`Listener` or :py:class:`PollStyleListener` which is returned
        by this method. See :py:meth:`BaseDriver.send` for more detail
        regarding message addressing.

        The driver must support receiving messages sent to the following
        addresses derived from the values in *target*:

        * all messages sent to the exchange and topic given in the target.
          This includes messages sent using a fanout pattern.
        * if the server attribute of the target is set then the driver must
          also subscribe to messages sent to the exchange, topic, and server

        For example, given a target with exchange 'my-exchange', topic
        'my-topic', and server 'my-server', the driver would create
        subscriptions for:

        * all messages sent to my-exchange and my-topic (including fanout)
        * all messages sent to my-exchange, my-topic, and my-server

        The driver must pass messages arriving from these subscriptions to the
        listener. For :py:class:`PollStyleListener` the driver should trigger
        the :py:meth:`PollStyleListener.poll` method to unblock and return the
        incoming messages. For :py:class:`Listener` the driver should invoke
        the callback with the incoming messages.

        This method only blocks long enough to establish the subscription(s)
        and construct the listener.  In the case of failover, the driver must
        restore the subscription(s). Subscriptions should remain active until
        the listener is stopped.

        :param target: The address(es) to subscribe to.
        :type target: Target
        :param batch_size: passed to the listener
        :type batch_size: int
        :param batch_timeout: passed to the listener
        :type batch_timeout: float
        :returns: None
        :raises: :py:exc:`MessagingException`
        """

    @abc.abstractmethod
    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Construct a notification listener for the given list of
        tuples of (target, priority) addresses.

        The driver must create a subscription for each (*target*, *priority*)
        pair. The topic for the subscription is created for each pair using the
        format `"%s.%s" % (target.topic, priority)`.  This format is used by
        the caller of the :py:meth:`BaseDriver.send_notification` when setting
        the topic member of the target parameter.

        Only the *exchange* and *topic* must be considered when creating
        subscriptions. *server* and *fanout* must be ignored.

        The *pool* parameter, if specified, should cause the driver to create a
        subscription that is shared with other subscribers using the same pool
        identifier. Each pool gets a single copy of the message. For example if
        there is a subscriber pool with identifier **foo** and another pool
        **bar**, then one **foo** subscriber and one **bar** subscriber will
        each receive a copy of the message.  The driver should implement a
        delivery pattern that distributes message in a balanced fashion across
        the subscribers in a pool.

        The driver must raise a :py:exc:`NotImplementedError` if pooling is not
        supported and a pool identifier is passed in.

        Refer to the description of :py:meth:`BaseDriver.send_notification` for
        further details regarding implementation.

        :param targets_and_priorities: List of (target, priority) pairs
        :type targets_and_priorities: list
        :param pool: pool identifier
        :type pool: str
        :param batch_size: passed to the listener
        :type batch_size: int
        :param batch_timeout: passed to the listener
        :type batch_timeout: float
        :returns: None
        :raises: :py:exc:`MessagingException`, :py:exc:`NotImplementedError`
        """

    @abc.abstractmethod
    def cleanup(self):
        """Release all resources used by the driver.  This method must block
        until the cleanup is complete.
        """
