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
Controller that manages the interface between the driver and the messaging
service.

This module defines a Controller class that is responsible for performing
messaging-related operations (Tasks) requested by the driver, and for managing
the connection to the messaging service.  The Controller creates a background
thread which performs all messaging operations and socket I/O.  The
Controller's messaging logic is executed in the background thread via lambda
functions scheduled by the Controller.
"""

import abc
import collections
import logging
import os
import platform
import random
import sys
import threading
import time
import uuid

import proton
import pyngus
from six import iteritems
from six import itervalues
from six import moves

from oslo_messaging._drivers.amqp1_driver.addressing import AddresserFactory
from oslo_messaging._drivers.amqp1_driver.addressing import keyify
from oslo_messaging._drivers.amqp1_driver.addressing import SERVICE_NOTIFY
from oslo_messaging._drivers.amqp1_driver.addressing import SERVICE_RPC
from oslo_messaging._drivers.amqp1_driver import eventloop
from oslo_messaging._i18n import _LE, _LI, _LW
from oslo_messaging import exceptions
from oslo_messaging.target import Target
from oslo_messaging import transport

if hasattr(time, 'monotonic'):
    now = time.monotonic
else:
    from monotonic import monotonic as now  # noqa

LOG = logging.getLogger(__name__)


class Task(object):
    """Run a command on the eventloop thread, wait until it completes
    """

    @abc.abstractmethod
    def wait(self):
        """Called by the client thread to wait for the operation to
        complete. The implementation may optionally return a value.
        """

    @abc.abstractmethod
    def _execute(self, controller):
        """This method will be run on the eventloop thread to perform the
        messaging operation.
        """


class SubscribeTask(Task):
    """A task that creates a subscription to the given target.  Messages
    arriving from the target are given to the listener.
    """
    def __init__(self, target, listener, notifications=False):
        super(SubscribeTask, self).__init__()
        self._target = target()  # mutable - need a copy
        self._subscriber_id = listener.id
        self._in_queue = listener.incoming
        self._service = SERVICE_NOTIFY if notifications else SERVICE_RPC
        self._wakeup = threading.Event()

    def wait(self):
        self._wakeup.wait()

    def _execute(self, controller):
        controller.subscribe(self)
        self._wakeup.set()


class SendTask(Task):
    """This is the class used by the Controller to send messages to a given
    destination.
    """
    def __init__(self, name, message, target, deadline, retry,
                 wait_for_ack, notification=False):
        super(SendTask, self).__init__()
        self.name = name
        # note: target can be either a Target class or a string
        # target is mutable - make copy
        self.target = target() if isinstance(target, Target) else target
        self.message = message
        self.deadline = deadline
        self.wait_for_ack = wait_for_ack
        self.service = SERVICE_NOTIFY if notification else SERVICE_RPC
        self.timer = None
        self._retry = None if retry is None or retry < 0 else retry
        self._wakeup = threading.Event()
        self._error = None
        self._sender = None

    def wait(self):
        self._wakeup.wait()
        return self._error

    def _execute(self, controller):
        if self.deadline:
            # time out the send
            self.timer = controller.processor.alarm(self._on_timeout,
                                                    self.deadline)
        controller.send(self)

    def _prepare(self, sender):
        """Called immediately before the message is handed off to the i/o
        system.  This implies that the sender link is up.
        """
        self._sender = sender

    def _on_ack(self, state, info):
        """If wait_for_ack is True, this is called by the eventloop thread when
        the ack/nack is received from the peer.  If wait_for_ack is False this
        is called by the eventloop right after the message is written to the
        link.  In the last case state will always be set to ACCEPTED.
        """
        if state != pyngus.SenderLink.ACCEPTED:
            msg = ("{name} message send to {target} failed: remote"
                   " disposition: {disp}, info:"
                   "{info}".format(name=self.name,
                                   target=self.target,
                                   disp=state,
                                   info=info))
            self._error = exceptions.MessageDeliveryFailure(msg)
            LOG.warning("%s", msg)
        self._cleanup()
        self._wakeup.set()

    def _on_timeout(self):
        """Invoked by the eventloop when our timer expires
        """
        self.timer = None
        self._sender and self._sender.cancel_send(self)
        msg = ("{name} message sent to {target} failed: timed"
               " out".format(name=self.name, target=self.target))
        LOG.warning("%s", msg)
        # Only raise a MessagingTimeout if the caller has explicitly specified
        # a timeout.
        self._error = exceptions.MessagingTimeout(msg) \
            if self.message.ttl else \
            exceptions.MessageDeliveryFailure(msg)
        self._cleanup()
        self._wakeup.set()

    def _on_error(self, description):
        """Invoked by the eventloop if the send operation fails for reasons
        other than timeout and nack.
        """
        msg = ("{name} message sent to {target} failed:"
               " {reason}".format(name=self.name,
                                  target=self.target,
                                  reason=description))
        LOG.warning("%s", msg)
        self._error = exceptions.MessageDeliveryFailure(msg)
        self._cleanup()
        self._wakeup.set()

    def _cleanup(self):
        self._sender = None
        if self.timer:
            self.timer.cancel()
            self.timer = None

    @property
    def _can_retry(self):
        # has the retry count expired?
        if self._retry is not None:
            self._retry -= 1
            if self._retry < 0:
                return False
        return True


class RPCCallTask(SendTask):
    """Performs an RPC Call.  Sends the request and waits for a response from
    the destination.
    """
    def __init__(self, target, message, deadline, retry, wait_for_ack):
        super(RPCCallTask, self).__init__("RPC Call", message, target,
                                          deadline, retry, wait_for_ack)
        self._reply_link = None
        self._reply_msg = None
        self._msg_id = None

    def wait(self):
        error = super(RPCCallTask, self).wait()
        return error or self._reply_msg

    def _prepare(self, sender):
        super(RPCCallTask, self)._prepare(sender)
        # reserve a message id for mapping the received response
        if self._msg_id:
            # already set so this is a re-transmit. To be safe cancel the old
            # msg_id and allocate a fresh one.
            self._reply_link.cancel_response(self._msg_id)
        self._reply_link = sender._reply_link
        rl = self._reply_link
        self._msg_id = rl.prepare_for_response(self.message, self._on_reply)

    def _on_reply(self, message):
        # called if/when the reply message arrives
        self._reply_msg = message
        self._cleanup()
        self._wakeup.set()

    def _on_ack(self, state, info):
        if state != pyngus.SenderLink.ACCEPTED:
            super(RPCCallTask, self)._on_ack(state, info)
        # must wait for reply if ACCEPTED

    def _cleanup(self):
        if self._msg_id:
            self._reply_link.cancel_response(self._msg_id)
            self._msg_id = None
        self._reply_link = None
        super(RPCCallTask, self)._cleanup()


class RPCMonitoredCallTask(RPCCallTask):
    """An RPC call which expects a periodic heartbeat until the response is
    received.  There are two timeouts:
    deadline - overall hard timeout, implemented in RPCCallTask
    call_monitor_timeout - keep alive timeout, reset when heartbeat arrives
    """
    def __init__(self, target, message, deadline, call_monitor_timeout,
                 retry, wait_for_ack):
        super(RPCMonitoredCallTask, self).__init__(target, message, deadline,
                                                   retry, wait_for_ack)
        assert call_monitor_timeout is not None  # nosec
        self._monitor_timeout = call_monitor_timeout
        self._monitor_timer = None
        self._set_alarm = None

    def _execute(self, controller):
        self._set_alarm = controller.processor.defer
        self._monitor_timer = self._set_alarm(self._call_timeout,
                                              self._monitor_timeout)
        super(RPCMonitoredCallTask, self)._execute(controller)

    def _call_timeout(self):
        # monitor_timeout expired
        self._monitor_timer = None
        self._sender and self._sender.cancel_send(self)
        msg = ("{name} message sent to {target} failed: call monitor timed"
               " out".format(name=self.name, target=self.target))
        LOG.warning("%s", msg)
        self._error = exceptions.MessagingTimeout(msg)
        self._cleanup()
        self._wakeup.set()

    def _on_reply(self, message):
        # if reply is null, then this is the call monitor heartbeat
        if message.body is None:
            self._monitor_timer.cancel()
            self._monitor_timer = self._set_alarm(self._call_timeout,
                                                  self._monitor_timeout)
        else:
            super(RPCMonitoredCallTask, self)._on_reply(message)

    def _cleanup(self):
        self._set_alarm = None
        if self._monitor_timer:
            self._monitor_timer.cancel()
            self._monitor_timer = None
        super(RPCMonitoredCallTask, self)._cleanup()


class MessageDispositionTask(Task):
    """A task that updates the message disposition as accepted or released
    for a Server
    """
    def __init__(self, disposition, released=False):
        super(MessageDispositionTask, self).__init__()
        self._disposition = disposition
        self._released = released

    def wait(self):
        # disposition update does not have to block the sender since there is
        # no result to pend for.  This avoids a thread context switch with
        # every RPC call
        pass

    def _execute(self, controller):
        try:
            self._disposition(self._released)
        except Exception as e:
            # there's really nothing we can do about a failed disposition.
            LOG.exception(_LE("Message acknowledgment failed: %s"), e)


class Sender(pyngus.SenderEventHandler):
    """A link for sending to a particular destination on the message bus.
    """
    def __init__(self, destination, scheduler, delay, service):
        super(Sender, self).__init__()
        self._destination = destination
        self._service = service
        self._address = None
        self._link = None
        self._scheduler = scheduler
        self._delay = delay  # for re-connecting/re-transmitting
        # holds all pending SendTasks
        self._pending_sends = collections.deque()
        # holds all messages sent but not yet acked
        self._unacked = set()
        self._reply_link = None
        self._connection = None
        self._resend_timer = None

    @property
    def pending_messages(self):
        return len(self._pending_sends)

    @property
    def unacked_messages(self):
        return len(self._unacked)

    def attach(self, connection, reply_link, addresser):
        """Open the link. Called by the Controller when the AMQP connection
        becomes active.
        """
        self._connection = connection
        self._reply_link = reply_link
        self._address = addresser.resolve(self._destination, self._service)
        LOG.debug("Sender %s attached", self._address)
        self._link = self._open_link()

    def detach(self):
        """Close the link.  Called by the controller when shutting down or in
        response to a close requested by the remote.  May be re-attached later
        (after a reset is done)
        """
        LOG.debug("Sender %s detached", self._address)
        self._connection = None
        self._reply_link = None
        if self._resend_timer:
            self._resend_timer.cancel()
            self._resend_timer = None
        if self._link:
            self._link.close()

    def reset(self, reason="Link reset"):
        """Called by the controller on connection failover. Release all link
        resources, abort any in-flight messages, and check the retry limit on
        all pending send requests.
        """
        self._address = None
        self._connection = None
        self._reply_link = None
        if self._link:
            self._link.destroy()
            self._link = None
        self._abort_unacked(reason)
        self._check_retry_limit(reason)

    def destroy(self, reason="Link destroyed"):
        """Destroy the sender and all pending messages.  Called on driver
        shutdown.
        """
        LOG.debug("Sender %s destroyed", self._address)
        self.reset(reason)
        self._abort_pending(reason)

    def send_message(self, send_task):
        """Send a message out the link.
        """
        if not self._can_send or self._pending_sends:
            self._pending_sends.append(send_task)
        else:
            self._send(send_task)

    def cancel_send(self, send_task):
        """Attempts to cancel a send request.  It is possible that the send has
        already completed, so this is best-effort.
        """
        # may be in either list, or none
        self._unacked.discard(send_task)
        try:
            self._pending_sends.remove(send_task)
        except ValueError:
            pass

    # Pyngus callbacks:

    def sender_active(self, sender_link):
        LOG.debug("Sender %s active", self._address)
        self._send_pending()

    def credit_granted(self, sender_link):
        pass

    def sender_remote_closed(self, sender_link, pn_condition):
        # The remote has initiated a close.  This could happen when the message
        # bus is shutting down, or it detected an error
        LOG.warning(_LW("sender %(addr)s failed due to remote initiated close:"
                        " condition=%(cond)s"),
                    {'addr': self._address, 'cond': pn_condition})
        self._link.close()
        # sender_closed() will be called once the link completes closing

    def sender_closed(self, sender_link):
        self._handle_sender_closed()

    def sender_failed(self, sender_link, error):
        """Protocol error occurred."""
        LOG.warning(_LW("sender %(addr)s failed error=%(error)s"),
                    {'addr': self._address, 'error': error})
        self._handle_sender_closed(str(error))

    # end Pyngus callbacks

    def _handle_sender_closed(self, reason="Sender closed"):
        self._abort_unacked(reason)
        if self._connection:
            # still attached, so attempt to restart the link
            self._check_retry_limit(reason)
            self._scheduler.defer(self._reopen_link, self._delay)

    def _check_retry_limit(self, reason):
        # Called on recoverable connection or link failure.  Remove any pending
        # sends that have exhausted their retry count:
        expired = set()
        for send_task in self._pending_sends:
            if not send_task._can_retry:
                expired.add(send_task)
                send_task._on_error("Message send failed: %s" % reason)
        while expired:
            self._pending_sends.remove(expired.pop())

    def _abort_unacked(self, error):
        # fail all messages that have been sent to the message bus and have not
        # been acked yet
        while self._unacked:
            send_task = self._unacked.pop()
            send_task._on_error("Message send failed: %s" % error)

    def _abort_pending(self, error):
        # fail all messages that have yet to be sent to the message bus
        while self._pending_sends:
            send_task = self._pending_sends.popleft()
            send_task._on_error("Message send failed: %s" % error)

    @property
    def _can_send(self):
        return self._link and self._link.active

    # acknowledge status
    _TIMED_OUT = pyngus.SenderLink.TIMED_OUT
    _ACCEPTED = pyngus.SenderLink.ACCEPTED
    _RELEASED = pyngus.SenderLink.RELEASED
    _MODIFIED = pyngus.SenderLink.MODIFIED

    def _send(self, send_task):
        send_task._prepare(self)
        send_task.message.address = self._address
        if send_task.wait_for_ack:
            self._unacked.add(send_task)

            def pyngus_callback(link, handle, state, info):
                # invoked when the message bus (n)acks this message
                if state == Sender._TIMED_OUT:
                    # ignore pyngus timeout - we maintain our own timer
                    # which will properly deal with this case
                    return
                self._unacked.discard(send_task)
                if state == Sender._ACCEPTED:
                    send_task._on_ack(Sender._ACCEPTED, info)
                elif (state == Sender._RELEASED or
                      (state == Sender._MODIFIED and
                          # assuming delivery-failed means in-doubt:
                          not info.get("delivery-failed") and
                          not info.get("undeliverable-here"))):
                    # These states indicate that the message was never
                    # forwarded beyond the next hop so they can be
                    # re-transmitted without risk of duplication
                    self._resend(send_task)
                else:
                    # some error - let task figure it out...
                    send_task._on_ack(state, info)

            self._link.send(send_task.message,
                            delivery_callback=pyngus_callback,
                            handle=self,
                            deadline=send_task.deadline)
        else:  # do not wait for ack
            self._link.send(send_task.message,
                            delivery_callback=None,
                            handle=self,
                            deadline=send_task.deadline)
            send_task._on_ack(pyngus.SenderLink.ACCEPTED, {})

    def _resend(self, send_task):
        # the message bus returned the message without forwarding it. Wait a
        # bit for other outstanding sends to finish - most likely ending up
        # here since they are all going to the same destination - then resend
        # this message
        if send_task._can_retry:
            # note well: once there is something on the pending list no further
            # messages will be sent (they will all queue up behind this one).
            self._pending_sends.append(send_task)
            if self._resend_timer is None:
                sched = self._scheduler
                # this will get the pending sends going again
                self._resend_timer = sched.defer(self._resend_pending,
                                                 self._delay)
        else:
            send_task._on_error("Send retries exhausted")

    def _resend_pending(self):
        # run from the _resend_timer, attempt to resend pending messages
        self._resend_timer = None
        self._send_pending()

    def _send_pending(self):
        # flush all pending messages out
        if self._can_send:
            while self._pending_sends:
                self._send(self._pending_sends.popleft())

    def _open_link(self):
        name = "openstack.org/om/sender/[%s]/%s" % (self._address,
                                                    uuid.uuid4().hex)
        link = self._connection.create_sender(name=name,
                                              source_address=self._address,
                                              target_address=self._address,
                                              event_handler=self)
        link.open()
        return link

    def _reopen_link(self):
        if self._connection:
            if self._link:
                self._link.destroy()
            self._link = self._open_link()


class Replies(pyngus.ReceiverEventHandler):
    """This is the receiving link for all RPC reply messages.  Messages are
    routed to the proper incoming queue using the correlation-id header in the
    message.
    """
    def __init__(self, connection, on_ready, on_down, capacity):
        self._correlation = {}  # map of correlation-id to response queue
        self._on_ready = on_ready
        self._on_down = on_down
        rname = ("openstack.org/om/receiver/[rpc-response]/%s"
                 % uuid.uuid4().hex)
        self._receiver = connection.create_receiver("rpc-response",
                                                    event_handler=self,
                                                    name=rname)

        # capacity determines the maximum number of reply messages this link is
        # willing to receive. As messages are received and capacity is
        # consumed, this driver will 'top up' the capacity back to max
        # capacity.  This number should be large enough to avoid needlessly
        # flow-controlling the replies.
        self._capacity = capacity
        self._capacity_low = (capacity + 1) / 2
        self._receiver.open()

    def detach(self):
        # close the link
        if self._receiver:
            self._receiver.close()

    def destroy(self):
        self._correlation.clear()
        if self._receiver:
            self._receiver.destroy()
            self._receiver = None

    def prepare_for_response(self, request, callback):
        """Apply a unique message identifier to this request message. This will
        be used to identify messages received in reply.  The identifier is
        placed in the 'id' field of the request message.  It is expected that
        the identifier will appear in the 'correlation-id' field of the
        corresponding response message.

        When the caller is done receiving replies, it must call cancel_response
        """
        request.id = uuid.uuid4().hex
        # reply is placed on reply_queue
        self._correlation[request.id] = callback
        request.reply_to = self._receiver.source_address
        return request.id

    def cancel_response(self, msg_id):
        """Abort waiting for the response message corresponding to msg_id.
        This can be used if the request fails and no reply is expected.
        """
        try:
            del self._correlation[msg_id]
        except KeyError:
            pass

    @property
    def active(self):
        return self._receiver and self._receiver.active

    # Pyngus ReceiverLink event callbacks:

    def receiver_active(self, receiver_link):
        """This is a Pyngus callback, invoked by Pyngus when the receiver_link
        has transitioned to the open state and is able to receive incoming
        messages.
        """
        LOG.debug("Replies link active src=%s", self._receiver.source_address)
        receiver_link.add_capacity(self._capacity)
        self._on_ready()

    def receiver_remote_closed(self, receiver, pn_condition):
        """This is a Pyngus callback, invoked by Pyngus when the peer of this
        receiver link has initiated closing the connection.
        """
        if pn_condition:
            LOG.error(_LE("Reply subscription closed by peer: %s"),
                      pn_condition)
        receiver.close()

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.error(_LE("Link to reply queue failed. error=%(error)s"),
                  {"error": error})
        self._on_down()

    def receiver_closed(self, receiver_link):
        self._on_down()

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        key = message.correlation_id
        try:
            self._correlation[key](message)
            receiver.message_accepted(handle)
        except KeyError:
            LOG.warning(_LW("Can't find receiver for response msg id=%s, "
                            "dropping!"), key)
            receiver.message_modified(handle, True, True, None)
        # ensure we have enough credit
        if receiver.capacity <= self._capacity_low:
            receiver.add_capacity(self._capacity - receiver.capacity)


class Server(pyngus.ReceiverEventHandler):
    """A group of links that receive messages from a set of addresses derived
    from a given target.  Messages arriving on the links are placed on the
    'incoming' queue.
    """
    def __init__(self, target, incoming, scheduler, delay, capacity):
        self._target = target
        self._incoming = incoming
        self._addresses = []
        self._capacity = capacity   # credit per each link
        self._capacity_low = (capacity + 1) / 2
        self._receivers = []
        self._scheduler = scheduler
        self._delay = delay  # for link re-attach
        self._connection = None
        self._reopen_scheduled = False

    def attach(self, connection):
        """Create receiver links over the given connection for all the
        configured addresses.
        """
        self._connection = connection
        for a in self._addresses:
            name = "openstack.org/om/receiver/[%s]/%s" % (a, uuid.uuid4().hex)
            r = self._open_link(a, name)
            self._receivers.append(r)

    def detach(self):
        """Attempt a clean shutdown of the links"""
        self._connection = None
        self._addresses = []
        for receiver in self._receivers:
            receiver.close()

    def reset(self):
        # destroy the links, but keep the addresses around since we may be
        # failing over.  Since links are destroyed, this cannot be called from
        # any of the following ReceiverLink callbacks.
        self._connection = None
        self._addresses = []
        self._reopen_scheduled = False
        for r in self._receivers:
            r.destroy()
        self._receivers = []

    # Pyngus ReceiverLink event callbacks.  Note that all of the Server's links
    # share this handler

    def receiver_remote_closed(self, receiver, pn_condition):
        """This is a Pyngus callback, invoked by Pyngus when the peer of this
        receiver link has initiated closing the connection.
        """
        LOG.debug("Server subscription to %s remote detach",
                  receiver.source_address)
        if pn_condition:
            vals = {
                "addr": receiver.source_address or receiver.target_address,
                "err_msg": pn_condition
            }
            LOG.error(_LE("Server subscription %(addr)s closed "
                          "by peer: %(err_msg)s"), vals)
        receiver.close()

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.error(_LE("Listener link queue failed. error=%(error)s"),
                  {"error": error})
        self.receiver_closed(receiver_link)

    def receiver_closed(self, receiver_link):
        LOG.debug("Server subscription to %s closed",
                  receiver_link.source_address)
        # If still attached, attempt to re-start link
        if self._connection and not self._reopen_scheduled:
            LOG.debug("Server subscription reopen scheduled")
            self._reopen_scheduled = True
            self._scheduler.defer(self._reopen_links, self._delay)

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        def message_disposition(released=False):
            if receiver in self._receivers and not receiver.closed:
                if released:
                    receiver.message_released(handle)
                else:
                    receiver.message_accepted(handle)
                if receiver.capacity <= self._capacity_low:
                    receiver.add_capacity(self._capacity - receiver.capacity)
            else:
                LOG.debug("Can't find receiver for settlement")

        qentry = {"message": message, "disposition": message_disposition}
        self._incoming.put(qentry)

    def _open_link(self, address, name):
        props = {"snd-settle-mode": "mixed"}
        r = self._connection.create_receiver(source_address=address,
                                             target_address=address,
                                             event_handler=self,
                                             name=name,
                                             properties=props)
        r.add_capacity(self._capacity)
        r.open()
        return r

    def _reopen_links(self):
        # attempt to re-establish any closed links
        LOG.debug("Server subscription reopening")
        self._reopen_scheduled = False
        if self._connection:
            for i in range(len(self._receivers)):
                link = self._receivers[i]
                if link.closed:
                    addr = link.target_address
                    name = link.name
                    link.destroy()
                    self._receivers[i] = self._open_link(addr, name)


class RPCServer(Server):
    """Subscribes to RPC addresses"""
    def __init__(self, target, incoming, scheduler, delay, capacity):
        super(RPCServer, self).__init__(target, incoming, scheduler, delay,
                                        capacity)

    def attach(self, connection, addresser):
        # Generate the AMQP 1.0 addresses for the base class
        self._addresses = [
            addresser.unicast_address(self._target, SERVICE_RPC),
            addresser.multicast_address(self._target, SERVICE_RPC),
            addresser.anycast_address(self._target, SERVICE_RPC)
        ]
        # now invoke the base class with the generated addresses
        super(RPCServer, self).attach(connection)


class NotificationServer(Server):
    """Subscribes to Notification addresses"""
    def __init__(self, target, incoming, scheduler, delay, capacity):
        super(NotificationServer, self).__init__(target, incoming, scheduler,
                                                 delay, capacity)

    def attach(self, connection, addresser):
        # Generate the AMQP 1.0 addresses for the base class
        self._addresses = [
            addresser.anycast_address(self._target, SERVICE_NOTIFY)
        ]
        # now invoke the base class with the generated addresses
        super(NotificationServer, self).attach(connection)


class Hosts(object):
    """An order list of TransportHost addresses. Connection failover progresses
    from one host to the next.  The default realm comes from the configuration
    and is only used if no realm is present in the URL.
    """
    def __init__(self, url, default_realm=None):
        self.virtual_host = url.virtual_host
        if url.hosts:
            self._entries = url.hosts[:]
        else:
            self._entries = [transport.TransportHost(hostname="localhost",
                                                     port=5672)]
        for entry in self._entries:
            entry.port = entry.port or 5672
            entry.username = entry.username
            entry.password = entry.password
            if default_realm and entry.username and '@' not in entry.username:
                entry.username = entry.username + '@' + default_realm
        self._current = random.randint(0, len(self._entries) - 1)  # nosec

    @property
    def current(self):
        return self._entries[self._current]

    def next(self):
        if len(self._entries) > 1:
            self._current = (self._current + 1) % len(self._entries)
        return self.current

    def __repr__(self):
        return '<Hosts ' + str(self) + '>'

    def __str__(self):
        r = ', vhost=%s' % self.virtual_host if self.virtual_host else ''
        return ", ".join(["%r" % th for th in self._entries]) + r


class Controller(pyngus.ConnectionEventHandler):
    """Controls the connection to the AMQP messaging service.  This object is
    the 'brains' of the driver.  It maintains the logic for addressing, sending
    and receiving messages, and managing the connection.  All messaging and I/O
    work is done on the Eventloop thread, allowing the driver to run
    asynchronously from the messaging clients.
    """
    def __init__(self, url, default_exchange, config):
        self.processor = None
        self._socket_connection = None
        self._node = platform.node() or "<UNKNOWN>"
        self._command = os.path.basename(sys.argv[0])
        self._pid = os.getpid()
        # queue of drivertask objects to execute on the eventloop thread
        self._tasks = moves.queue.Queue(maxsize=500)
        # limit the number of Task()'s to execute per call to _process_tasks().
        # This allows the eventloop main thread to return to servicing socket
        # I/O in a timely manner
        self._max_task_batch = 50
        # cache of all Sender links indexed by address:
        self._all_senders = {}
        # active Sender links indexed by address:
        self._active_senders = set()
        # closing Sender links indexed by address:
        self._purged_senders = []
        # Servers indexed by target. Each entry is a map indexed by the
        # specific ProtonListener's identifier:
        self._servers = {}

        self._container_name = config.oslo_messaging_amqp.container_name
        self.idle_timeout = config.oslo_messaging_amqp.idle_timeout
        self.trace_protocol = config.oslo_messaging_amqp.trace
        self.ssl = config.oslo_messaging_amqp.ssl
        self.ssl_ca_file = config.oslo_messaging_amqp.ssl_ca_file
        self.ssl_cert_file = config.oslo_messaging_amqp.ssl_cert_file
        self.ssl_key_file = config.oslo_messaging_amqp.ssl_key_file
        self.ssl_key_password = config.oslo_messaging_amqp.ssl_key_password
        self.ssl_verify_vhost = config.oslo_messaging_amqp.ssl_verify_vhost
        self.pseudo_vhost = config.oslo_messaging_amqp.pseudo_vhost
        self.sasl_mechanisms = config.oslo_messaging_amqp.sasl_mechanisms
        self.sasl_config_dir = config.oslo_messaging_amqp.sasl_config_dir
        self.sasl_config_name = config.oslo_messaging_amqp.sasl_config_name
        self.hosts = Hosts(url, config.oslo_messaging_amqp.sasl_default_realm)
        self.conn_retry_interval = \
            config.oslo_messaging_amqp.connection_retry_interval
        self.conn_retry_backoff = \
            config.oslo_messaging_amqp.connection_retry_backoff
        self.conn_retry_interval_max = \
            config.oslo_messaging_amqp.connection_retry_interval_max
        self.link_retry_delay = config.oslo_messaging_amqp.link_retry_delay

        _opts = config.oslo_messaging_amqp
        factory_args = {"legacy_server_prefix": _opts.server_request_prefix,
                        "legacy_broadcast_prefix": _opts.broadcast_prefix,
                        "legacy_group_prefix": _opts.group_request_prefix,
                        "rpc_prefix": _opts.rpc_address_prefix,
                        "notify_prefix": _opts.notify_address_prefix,
                        "multicast": _opts.multicast_address,
                        "unicast": _opts.unicast_address,
                        "anycast": _opts.anycast_address,
                        "notify_exchange": _opts.default_notification_exchange,
                        "rpc_exchange": _opts.default_rpc_exchange}

        self.addresser_factory = AddresserFactory(default_exchange,
                                                  _opts.addressing_mode,
                                                  **factory_args)
        self.addresser = None

        # cannot send an RPC request until the replies link is active, as we
        # need the peer assigned address, so need to delay sending any RPC
        # requests until this link is active:
        self.reply_link = None
        # Set True when the driver is shutting down
        self._closing = False
        # only schedule one outstanding reconnect attempt at a time
        self._reconnecting = False
        self._delay = self.conn_retry_interval  # seconds between retries
        # prevent queuing up multiple requests to run _process_tasks()
        self._process_tasks_scheduled = False
        self._process_tasks_lock = threading.Lock()
        # credit levels for incoming links
        self._reply_credit = _opts.reply_link_credit
        self._rpc_credit = _opts.rpc_server_credit
        self._notify_credit = _opts.notify_server_credit
        # sender link maintenance timer and interval
        self._link_maint_timer = None
        self._link_maint_timeout = _opts.default_sender_link_timeout

    def connect(self):
        """Connect to the messaging service."""
        self.processor = eventloop.Thread(self._container_name, self._node,
                                          self._command, self._pid)
        self.processor.wakeup(lambda: self._do_connect())

    def add_task(self, task):
        """Add a Task for execution on processor thread."""
        self._tasks.put(task)
        self._schedule_task_processing()

    def shutdown(self, timeout=30):
        """Shutdown the messaging service."""
        LOG.info(_LI("Shutting down the AMQP 1.0 connection"))
        if self.processor:
            self.processor.wakeup(self._start_shutdown)
            LOG.debug("Waiting for eventloop to exit")
            self.processor.join(timeout)
            self._hard_reset("Shutting down")
            for sender in itervalues(self._all_senders):
                sender.destroy()
            self._all_senders.clear()
            self._servers.clear()
            self.processor.destroy()
            self.processor = None
        LOG.debug("Eventloop exited, driver shut down")

    # The remaining methods are reserved to run from the eventloop thread only!
    # They must not be invoked directly!

    # methods executed by Tasks created by the driver:

    def send(self, send_task):
        if send_task.deadline and send_task.deadline <= now():
            send_task._on_timeout()
            return
        key = keyify(send_task.target, send_task.service)
        sender = self._all_senders.get(key)
        if not sender:
            sender = Sender(send_task.target, self.processor,
                            self.link_retry_delay, send_task.service)
            self._all_senders[key] = sender
            if self.reply_link and self.reply_link.active:
                sender.attach(self._socket_connection.pyngus_conn,
                              self.reply_link, self.addresser)
        self._active_senders.add(key)
        sender.send_message(send_task)

    def subscribe(self, subscribe_task):
        """Subscribe to a given target"""
        if subscribe_task._service == SERVICE_NOTIFY:
            t = "notification"
            server = NotificationServer(subscribe_task._target,
                                        subscribe_task._in_queue,
                                        self.processor,
                                        self.link_retry_delay,
                                        self._notify_credit)
        else:
            t = "RPC"
            server = RPCServer(subscribe_task._target,
                               subscribe_task._in_queue,
                               self.processor,
                               self.link_retry_delay,
                               self._rpc_credit)

        LOG.debug("Subscribing to %(type)s target %(target)s",
                  {'type': t, 'target': subscribe_task._target})
        key = keyify(subscribe_task._target, subscribe_task._service)
        servers = self._servers.get(key)
        if servers is None:
            servers = {}
            self._servers[key] = servers
        servers[subscribe_task._subscriber_id] = server
        if self._active:
            server.attach(self._socket_connection.pyngus_conn,
                          self.addresser)

    # commands executed on the processor (eventloop) via 'wakeup()':

    def _do_connect(self):
        """Establish connection and reply subscription on processor thread."""
        host = self.hosts.current
        conn_props = {'properties': {u'process': self._command,
                                     u'pid': self._pid,
                                     u'node': self._node}}
        # only set hostname in the AMQP 1.0 Open performative if the message
        # bus can interpret it as the virtual host.  We leave it unspecified
        # since apparently noone can agree on how it should be used otherwise!
        if self.hosts.virtual_host and not self.pseudo_vhost:
            conn_props['hostname'] = self.hosts.virtual_host
        if self.idle_timeout:
            conn_props["idle-time-out"] = float(self.idle_timeout)
        if self.trace_protocol:
            conn_props["x-trace-protocol"] = self.trace_protocol

        # SSL configuration
        ssl_enabled = False
        if self.ssl:
            ssl_enabled = True
            conn_props["x-ssl"] = self.ssl
        if self.ssl_ca_file:
            conn_props["x-ssl-ca-file"] = self.ssl_ca_file
            ssl_enabled = True
        if self.ssl_cert_file:
            ssl_enabled = True
            conn_props["x-ssl-identity"] = (self.ssl_cert_file,
                                            self.ssl_key_file,
                                            self.ssl_key_password)
        if ssl_enabled:
            # Set the identity of the remote server for SSL to use when
            # verifying the received certificate.  Typically this is the DNS
            # name used to set up the TCP connections.  However some servers
            # may provide a certificate for the virtual host instead.  If that
            # is the case we need to use the virtual hostname instead.
            # Refer to SSL Server Name Indication (SNI) for the entire story:
            # https://tools.ietf.org/html/rfc6066
            if self.ssl_verify_vhost:
                if self.hosts.virtual_host:
                    conn_props['x-ssl-peer-name'] = self.hosts.virtual_host
            else:
                conn_props['x-ssl-peer-name'] = host.hostname

        # SASL configuration:
        if self.sasl_mechanisms:
            conn_props["x-sasl-mechs"] = self.sasl_mechanisms
        if self.sasl_config_dir:
            conn_props["x-sasl-config-dir"] = self.sasl_config_dir
        if self.sasl_config_name:
            conn_props["x-sasl-config-name"] = self.sasl_config_name

        self._socket_connection = self.processor.connect(host,
                                                         handler=self,
                                                         properties=conn_props)
        LOG.debug("Connection initiated")

    def _process_tasks(self):
        """Execute Task objects in the context of the processor thread."""
        with self._process_tasks_lock:
            self._process_tasks_scheduled = False
        count = 0
        while (not self._tasks.empty() and
               count < self._max_task_batch):
            try:
                self._tasks.get(False)._execute(self)
            except Exception as e:
                LOG.exception(_LE("Error processing task: %s"), e)
            count += 1

        # if we hit _max_task_batch, resume task processing later:
        if not self._tasks.empty():
            self._schedule_task_processing()

    def _schedule_task_processing(self):
        """_process_tasks() helper: prevent queuing up multiple requests for
        task processing.  This method is called both by the application thread
        and the processing thread.
        """
        if self.processor:
            with self._process_tasks_lock:
                already_scheduled = self._process_tasks_scheduled
                self._process_tasks_scheduled = True
            if not already_scheduled:
                self.processor.wakeup(lambda: self._process_tasks())

    def _start_shutdown(self):
        """Called when the application is closing the transport.
        Attempt to cleanly flush/close all links.
        """
        self._closing = True
        if self._active:
            # try a clean shutdown
            self._detach_senders()
            self._detach_servers()
            self.reply_link.detach()
            self._socket_connection.pyngus_conn.close()
        else:
            # don't wait for a close from the remote, may never happen
            self.processor.shutdown()

    # reply link callbacks:

    def _reply_link_ready(self):
        """Invoked when the Replies reply link has become active.  At this
        point, we are ready to receive messages, so start all pending RPC
        requests.
        """
        LOG.info(_LI("Messaging is active (%(hostname)s:%(port)s%(vhost)s)"),
                 {'hostname': self.hosts.current.hostname,
                  'port': self.hosts.current.port,
                  'vhost': ("/" + self.hosts.virtual_host
                            if self.hosts.virtual_host else "")})

        for sender in itervalues(self._all_senders):
            sender.attach(self._socket_connection.pyngus_conn,
                          self.reply_link, self.addresser)

    def _reply_link_down(self):
        # Treat it as a recoverable failure because the RPC reply address is
        # now invalid for all in-flight RPC requests.
        if not self._closing:
            self._detach_senders()
            self._detach_servers()
            self._socket_connection.pyngus_conn.close()
            # once closed, _handle_connection_loss() will initiate reconnect

    # callback from eventloop on socket error

    def socket_error(self, error):
        """Called by eventloop when a socket error occurs."""
        LOG.error(_LE("Socket failure: %s"), error)
        self._handle_connection_loss(str(error))

    # Pyngus connection event callbacks (and their helpers), all invoked from
    # the eventloop thread:

    def connection_failed(self, connection, error):
        """This is a Pyngus callback, invoked by Pyngus when a non-recoverable
        error occurs on the connection.
        """
        if connection is not self._socket_connection.pyngus_conn:
            # pyngus bug: ignore failure callback on destroyed connections
            return
        LOG.debug("AMQP Connection failure: %s", error)
        self._handle_connection_loss(str(error))

    def connection_active(self, connection):
        """This is a Pyngus callback, invoked by Pyngus when the connection to
        the peer is up.  At this point, the driver will activate all subscriber
        links (server) and the reply link.
        """
        LOG.debug("Connection active (%(hostname)s:%(port)s), subscribing...",
                  {'hostname': self.hosts.current.hostname,
                   'port': self.hosts.current.port})
        # allocate an addresser based on the advertised properties of the
        # message bus
        props = connection.remote_properties or {}
        self.addresser = self.addresser_factory(props,
                                                self.hosts.virtual_host
                                                if self.pseudo_vhost else None)
        for servers in itervalues(self._servers):
            for server in itervalues(servers):
                server.attach(self._socket_connection.pyngus_conn,
                              self.addresser)
        self.reply_link = Replies(self._socket_connection.pyngus_conn,
                                  self._reply_link_ready,
                                  self._reply_link_down,
                                  self._reply_credit)
        self._delay = self.conn_retry_interval   # reset
        # schedule periodic maintenance of sender links
        self._link_maint_timer = self.processor.defer(self._purge_sender_links,
                                                      self._link_maint_timeout)

    def connection_closed(self, connection):
        """This is a Pyngus callback, invoked by Pyngus when the connection has
        cleanly closed.  This occurs after the driver closes the connection
        locally, and the peer has acknowledged the close.  At this point, the
        shutdown of the driver's connection is complete.
        """
        LOG.debug("AMQP connection closed.")
        # if the driver isn't being shutdown, failover and reconnect
        self._handle_connection_loss("AMQP connection closed.")

    def connection_remote_closed(self, connection, reason):
        """This is a Pyngus callback, invoked by Pyngus when the peer has
        requested that the connection be closed.
        """
        # The messaging service/broker is trying to shut down the
        # connection. Acknowledge the close, and try to reconnect/failover
        # later once the connection has closed (connection_closed is called).
        if reason:
            LOG.info(_LI("Connection closed by peer: %s"), reason)
        self._detach_senders()
        self._detach_servers()
        self.reply_link.detach()
        self._socket_connection.pyngus_conn.close()

    def sasl_done(self, connection, pn_sasl, outcome):
        """This is a Pyngus callback invoked when the SASL handshake
        has completed.  The outcome of the handshake is passed in the outcome
        argument.
        """
        if outcome == proton.SASL.OK:
            return
        LOG.error(_LE("AUTHENTICATION FAILURE: Cannot connect to "
                      "%(hostname)s:%(port)s as user %(username)s"),
                  {'hostname': self.hosts.current.hostname,
                   'port': self.hosts.current.port,
                   'username': self.hosts.current.username})
        # pyngus will invoke connection_failed() eventually

    def _handle_connection_loss(self, reason):
        """The connection to the messaging service has been lost.  Try to
        reestablish the connection/failover if not shutting down the driver.
        """
        self.addresser = None
        self._socket_connection.close()
        if self._closing:
            # we're in the middle of shutting down the driver anyways,
            # just consider it done:
            self.processor.shutdown()
        else:
            # for some reason, we've lost the connection to the messaging
            # service.  Try to re-establish the connection:
            if not self._reconnecting:
                self._reconnecting = True
                LOG.info(_LI("delaying reconnect attempt for %d seconds"),
                         self._delay)
                self.processor.defer(lambda: self._do_reconnect(reason),
                                     self._delay)
                self._delay = min(self._delay * self.conn_retry_backoff,
                                  self.conn_retry_interval_max)
            if self._link_maint_timer:
                self._link_maint_timer.cancel()
                self._link_maint_timer = None

    def _do_reconnect(self, reason):
        """Invoked on connection/socket failure, failover and re-connect to the
        messaging service.
        """
        self._reconnecting = False
        if not self._closing:
            self._hard_reset(reason)
            host = self.hosts.next()
            LOG.info(_LI("Reconnecting to: %(hostname)s:%(port)s"),
                     {'hostname': host.hostname, 'port': host.port})
            self._socket_connection.connect(host)

    def _hard_reset(self, reason):
        """Reset the controller to its pre-connection state"""
        # note well: since this method destroys the connection, it cannot be
        # invoked directly from a pyngus callback.  Use processor.defer() to
        # run this method on the main loop instead.
        for sender in self._purged_senders:
            sender.destroy(reason)
        del self._purged_senders[:]
        self._active_senders.clear()
        unused = []
        for key, sender in iteritems(self._all_senders):
            # clean up any sender links that no longer have messages to send
            if sender.pending_messages == 0:
                unused.append(key)
            else:
                sender.reset(reason)
                self._active_senders.add(key)
        for key in unused:
            self._all_senders[key].destroy(reason)
            del self._all_senders[key]
        for servers in itervalues(self._servers):
            for server in itervalues(servers):
                server.reset()
        if self.reply_link:
            self.reply_link.destroy()
            self.reply_link = None
        if self._socket_connection:
            self._socket_connection.reset()

    def _detach_senders(self):
        """Close all sender links"""
        for sender in itervalues(self._all_senders):
            sender.detach()

    def _detach_servers(self):
        """Close all listener links"""
        for servers in itervalues(self._servers):
            for server in itervalues(servers):
                server.detach()

    def _purge_sender_links(self):
        """Purge inactive sender links"""
        if not self._closing:
            # destroy links that have already been closed
            for sender in self._purged_senders:
                sender.destroy("Idle link purged")
            del self._purged_senders[:]

            # determine next set to purge
            purge = set(self._all_senders.keys()) - self._active_senders
            for key in purge:
                sender = self._all_senders[key]
                if not sender.pending_messages and not sender.unacked_messages:
                    sender.detach()
                    self._purged_senders.append(self._all_senders.pop(key))
            self._active_senders.clear()
            self._link_maint_timer = \
                self.processor.defer(self._purge_sender_links,
                                     self._link_maint_timeout)

    @property
    def _active(self):
        # Is the connection up
        return (self._socket_connection and
                self._socket_connection.pyngus_conn.active)
