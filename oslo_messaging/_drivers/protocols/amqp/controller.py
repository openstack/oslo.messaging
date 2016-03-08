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
import logging
import threading
import uuid

from oslo_config import cfg
import proton
import pyngus
from six import moves

from oslo_messaging._drivers.protocols.amqp import eventloop
from oslo_messaging._drivers.protocols.amqp import opts
from oslo_messaging._i18n import _LE, _LI, _LW
from oslo_messaging import exceptions
from oslo_messaging import transport

LOG = logging.getLogger(__name__)


class Task(object):
    """Perform a messaging operation via the Controller."""
    @abc.abstractmethod
    def execute(self, controller):
        """This method will be run on the eventloop thread."""


class Sender(pyngus.SenderEventHandler):
    """A single outgoing link to a given address"""
    def __init__(self, address):
        self._address = address
        self._link = None

    def attach(self, connection):
        # open a link to the destination
        sname = "Producer-%s:src=%s:tgt=%s" % (uuid.uuid4().hex,
                                               self._address,
                                               self._address)
        self._link = connection.create_sender(name=sname,
                                              source_address=self._address,
                                              target_address=self._address)
        self._link.open()

    def detach(self):
        # close the link
        if self._link:
            self._link.close()

    def destroy(self):
        # drop reference to link. The link will be freed when the
        # connection is destroyed
        self._link = None

    def send(self, message, callback):
        # send message out the link, invoke callback when acked
        self._link.send(message, delivery_callback=callback)

    def sender_remote_closed(self, sender_link, pn_condition):
        LOG.debug("sender_remote_closed condition=%s", pn_condition)
        sender_link.close()

    def sender_failed(self, sender_link, error):
        """Protocol error occurred."""
        LOG.error(_LE("Outgoing link to %(addr) failed. error=%(error)"),
                  {"addr": self._address, "error": error})


class Replies(pyngus.ReceiverEventHandler):
    """This is the receiving link for all reply messages.  Messages are routed
    to the proper Listener's incoming queue using the correlation-id header in
    the message.
    """
    def __init__(self, connection, on_ready):
        self._correlation = {}  # map of correlation-id to response queue
        self._ready = False
        self._on_ready = on_ready
        rname = "Consumer-%s:src=[dynamic]:tgt=replies" % uuid.uuid4().hex
        self._receiver = connection.create_receiver("replies",
                                                    event_handler=self,
                                                    name=rname)

        # capacity determines the maximum number of reply messages this link
        # can receive. As messages are received and credit is consumed, this
        # driver will 'top up' the credit back to max capacity.  This number
        # should be large enough to avoid needlessly flow-controlling the
        # replies.
        self.capacity = 100  # TODO(kgiusti) guesstimate - make configurable
        self._credit = 0
        self._receiver.open()

    def detach(self):
        # close the link
        self._receiver.close()

    def destroy(self):
        # drop reference to link. Link will be freed when the connection is
        # released.
        self._receiver = None

    def ready(self):
        return self._ready

    def prepare_for_response(self, request, reply_queue):
        """Apply a unique message identifier to this request message. This will
        be used to identify messages sent in reply.  The identifier is placed
        in the 'id' field of the request message.  It is expected that the
        identifier will appear in the 'correlation-id' field of the
        corresponding response message.
        """
        request.id = uuid.uuid4().hex
        # reply is placed on reply_queue
        self._correlation[request.id] = reply_queue
        request.reply_to = self._receiver.source_address
        LOG.debug("Reply for msg id=%(id)s expected on link %(reply_to)s",
                  {'id': request.id, 'reply_to': request.reply_to})
        return request.id

    def cancel_response(self, msg_id):
        """Abort waiting for a response message.  This can be used if the
        request fails and no reply is expected.
        """
        if msg_id in self._correlation:
            del self._correlation[msg_id]

    # Pyngus ReceiverLink event callbacks:

    def receiver_active(self, receiver_link):
        """This is a Pyngus callback, invoked by Pyngus when the receiver_link
        has transitioned to the open state and is able to receive incoming
        messages.
        """
        self._ready = True
        self._update_credit()
        self._on_ready()
        LOG.debug("Replies expected on link %s",
                  self._receiver.source_address)

    def receiver_remote_closed(self, receiver, pn_condition):
        """This is a Pyngus callback, invoked by Pyngus when the peer of this
        receiver link has initiated closing the connection.
        """
        # TODO(kgiusti)  Log for now, possibly implement a recovery strategy if
        # necessary.
        if pn_condition:
            LOG.error(_LE("Reply subscription closed by peer: %s"),
                      pn_condition)
        receiver.close()

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.error(_LE("Link to reply queue %(addr) failed. error=%(error)"),
                  {"addr": self._address, "error": error})

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        self._credit = self._credit - 1
        self._update_credit()

        key = message.correlation_id
        if key in self._correlation:
            LOG.debug("Received response for msg id=%s", key)
            result = {"status": "OK",
                      "response": message}
            self._correlation[key].put(result)
            # cleanup (only need one response per request)
            del self._correlation[key]
            receiver.message_accepted(handle)
        else:
            LOG.warning(_LW("Can't find receiver for response msg id=%s, "
                            "dropping!"), key)
            receiver.message_modified(handle, True, True, None)

    def _update_credit(self):
        # ensure we have enough credit
        if self._credit < self.capacity / 2:
            self._receiver.add_capacity(self.capacity - self._credit)
            self._credit = self.capacity


class Server(pyngus.ReceiverEventHandler):
    """A group of links that receive messages from a set of addresses derived
    from a given target.  Messages arriving on the links are placed on the
    'incoming' queue.
    """
    def __init__(self, addresses, incoming, subscription_id):
        self._incoming = incoming
        self._addresses = addresses
        self._capacity = 500   # credit per link
        self._receivers = []
        self._id = subscription_id

    def attach(self, connection):
        """Create receiver links over the given connection for all the
        configured addresses.
        """
        for a in self._addresses:
            props = {"snd-settle-mode": "settled"}
            rname = "Consumer-%s:src=%s:tgt=%s" % (uuid.uuid4().hex, a, a)
            r = connection.create_receiver(source_address=a,
                                           target_address=a,
                                           event_handler=self,
                                           name=rname,
                                           properties=props)

            # TODO(kgiusti) Hardcoding credit here is sub-optimal.  A better
            # approach would monitor for a back-up of inbound messages to be
            # processed by the consuming application and backpressure the
            # sender based on configured thresholds.
            r.add_capacity(self._capacity)
            r.open()
            self._receivers.append(r)

    def detach(self):
        # close the links
        for receiver in self._receivers:
            receiver.close()

    def reset(self):
        # destroy the links, but keep the addresses around since we may be
        # failing over.  Since links are destroyed, this cannot be called from
        # any of the following ReceiverLink callbacks.
        for r in self._receivers:
            r.destroy()
        self._receivers = []

    # Pyngus ReceiverLink event callbacks:

    def receiver_remote_closed(self, receiver, pn_condition):
        """This is a Pyngus callback, invoked by Pyngus when the peer of this
        receiver link has initiated closing the connection.
        """
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
        LOG.error(_LE("Listener link queue %(addr) failed. error=%(error)"),
                  {"addr": self._address, "error": error})

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        if receiver.capacity < self._capacity / 2:
            receiver.add_capacity(self._capacity - receiver.capacity)
        self._incoming.put(message)
        LOG.debug("message received: %s", message)
        receiver.message_accepted(handle)


class Hosts(object):
    """An order list of TransportHost addresses. Connection failover
    progresses from one host to the next.  username and password come from the
    configuration and are used only if no username/password was given in the
    URL.
    """
    def __init__(self, entries=None, default_username=None,
                 default_password=None):
        if entries:
            self._entries = entries[:]
        else:
            self._entries = [transport.TransportHost(hostname="localhost",
                                                     port=5672)]
        for entry in self._entries:
            entry.port = entry.port or 5672
            entry.username = entry.username or default_username
            entry.password = entry.password or default_password
        self._current = 0

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
        return ", ".join(["%r" % th for th in self._entries])


class Controller(pyngus.ConnectionEventHandler):
    """Controls the connection to the AMQP messaging service.  This object is
    the 'brains' of the driver.  It maintains the logic for addressing, sending
    and receiving messages, and managing the connection.  All messaging and I/O
    work is done on the Eventloop thread, allowing the driver to run
    asynchronously from the messaging clients.
    """
    def __init__(self, hosts, default_exchange, config):
        self.processor = None
        self._socket_connection = None
        # queue of Task() objects to execute on the eventloop once the
        # connection is ready:
        self._tasks = moves.queue.Queue(maxsize=500)
        # limit the number of Task()'s to execute per call to _process_tasks().
        # This allows the eventloop main thread to return to servicing socket
        # I/O in a timely manner
        self._max_task_batch = 50
        # cache of sending links indexed by address:
        self._senders = {}
        # Servers indexed by target. Each entry is a map indexed by the
        # specific ProtonListener's identifier:
        self._servers = {}

        opt_group = cfg.OptGroup(name='oslo_messaging_amqp',
                                 title='AMQP 1.0 driver options')
        config.register_group(opt_group)
        config.register_opts(opts.amqp1_opts, group=opt_group)

        self.server_request_prefix = \
            config.oslo_messaging_amqp.server_request_prefix
        self.broadcast_prefix = config.oslo_messaging_amqp.broadcast_prefix
        self.group_request_prefix = \
            config.oslo_messaging_amqp.group_request_prefix
        self._container_name = config.oslo_messaging_amqp.container_name
        self.idle_timeout = config.oslo_messaging_amqp.idle_timeout
        self.trace_protocol = config.oslo_messaging_amqp.trace
        self.ssl_ca_file = config.oslo_messaging_amqp.ssl_ca_file
        self.ssl_cert_file = config.oslo_messaging_amqp.ssl_cert_file
        self.ssl_key_file = config.oslo_messaging_amqp.ssl_key_file
        self.ssl_key_password = config.oslo_messaging_amqp.ssl_key_password
        self.ssl_allow_insecure = \
            config.oslo_messaging_amqp.allow_insecure_clients
        self.sasl_mechanisms = config.oslo_messaging_amqp.sasl_mechanisms
        self.sasl_config_dir = config.oslo_messaging_amqp.sasl_config_dir
        self.sasl_config_name = config.oslo_messaging_amqp.sasl_config_name
        self.hosts = Hosts(hosts, config.oslo_messaging_amqp.username,
                           config.oslo_messaging_amqp.password)
        self.separator = "."
        self.fanout_qualifier = "all"
        self.default_exchange = default_exchange

        # can't handle a request until the replies link is active, as
        # we need the peer assigned address, so need to delay any
        # processing of task queue until this is done
        self._replies = None
        # Set True when the driver is shutting down
        self._closing = False
        # only schedule one outstanding reconnect attempt at a time
        self._reconnecting = False
        self._delay = 0  # seconds between retries
        # prevent queuing up multiple requests to run _process_tasks()
        self._process_tasks_scheduled = False
        self._process_tasks_lock = threading.Lock()

    def connect(self):
        """Connect to the messaging service."""
        self.processor = eventloop.Thread(self._container_name)
        self.processor.wakeup(lambda: self._do_connect())

    def add_task(self, task):
        """Add a Task for execution on processor thread."""
        self._tasks.put(task)
        self._schedule_task_processing()

    def shutdown(self, timeout=None):
        """Shutdown the messaging service."""
        LOG.info(_LI("Shutting down the AMQP 1.0 connection"))
        if self.processor:
            self.processor.wakeup(lambda: self._start_shutdown())
            LOG.debug("Waiting for eventloop to exit")
            self.processor.join(timeout)
            self._hard_reset()
            self.processor.destroy()
            self.processor = None
        LOG.debug("Eventloop exited, driver shut down")

    # The remaining methods are reserved to run from the eventloop thread only!
    # They must not be invoked directly!

    # methods executed by Tasks created by the driver:

    def request(self, target, request, result_queue, reply_expected=False):
        """Send a request message to the given target and arrange for a
        result to be put on the result_queue. If reply_expected, the result
        will include the reply message (if successful).
        """
        address = self._resolve(target)
        LOG.debug("Sending request for %(target)s to %(address)s",
                  {'target': target, 'address': address})
        if reply_expected:
            msg_id = self._replies.prepare_for_response(request, result_queue)

        def _callback(link, handle, state, info):
            if state == pyngus.SenderLink.ACCEPTED:  # message received
                if not reply_expected:
                    # can wake up the sender now
                    result = {"status": "OK"}
                    result_queue.put(result)
                else:
                    # we will wake up the sender when the reply message is
                    # received.  See Replies.message_received()
                    pass
            else:  # send failed/rejected/etc
                msg = "Message send failed: remote disposition: %s, info: %s"
                exc = exceptions.MessageDeliveryFailure(msg % (state, info))
                result = {"status": "ERROR", "error": exc}
                if reply_expected:
                    # no response will be received, so cancel the correlation
                    self._replies.cancel_response(msg_id)
                result_queue.put(result)
        self._send(address, request, _callback)

    def response(self, address, response):
        """Send a response message to the client listening on 'address'.
        To prevent a misbehaving client from blocking a server indefinitely,
        the message is send asynchronously.
        """
        LOG.debug("Sending response to %s", address)
        self._send(address, response)

    def subscribe(self, target, in_queue, subscription_id):
        """Subscribe to messages sent to 'target', place received messages on
        'in_queue'.
        """
        addresses = [
            self._server_address(target),
            self._broadcast_address(target),
            self._group_request_address(target)
        ]
        self._subscribe(target, addresses, in_queue, subscription_id)

    def subscribe_notifications(self, target, in_queue, subscription_id):
        """Subscribe for notifications on 'target', place received messages on
        'in_queue'.
        """
        addresses = [self._group_request_address(target)]
        self._subscribe(target, addresses, in_queue, subscription_id)

    def _subscribe(self, target, addresses, in_queue, subscription_id):
        LOG.debug("Subscribing to %(target)s (%(addresses)s)",
                  {'target': target, 'addresses': addresses})
        server = Server(addresses, in_queue, subscription_id)
        servers = self._servers.get(target)
        if servers is None:
            servers = {}
            self._servers[target] = servers
        servers[subscription_id] = server
        server.attach(self._socket_connection.connection)

    def _resolve(self, target):
        """Return a link address for a given target."""
        if target.server:
            return self._server_address(target)
        elif target.fanout:
            return self._broadcast_address(target)
        else:
            return self._group_request_address(target)

    def _sender(self, address):
        # if we already have a sender for that address, use it
        # else establish the sender and cache it
        sender = self._senders.get(address)
        if sender is None:
            sender = Sender(address)
            sender.attach(self._socket_connection.connection)
            self._senders[address] = sender
        return sender

    def _send(self, addr, message, callback=None, handle=None):
        """Send the message out the link addressed by 'addr'.  If a
        delivery_callback is given it will be invoked when the send has
        completed (whether successfully or in error).
        """
        address = str(addr)
        message.address = address
        self._sender(address).send(message, callback)

    def _server_address(self, target):
        return self._concatenate([self.server_request_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic, target.server])

    def _broadcast_address(self, target):
        return self._concatenate([self.broadcast_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic, self.fanout_qualifier])

    def _group_request_address(self, target):
        return self._concatenate([self.group_request_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic])

    def _concatenate(self, items):
        return self.separator.join(filter(bool, items))

    # commands executed on the processor (eventloop) via 'wakeup()':

    def _do_connect(self):
        """Establish connection and reply subscription on processor thread."""
        host = self.hosts.current
        conn_props = {'hostname': host.hostname}
        if self.idle_timeout:
            conn_props["idle-time-out"] = float(self.idle_timeout)
        if self.trace_protocol:
            conn_props["x-trace-protocol"] = self.trace_protocol
        if self.ssl_ca_file:
            conn_props["x-ssl-ca-file"] = self.ssl_ca_file
        if self.ssl_cert_file:
            # assume this connection is for a server.  If client authentication
            # support is developed, we'll need an explicit flag (server or
            # client)
            conn_props["x-ssl-server"] = True
            conn_props["x-ssl-identity"] = (self.ssl_cert_file,
                                            self.ssl_key_file,
                                            self.ssl_key_password)
            conn_props["x-ssl-allow-cleartext"] = self.ssl_allow_insecure
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
               count < self._max_task_batch and
               self._can_process_tasks):
            try:
                self._tasks.get(False).execute(self)
            except Exception as e:
                LOG.exception(_LE("Error processing task: %s"), e)
            count += 1

        # if we hit _max_task_batch, resume task processing later:
        if not self._tasks.empty() and self._can_process_tasks:
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

    @property
    def _can_process_tasks(self):
        """_process_tasks helper(): indicates that the driver is ready to
        process Tasks.  In order to process messaging-related tasks, the reply
        queue link must be active.
        """
        return (not self._closing and
                self._replies and self._replies.ready())

    def _start_shutdown(self):
        """Called when the application is closing the transport.
        Attempt to cleanly flush/close all links.
        """
        self._closing = True
        if (self._socket_connection
                and self._socket_connection.connection
                and self._socket_connection.connection.active):
            # try a clean shutdown
            for sender in self._senders.values():
                sender.detach()
            for servers in self._servers.values():
                for server in servers.values():
                    server.detach()
            self._replies.detach()
            self._socket_connection.connection.close()
        else:
            # don't wait for a close from the remote, may never happen
            self.processor.shutdown()

    # reply link active callback:

    def _reply_link_ready(self):
        """Invoked when the Replies reply link has become active.  At this
        point, we are ready to send/receive messages (via Task processing).
        """
        LOG.info(_LI("Messaging is active (%(hostname)s:%(port)s)"),
                 {'hostname': self.hosts.current.hostname,
                  'port': self.hosts.current.port})
        self._schedule_task_processing()

    # callback from eventloop on socket error

    def socket_error(self, error):
        """Called by eventloop when a socket error occurs."""
        LOG.error(_LE("Socket failure: %s"), error)
        self._handle_connection_loss()

    # Pyngus connection event callbacks (and their helpers), all invoked from
    # the eventloop thread:

    def connection_failed(self, connection, error):
        """This is a Pyngus callback, invoked by Pyngus when a non-recoverable
        error occurs on the connection.
        """
        if connection is not self._socket_connection.connection:
            # pyngus bug: ignore failure callback on destroyed connections
            return
        LOG.debug("AMQP Connection failure: %s", error)
        self._handle_connection_loss()

    def connection_active(self, connection):
        """This is a Pyngus callback, invoked by Pyngus when the connection to
        the peer is up.  At this point, the driver will activate all subscriber
        links (server) and the reply link.
        """
        LOG.debug("Connection active (%(hostname)s:%(port)s), subscribing...",
                  {'hostname': self.hosts.current.hostname,
                   'port': self.hosts.current.port})
        for servers in self._servers.values():
            for server in servers.values():
                server.attach(self._socket_connection.connection)
        self._replies = Replies(self._socket_connection.connection,
                                lambda: self._reply_link_ready())
        self._delay = 0

    def connection_closed(self, connection):
        """This is a Pyngus callback, invoked by Pyngus when the connection has
        cleanly closed.  This occurs after the driver closes the connection
        locally, and the peer has acknowledged the close.  At this point, the
        shutdown of the driver's connection is complete.
        """
        LOG.debug("AMQP connection closed.")
        # if the driver isn't being shutdown, failover and reconnect
        self._handle_connection_loss()

    def connection_remote_closed(self, connection, reason):
        """This is a Pyngus callback, invoked by Pyngus when the peer has
        requested that the connection be closed.
        """
        # The messaging service/broker is trying to shut down the
        # connection. Acknowledge the close, and try to reconnect/failover
        # later once the connection has closed (connection_closed is called).
        if reason:
            LOG.info(_LI("Connection closed by peer: %s"), reason)
        self._socket_connection.connection.close()

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
        # connection failure will be handled later

    def _handle_connection_loss(self):
        """The connection to the messaging service has been lost.  Try to
        reestablish the connection/failover if not shutting down the driver.
        """
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
                self.processor.schedule(lambda: self._do_reconnect(),
                                        self._delay)
                self._delay = (1 if self._delay == 0
                               else min(self._delay * 2, 60))

    def _do_reconnect(self):
        """Invoked on connection/socket failure, failover and re-connect to the
        messaging service.
        """
        if not self._closing:
            self._hard_reset()
            self._reconnecting = False
            host = self.hosts.next()
            LOG.info(_LI("Reconnecting to: %(hostname)s:%(port)s"),
                     {'hostname': host.hostname, 'port': host.port})
            self._socket_connection.connect(host)

    def _hard_reset(self):
        """Reset the controller to its pre-connection state"""
        # note well: since this method destroys the connection, it cannot be
        # invoked directly from a pyngus callback.  Use processor.schedule() to
        # run this method on the main loop instead.
        for sender in self._senders.values():
            sender.destroy()
        self._senders.clear()
        for servers in self._servers.values():
            for server in servers.values():
                # discard links, but keep servers around to re-attach if
                # failing over
                server.reset()
        if self._replies:
            self._replies.destroy()
            self._replies = None
        if self._socket_connection:
            self._socket_connection.reset()
