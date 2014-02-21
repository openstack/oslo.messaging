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
import threading
import uuid

import pyngus
from six import moves

from oslo.config import cfg
from oslo.messaging._drivers.protocols.amqp import eventloop

LOG = logging.getLogger(__name__)

_amqp1_opts = [
    cfg.StrOpt('server_request_prefix',
               default='exclusive',
               help="address prefix used when sending to a specific server"),

    cfg.StrOpt('broadcast_prefix',
               default='broadcast',
               help="address prefix used when broadcasting to all servers"),

    cfg.StrOpt('group_request_prefix',
               default='unicast',
               help="address prefix when sending to any server in group"),

    cfg.StrOpt('container_name',
               default=None,
               help='Name for the AMQP container'),

    cfg.IntOpt('idle_timeout',
               default=0,  # disabled
               help='Timeout for inactive connections (in seconds)'),

    cfg.BoolOpt('trace',
                default=False,
                help='Debug: dump AMQP frames to stdout'),

    cfg.StrOpt('ssl_ca_file',
               default='',
               help="CA certificate PEM file for verifing server certificate"),

    cfg.StrOpt('ssl_cert_file',
               default='',
               help='Identifying certificate PEM file to present to clients'),

    cfg.StrOpt('ssl_key_file',
               default='',
               help='Private key PEM file used to sign cert_file certificate'),

    cfg.StrOpt('ssl_key_password',
               default=None,
               help='Password for decrypting ssl_key_file (if encrypted)'),

    cfg.BoolOpt('allow_insecure_clients',
                default=False,
                help='Accept clients using either SSL or plain TCP')
]


class Task(object):
    """Perform a messaging operation via the Controller."""
    @abc.abstractmethod
    def execute(self, controller):
        """This method will be run on the eventloop thread."""


class Replies(pyngus.ReceiverEventHandler):
    """This is the receiving link for all reply messages.  Messages are routed
    to the proper Listener's incoming queue using the correlation-id header in
    the message.
    """
    def __init__(self, connection, on_ready):
        self._correlation = {}  # map of correlation-id to response queue
        self._ready = False
        self._on_ready = on_ready
        self._receiver = connection.create_receiver("replies",
                                                    event_handler=self)
        # capacity determines the maximum number of reply messages this link
        # can receive. As messages are received and credit is consumed, this
        # driver will 'top up' the credit back to max capacity.  This number
        # should be large enough to avoid needlessly flow-controlling the
        # replies.
        self.capacity = 100  # TODO(kgiusti) guesstimate - make configurable
        self._credit = 0
        self._receiver.open()

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
        LOG.debug("Reply for msg id=%s expected on link %s",
                  request.id, request.reply_to)

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
        # TODO(kgiusti) Unclear if this error will ever occur (as opposed to
        # the Connection failing instead).  Log for now, possibly implement a
        # recovery strategy if necessary.
        LOG.error("Reply subscription closed by peer: %s",
                  (pn_condition or "no error given"))

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        self._credit = self._credit - 1
        self._update_credit()

        key = message.correlation_id
        if key in self._correlation:
            LOG.debug("Received response for msg id=%s", key)
            self._correlation[key].put(message)
            # cleanup (only need one response per request)
            del self._correlation[key]
        else:
            LOG.warn("Can't find receiver for response msg id=%s, dropping!",
                     key)
        receiver.message_accepted(handle)

    def _update_credit(self):
        if self.capacity > self._credit:
            self._receiver.add_capacity(self.capacity - self._credit)
            self._credit = self.capacity


class Server(pyngus.ReceiverEventHandler):
    """A group of links that receive messages from a set of addresses derived
    from a given target.  Messages arriving on the links are placed on the
    'incoming' queue.
    """
    def __init__(self, addresses, incoming):
        self._incoming = incoming
        self._addresses = addresses

    def attach(self, connection):
        """Create receiver links over the given connection for all the
        configured addresses.
        """
        self._receivers = []
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
            r.add_capacity(500)
            r.open()
            self._receivers.append(r)

    # Pyngus ReceiverLink event callbacks:

    def receiver_remote_closed(self, receiver, pn_condition):
        """This is a Pyngus callback, invoked by Pyngus when the peer of this
        receiver link has initiated closing the connection.
        """
        text = "Server subscription %(addr)s closed by peer: %(err_msg)s"
        vals = {
            "addr": receiver.source_address or receiver.target_address,
            "err_msg": pn_condition or "no error given"
        }
        LOG.error(text % vals)

    def message_received(self, receiver, message, handle):
        """This is a Pyngus callback, invoked by Pyngus when a new message
        arrives on this receiver link from the peer.
        """
        # TODO(kgiusti) Sub-optimal to grant one credit each time a message
        # arrives.  A better approach would grant batches of credit on demand.
        receiver.add_capacity(1)
        self._incoming.put(message)
        LOG.debug("message received: %s", message)
        receiver.message_accepted(handle)


class Hosts(object):
    """An order list of peer addresses.  Connection failover progresses from
    one host to the next.
    """
    HostnamePort = collections.namedtuple('HostnamePort',
                                          ['hostname', 'port'])

    def __init__(self, entries=None):
        self._entries = [self.HostnamePort(h, p) for h, p in entries or []]
        self._current = 0

    def add(self, hostname, port=5672):
        self._entries.append(self.HostnamePort(hostname, port))

    @property
    def current(self):
        if len(self._entries):
            return self._entries[self._current]
        else:
            return self.HostnamePort("localhost", 5672)

    def next(self):
        if len(self._entries) > 1:
            self._current = (self._current + 1) % len(self._entries)
        return self.current

    def __repr__(self):
        return '<Hosts ' + str(self) + '>'

    def __str__(self):
        return ", ".join(["%s:%i" % e for e in self._entries])


class Controller(pyngus.ConnectionEventHandler):
    """Controls the connection to the AMQP messaging service.  This object is
    the 'brains' of the driver.  It maintains the logic for addressing, sending
    and receiving messages, and managing the connection.  All messaging and I/O
    work is done on the Eventloop thread, allowing the driver to run
    asynchronously from the messaging clients.
    """
    def __init__(self, hosts, default_exchange, config):
        self.processor = None
        # queue of Task() objects to execute on the eventloop once the
        # connection is ready:
        self._tasks = moves.queue.Queue(maxsize=500)
        # limit the number of Task()'s to execute per call to _process_tasks().
        # This allows the eventloop main thread to return to servicing socket
        # I/O in a timely manner
        self._max_task_batch = 50
        # cache of sending links indexed by address:
        self._senders = {}
        # Servers (set of receiving links), indexed by target:
        self._servers = {}
        self.hosts = Hosts(hosts)

        opt_group = cfg.OptGroup(name='amqp1',
                                 title='AMQP 1.0 options')
        config.register_group(opt_group)
        config.register_opts(_amqp1_opts, group=opt_group)

        self.server_request_prefix = config.amqp1.server_request_prefix
        self.broadcast_prefix = config.amqp1.broadcast_prefix
        self.group_request_prefix = config.amqp1.group_request_prefix
        self._container_name = config.amqp1.container_name
        if not self._container_name:
            self._container_name = "container-%s" % uuid.uuid4().hex
        self.idle_timeout = config.amqp1.idle_timeout
        self.trace_protocol = config.amqp1.trace
        self.ssl_ca_file = config.amqp1.ssl_ca_file
        self.ssl_cert_file = config.amqp1.ssl_cert_file
        self.ssl_key_file = config.amqp1.ssl_key_file
        self.ssl_key_password = config.amqp1.ssl_key_password
        self.ssl_allow_insecure = config.amqp1.allow_insecure_clients
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

    def destroy(self):
        """Shutdown the messaging service."""
        if self.processor:
            self.processor.wakeup(lambda: self._start_shutdown())
            LOG.info("Waiting for eventloop to exit")
            self.processor.join()
            self.processor = None
        LOG.info("Eventloop exited, driver shut down")

    # The remaining methods are reserved to run from the eventloop thread only!
    # They must not be invoked directly!

    # methods executed by Tasks created by the driver:

    def request(self, target, request, reply_queue=None):
        """Send a request message to the given target, and arrange for a
        response to be put on the optional reply_queue if specified
        """
        address = self._resolve(target)
        LOG.debug("Sending request for %s to %s", target, address)
        if reply_queue is not None:
            self._replies.prepare_for_response(request, reply_queue)
        self._send(address, request)

    def response(self, address, response):
        LOG.debug("Sending response to %s", address)
        self._send(address, response)

    def subscribe(self, target, in_queue):
        """Subscribe to messages sent to 'target', place received messages on
        'in_queue'.
        """
        addresses = [
            self._server_address(target),
            self._broadcast_address(target),
            self._group_request_address(target)
        ]
        self._subscribe(target, addresses, in_queue)

    def subscribe_notifications(self, target, in_queue):
        """Subscribe for notifications on 'target', place received messages on
        'in_queue'.
        """
        addresses = [self._group_request_address(target)]
        self._subscribe(target, addresses, in_queue)

    def _subscribe(self, target, addresses, in_queue):
        LOG.debug("Subscribing to %s (%s)", target, addresses)
        self._servers[target] = Server(addresses, in_queue)
        self._servers[target].attach(self._socket_connection.connection)

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
        if address in self._senders:
            sender = self._senders[address]
        else:
            sname = "Producer-%s:src=%s:tgt=%s" % (uuid.uuid4().hex,
                                                   address, address)
            conn = self._socket_connection.connection
            sender = conn.create_sender(source_address=address,
                                        target_address=address,
                                        name=sname)
            sender.open()
            self._senders[address] = sender
        return sender

    def _send(self, addr, message):
        """Send the message out the link addressed by 'addr'."""
        address = str(addr)
        message.address = address
        self._sender(address).send(message)

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
        hostname = self.hosts.current.hostname
        port = self.hosts.current.port
        conn_props = {}
        if self.idle_timeout:
            conn_props["idle-time-out"] = float(self.idle_timeout)
        if self.trace_protocol:
            conn_props["x-trace-protocol"] = self.trace_protocol
        if self.ssl_ca_file:
            conn_props["x-ssl-ca-file"] = self.ssl_ca_file
        if self.ssl_cert_file:
            # assume this connection is for a server.  If client authentication
            # support is developed, we'll need an explict flag (server or
            # client)
            conn_props["x-ssl-server"] = True
            conn_props["x-ssl-identity"] = (self.ssl_cert_file,
                                            self.ssl_key_file,
                                            self.ssl_key_password)
            conn_props["x-ssl-allow-cleartext"] = self.ssl_allow_insecure
        self._socket_connection = self.processor.connect(hostname, port,
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
                LOG.exception("Error processing task: %s", e)
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
        """Called when the driver destroys the controller, this method attempts
        to cleanly close the AMQP connection to the peer.
        """
        LOG.info("Shutting down AMQP connection")
        self._closing = True
        if self._socket_connection.connection.active:
            # try a clean shutdown
            self._socket_connection.connection.close()
        else:
            # don't wait for a close from the remote, may never happen
            self._complete_shutdown()

    # reply link active callback:

    def _reply_link_ready(self):
        """Invoked when the Replies reply link has become active.  At this
        point, we are ready to send/receive messages (via Task processing).
        """
        LOG.info("Messaging is active (%s:%i)", self.hosts.current.hostname,
                 self.hosts.current.port)
        self._schedule_task_processing()

    # callback from eventloop on socket error

    def socket_error(self, error):
        """Called by eventloop when a socket error occurs."""
        LOG.debug("Socket failure: %s", error)
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
        LOG.debug("Connection active (%s:%i), subscribing...",
                  self.hosts.current.hostname, self.hosts.current.port)
        for s in self._servers.itervalues():
            s.attach(self._socket_connection.connection)
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
        if not self._closing:
            # The messaging service/broker is trying to shut down the
            # connection. Acknowledge the close, and try to reconnect/failover
            # later once the connection has closed (connection_closed is
            # called).
            LOG.info("Connection closed by peer: %s",
                     reason or "no reason given")
            self._socket_connection.connection.close()

    def _complete_shutdown(self):
        """The AMQP Connection has closed, and the driver shutdown is complete.
        Clean up controller resources and exit.
        """
        self._socket_connection.close()
        self.processor.shutdown()
        LOG.info("Messaging has shutdown")

    def _handle_connection_loss(self):
        """The connection to the messaging service has been lost.  Try to
        reestablish the connection/failover.
        """
        if self._closing:
            # we're in the middle of shutting down the driver anyways,
            # just consider it done:
            self._complete_shutdown()
        else:
            # for some reason, we've lost the connection to the messaging
            # service.  Try to re-establish the connection:
            if not self._reconnecting:
                self._reconnecting = True
                self._replies = None
                if self._delay == 0:
                    self._delay = 1
                    self._do_reconnect()
                else:
                    d = self._delay
                    LOG.info("delaying reconnect attempt for %d seconds", d)
                    self.processor.schedule(lambda: self._do_reconnect(), d)
                    self._delay = min(d * 2, 60)

    def _do_reconnect(self):
        """Invoked on connection/socket failure, failover and re-connect to the
        messaging service.
        """
        if not self._closing:
            self._reconnecting = False
            self._senders = {}
            self._socket_connection.reset()
            hostname, port = self.hosts.next()
            LOG.info("Reconnecting to: %s:%i", hostname, port)
            self._socket_connection.connect(hostname, port)
