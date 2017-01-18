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
A thread that performs all messaging I/O and protocol event handling.

This module provides a background thread that handles messaging operations
scheduled via the Controller, and performs blocking socket I/O and timer
processing.  This thread is designed to be as simple as possible - all the
protocol specific intelligence is provided by the Controller and executed on
the background thread via callables.
"""

import collections
import errno
import heapq
import logging
import math
from monotonic import monotonic as now  # noqa
import os
import select
import socket
import threading
import uuid

import pyngus

from oslo_messaging._i18n import _LE, _LI, _LW
LOG = logging.getLogger(__name__)


def compute_timeout(offset):
    # minimize the timer granularity to one second so we don't have to track
    # too many timers
    return math.ceil(now() + offset)


class _SocketConnection(object):
    """Associates a pyngus Connection with a python network socket,
    and handles all connection-related I/O and timer events.
    """

    def __init__(self, name, container, properties, handler):
        self.name = name
        self.socket = None
        self.pyngus_conn = None
        self._properties = properties
        # The handler is a pyngus ConnectionEventHandler, which is invoked by
        # pyngus on connection-related events (active, closed, error, etc).
        # Currently it is the Controller object.
        self._handler = handler
        self._container = container

    def fileno(self):
        """Allows use of a _SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def read_socket(self):
        """Called to read from the socket."""
        if self.socket:
            try:
                pyngus.read_socket_input(self.pyngus_conn, self.socket)
                self.pyngus_conn.process(now())
            except (socket.timeout, socket.error) as e:
                # pyngus handles EAGAIN/EWOULDBLOCK and EINTER
                self.pyngus_conn.close_input()
                self.pyngus_conn.close_output()
                self._handler.socket_error(str(e))

    def write_socket(self):
        """Called to write to the socket."""
        if self.socket:
            try:
                pyngus.write_socket_output(self.pyngus_conn, self.socket)
                self.pyngus_conn.process(now())
            except (socket.timeout, socket.error) as e:
                # pyngus handles EAGAIN/EWOULDBLOCK and EINTER
                self.pyngus_conn.close_output()
                self.pyngus_conn.close_input()
                self._handler.socket_error(str(e))

    def connect(self, host):
        """Connect to host and start the AMQP protocol."""
        addr = socket.getaddrinfo(host.hostname, host.port, socket.AF_UNSPEC,
                                  socket.SOCK_STREAM)
        if not addr:
            key = "%s:%i" % (host.hostname, host.port)
            error = "Invalid peer address '%s'" % key
            LOG.error(_LE("Invalid peer address '%s'"), key)
            self._handler.socket_error(error)
            return
        my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
        my_socket.setblocking(0)  # 0=non-blocking
        my_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            my_socket.connect(addr[0][4])
        except socket.error as e:
            if e.errno != errno.EINPROGRESS:
                error = "Socket connect failure '%s'" % str(e)
                LOG.error(_LE("Socket connect failure '%s'"), str(e))
                self._handler.socket_error(error)
                return
        self.socket = my_socket

        props = self._properties.copy()
        if pyngus.VERSION >= (2, 0, 0):
            # configure client authentication
            #
            props['x-server'] = False
            if host.username:
                props['x-username'] = host.username
                props['x-password'] = host.password or ""

        self.pyngus_conn = self._container.create_connection(self.name,
                                                             self._handler,
                                                             props)
        self.pyngus_conn.user_context = self

        if pyngus.VERSION < (2, 0, 0):
            # older versions of pyngus requires manual SASL configuration:
            # determine the proper SASL mechanism: PLAIN if a username/password
            # is present, else ANONYMOUS
            pn_sasl = self.pyngus_conn.pn_sasl
            if host.username:
                password = host.password if host.password else ""
                pn_sasl.plain(host.username, password)
            else:
                pn_sasl.mechanisms("ANONYMOUS")
                pn_sasl.client()

        self.pyngus_conn.open()

    def reset(self, name=None):
        """Clean up the current state, expect 'connect()' to be recalled
        later.
        """
        # note well: since destroy() is called on the connection, do not invoke
        # this method from a pyngus callback!
        if self.pyngus_conn:
            self.pyngus_conn.destroy()
            self.pyngus_conn = None
        self.close()
        if name:
            self.name = name

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None


class Scheduler(object):
    """Schedule callables to be run in the future.
    """
    class Event(object):
        # simply hold a reference to a callback that can be set to None if the
        # alarm is canceled
        def __init__(self, callback):
            self.callback = callback

        def cancel(self):
            # quicker than rebalancing the tree
            self.callback = None

    def __init__(self):
        self._callbacks = {}
        self._deadlines = []

    def alarm(self, request, deadline):
        """Request a callable be executed at a specific time
        """
        try:
            callbacks = self._callbacks[deadline]
        except KeyError:
            callbacks = list()
            self._callbacks[deadline] = callbacks
            heapq.heappush(self._deadlines, deadline)
        entry = Scheduler.Event(request)
        callbacks.append(entry)
        return entry

    def defer(self, request, delay):
        """Request a callable be executed after delay seconds
        """
        return self.alarm(request, compute_timeout(delay))

    @property
    def _next_deadline(self):
        """The timestamp of the next expiring event or None
        """
        return self._deadlines[0] if self._deadlines else None

    def _get_delay(self, max_delay=None):
        """Get the delay in milliseconds until the next callable needs to be
        run, or 'max_delay' if no outstanding callables or the delay to the
        next callable is > 'max_delay'.
        """
        due = self._deadlines[0] if self._deadlines else None
        if due is None:
            return max_delay
        _now = now()
        if due <= _now:
            return 0
        else:
            return min(due - _now, max_delay) if max_delay else due - _now

    def _process(self):
        """Invoke all expired callables."""
        if self._deadlines:
            _now = now()
            try:
                while self._deadlines[0] <= _now:
                    deadline = heapq.heappop(self._deadlines)
                    callbacks = self._callbacks[deadline]
                    del self._callbacks[deadline]
                    for cb in callbacks:
                        cb.callback and cb.callback()
            except IndexError:
                pass


class Requests(object):
    """A queue of callables to execute from the eventloop thread's main
    loop.
    """
    def __init__(self):
        self._requests = collections.deque()
        self._wakeup_pipe = os.pipe()
        self._pipe_ready = False  # prevents blocking on an empty pipe
        self._pipe_lock = threading.Lock()

    def wakeup(self, request=None):
        """Enqueue a callable to be executed by the eventloop, and force the
        eventloop thread to wake up from select().
        """
        with self._pipe_lock:
            if request:
                self._requests.append(request)
            if not self._pipe_ready:
                self._pipe_ready = True
                os.write(self._wakeup_pipe[1], b'!')

    def fileno(self):
        """Allows this request queue to be used by select()."""
        return self._wakeup_pipe[0]

    def process_requests(self):
        """Invoked by the eventloop thread, execute each queued callable."""
        with self._pipe_lock:
            if not self._pipe_ready:
                return
            self._pipe_ready = False
            os.read(self._wakeup_pipe[0], 512)
            requests = self._requests
            self._requests = collections.deque()

        for r in requests:
            r()


class Thread(threading.Thread):
    """Manages socket I/O and executes callables queued up by external
    threads.
    """
    def __init__(self, container_name, node, command, pid):
        super(Thread, self).__init__()

        # callables from other threads:
        self._requests = Requests()
        # delayed callables (only used on this thread for now):
        self._scheduler = Scheduler()

        self._connection = None

        # Configure a container
        if container_name is None:
            container_name = ("openstack.org/om/container/%s/%s/%s/%s" %
                              (node, command, pid, uuid.uuid4().hex))
        self._container = pyngus.Container(container_name)

        self.name = "Thread for Proton container: %s" % self._container.name
        self._shutdown = False
        self.daemon = True
        self.start()

    def wakeup(self, request=None):
        """Wake up the eventloop thread, Optionally providing a callable to run
        when the eventloop wakes up.  Thread safe.
        """
        self._requests.wakeup(request)

    def shutdown(self):
        """Shutdown the eventloop thread.  Thread safe.
        """
        LOG.debug("eventloop shutdown requested")
        self._shutdown = True
        self.wakeup()

    def destroy(self):
        # release the container.  This can only be called after the eventloop
        # thread exited
        self._container.destroy()
        self._container = None

    # the following methods are not thread safe - they must be run from the
    # eventloop thread

    def defer(self, request, delay):
        """Invoke request after delay seconds."""
        return self._scheduler.defer(request, delay)

    def alarm(self, request, deadline):
        """Invoke request at a particular time"""
        return self._scheduler.alarm(request, deadline)

    def connect(self, host, handler, properties):
        """Get a _SocketConnection to a peer represented by url."""
        key = "openstack.org/om/connection/%s:%s/" % (host.hostname, host.port)
        # return pre-existing
        conn = self._container.get_connection(key)
        if conn:
            return conn.user_context

        # create a new connection - this will be stored in the
        # container, using the specified name as the lookup key, or if
        # no name was provided, the host:port combination
        sc = _SocketConnection(key, self._container,
                               properties, handler=handler)
        sc.connect(host)
        self._connection = sc
        return sc

    def run(self):
        """Run the proton event/timer loop."""
        LOG.debug("Starting Proton thread, container=%s",
                  self._container.name)

        while not self._shutdown:

            readfds = [self._requests]
            writefds = []
            deadline = self._scheduler._next_deadline

            pyngus_conn = self._connection and self._connection.pyngus_conn
            if pyngus_conn and self._connection.socket:
                if pyngus_conn.needs_input:
                    readfds.append(self._connection)
                if pyngus_conn.has_output:
                    writefds.append(self._connection)
                if pyngus_conn.deadline:
                    deadline = (pyngus_conn.deadline if not deadline else
                                min(deadline, pyngus_conn.deadline))

            # force select to return in time to service the next expiring timer
            if deadline:
                _now = now()
                timeout = 0 if deadline <= _now else (deadline - _now)
            else:
                timeout = None

            # and now we wait...
            try:
                select.select(readfds, writefds, [], timeout)
            except select.error as serror:
                if serror[0] == errno.EINTR:
                    LOG.warning(_LW("ignoring interrupt from select(): %s"),
                                str(serror))
                    continue
                raise  # assuming fatal...

            # Ignore the select return value - simply poll the socket for I/O.
            # Testing shows that polling improves latency over checking the
            # lists returned by select()
            self._requests.process_requests()
            self._connection.read_socket()
            if pyngus_conn and pyngus_conn.deadline:
                _now = now()
                if pyngus_conn.deadline <= _now:
                    pyngus_conn.process(_now)
            self._connection.write_socket()

            self._scheduler._process()  # run any deferred requests

        LOG.info(_LI("eventloop thread exiting, container=%s"),
                 self._container.name)
