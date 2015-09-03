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

import errno
import heapq
import logging
import os
import select
import socket
import sys
import threading
import time
import uuid

import pyngus
from six import moves

LOG = logging.getLogger(__name__)


class _SocketConnection(object):
    """Associates a pyngus Connection with a python network socket,
    and handles all connection-related I/O and timer events.
    """

    def __init__(self, name, container, properties, handler):
        self.name = name
        self.socket = None
        self._properties = properties or {}
        self._properties["properties"] = self._get_name_and_pid()
        # The handler is a pyngus ConnectionEventHandler, which is invoked by
        # pyngus on connection-related events (active, closed, error, etc).
        # Currently it is the Controller object.
        self._handler = handler
        self._container = container
        self.connection = None

    def _get_name_and_pid(self):
        # helps identify the process that is using the connection
        return {u'process': os.path.basename(sys.argv[0]), u'pid': os.getpid()}

    def fileno(self):
        """Allows use of a _SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def read(self):
        """Called when socket is read-ready."""
        while True:
            try:
                rc = pyngus.read_socket_input(self.connection, self.socket)
                self.connection.process(time.time())
                return rc
            except (socket.timeout, socket.error) as e:
                # pyngus handles EAGAIN/EWOULDBLOCK and EINTER
                self.connection.close_input()
                self.connection.close()
                self._handler.socket_error(str(e))
                return pyngus.Connection.EOS

    def write(self):
        """Called when socket is write-ready."""
        while True:
            try:
                rc = pyngus.write_socket_output(self.connection, self.socket)
                self.connection.process(time.time())
                return rc
            except (socket.timeout, socket.error) as e:
                # pyngus handles EAGAIN/EWOULDBLOCK and EINTER
                self.connection.close_output()
                self.connection.close()
                self._handler.socket_error(str(e))
                return pyngus.Connection.EOS

    def connect(self, host):
        """Connect to host and start the AMQP protocol."""
        addr = socket.getaddrinfo(host.hostname, host.port,
                                  socket.AF_INET, socket.SOCK_STREAM)
        if not addr:
            key = "%s:%i" % (host.hostname, host.port)
            error = "Invalid peer address '%s'" % key
            LOG.error(error)
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
                LOG.error(error)
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

        c = self._container.create_connection(self.name, self._handler, props)
        c.user_context = self
        self.connection = c

        if pyngus.VERSION < (2, 0, 0):
            # older versions of pyngus requires manual SASL configuration:
            # determine the proper SASL mechanism: PLAIN if a username/password
            # is present, else ANONYMOUS
            pn_sasl = self.connection.pn_sasl
            if host.username:
                password = host.password if host.password else ""
                pn_sasl.plain(host.username, password)
            else:
                pn_sasl.mechanisms("ANONYMOUS")
                # TODO(kgiusti): server if accepting inbound connections
                pn_sasl.client()

        self.connection.open()

    def reset(self, name=None):
        """Clean up the current state, expect 'connect()' to be recalled
        later.
        """
        # note well: since destroy() is called on the connection, do not invoke
        # this method from a pyngus callback!
        if self.connection:
            self.connection.destroy()
            self.connection = None
        self.close()
        if name:
            self.name = name

    def close(self):
        if self.socket:
            self.socket.close()


class Schedule(object):
    """A list of callables (requests). Each callable may have a delay (in
    milliseconds) which causes the callable to be scheduled to run after the
    delay passes.
    """
    def __init__(self):
        self._entries = []

    def schedule(self, request, delay):
        """Request a callable be executed after delay."""
        entry = (time.time() + delay, request)
        heapq.heappush(self._entries, entry)

    def get_delay(self, max_delay=None):
        """Get the delay in milliseconds until the next callable needs to be
        run, or 'max_delay' if no outstanding callables or the delay to the
        next callable is > 'max_delay'.
        """
        due = self._entries[0][0] if self._entries else None
        if due is None:
            return max_delay
        now = time.time()
        if due < now:
            return 0
        else:
            return min(due - now, max_delay) if max_delay else due - now

    def process(self):
        """Invoke all expired callables."""
        while self._entries and self._entries[0][0] < time.time():
            heapq.heappop(self._entries)[1]()


class Requests(object):
    """A queue of callables to execute from the eventloop thread's main
    loop.
    """
    def __init__(self):
        self._requests = moves.queue.Queue(maxsize=10)
        self._wakeup_pipe = os.pipe()

    def wakeup(self, request=None):
        """Enqueue a callable to be executed by the eventloop, and force the
        eventloop thread to wake up from select().
        """
        if request:
            self._requests.put(request)
        os.write(self._wakeup_pipe[1], "!")

    def fileno(self):
        """Allows this request queue to be used by select()."""
        return self._wakeup_pipe[0]

    def read(self):
        """Invoked by the eventloop thread, execute each queued callable."""
        os.read(self._wakeup_pipe[0], 512)
        # first pop of all current tasks
        requests = []
        while not self._requests.empty():
            requests.append(self._requests.get())
        # then process them, this allows callables to re-register themselves to
        # be run on the next iteration of the I/O loop
        for r in requests:
            r()


class Thread(threading.Thread):
    """Manages socket I/O and executes callables queued up by external
    threads.
    """
    def __init__(self, container_name=None):
        super(Thread, self).__init__()

        # callables from other threads:
        self._requests = Requests()
        # delayed callables (only used on this thread for now):
        self._schedule = Schedule()

        # Configure a container
        if container_name is None:
            container_name = "Container-" + uuid.uuid4().hex
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

    def shutdown(self, wait=True, timeout=None):
        """Shutdown the eventloop thread.  Thread safe.
        """
        LOG.debug("eventloop shutdown requested")
        self._shutdown = True
        self.wakeup()
        if wait:
            self.join(timeout=timeout)
        LOG.debug("eventloop shutdown complete")

    # the following methods are not thread safe - they must be run from the
    # eventloop thread

    def schedule(self, request, delay):
        """Invoke request after delay seconds."""
        self._schedule.schedule(request, delay)

    def connect(self, host, handler, properties=None, name=None):
        """Get a _SocketConnection to a peer represented by url."""
        key = name or "%s:%i" % (host.hostname, host.port)
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
        return sc

    def run(self):
        """Run the proton event/timer loop."""
        LOG.debug("Starting Proton thread, container=%s",
                  self._container.name)

        while not self._shutdown:
            readers, writers, timers = self._container.need_processing()

            readfds = [c.user_context for c in readers]
            # additionally, always check for readability of pipe we
            # are using to wakeup processing thread by other threads
            readfds.append(self._requests)
            writefds = [c.user_context for c in writers]

            timeout = None
            if timers:
                deadline = timers[0].deadline  # 0 == next expiring timer
                now = time.time()
                timeout = 0 if deadline <= now else deadline - now

            # adjust timeout for any deferred requests
            timeout = self._schedule.get_delay(timeout)

            try:
                results = select.select(readfds, writefds, [], timeout)
            except select.error as serror:
                if serror[0] == errno.EINTR:
                    LOG.warning("ignoring interrupt from select(): %s",
                                str(serror))
                    continue
                raise  # assuming fatal...

            # don't process any I/O or timers if woken up by a shutdown:
            # if we've been forked we don't want to do I/O on the parent's
            # sockets
            if self._shutdown:
                break

            readable, writable, ignore = results

            for r in readable:
                r.read()

            for t in timers:
                if t.deadline > time.time():
                    break
                t.process(time.time())

            for w in writable:
                w.write()

            self._schedule.process()  # run any deferred requests

        LOG.info("eventloop thread exiting, container=%s",
                 self._container.name)
        self._container.destroy()
