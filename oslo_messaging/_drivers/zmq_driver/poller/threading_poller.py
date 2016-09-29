#    Copyright 2015 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_poller

zmq = zmq_async.import_zmq()

LOG = logging.getLogger(__name__)


class ThreadingPoller(zmq_poller.ZmqPoller):

    def __init__(self):
        self.poller = zmq.Poller()
        self.sockets_and_recv_methods = {}

    def register(self, socket, recv_method=None):
        socket_handle = socket.handle
        if socket_handle in self.sockets_and_recv_methods:
            return
        LOG.debug("Registering socket %s", socket_handle.identity)
        self.sockets_and_recv_methods[socket_handle] = (socket, recv_method)
        self.poller.register(socket_handle, zmq.POLLIN)

    def unregister(self, socket):
        socket_handle = socket.handle
        socket_and_recv_method = \
            self.sockets_and_recv_methods.pop(socket_handle, None)
        if socket_and_recv_method:
            LOG.debug("Unregistering socket %s", socket_handle.identity)
            self.poller.unregister(socket_handle)

    def poll(self, timeout=None):
        if timeout is not None and timeout > 0:
            timeout *= 1000  # convert seconds to milliseconds

        socket_handles = {}
        try:
            socket_handles = dict(self.poller.poll(timeout=timeout))
        except zmq.ZMQError as e:
            LOG.debug("Polling terminated with error: %s", e)

        if not socket_handles:
            return None, None
        for socket_handle in socket_handles:
            socket, recv_method = self.sockets_and_recv_methods[socket_handle]
            if recv_method:
                return recv_method(socket), socket
            else:
                return socket.recv_multipart(), socket

    def close(self):
        pass  # Nothing to do for threading poller


class ThreadingExecutor(zmq_poller.Executor):

    def __init__(self, method):
        self._method = method
        thread = threading.Thread(target=self._loop)
        thread.daemon = True
        super(ThreadingExecutor, self).__init__(thread)
        self._stop = threading.Event()

    def _loop(self):
        while not self._stop.is_set():
            self._method()

    def execute(self):
        self.thread.start()

    def stop(self):
        self._stop.set()
