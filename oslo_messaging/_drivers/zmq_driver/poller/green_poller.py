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

import threading

import eventlet

from oslo_messaging._drivers.zmq_driver import zmq_poller


class GreenPoller(zmq_poller.ZmqPoller):

    def __init__(self):
        self.incoming_queue = eventlet.queue.LightQueue()
        self.green_pool = eventlet.GreenPool()
        self.thread_by_socket = {}

    def register(self, socket, recv_method=None):
        if socket not in self.thread_by_socket:
            self.thread_by_socket[socket] = self.green_pool.spawn(
                self._socket_receive, socket, recv_method)

    def _socket_receive(self, socket, recv_method=None):
        while True:
            if recv_method:
                incoming = recv_method(socket)
            else:
                incoming = socket.recv_multipart()
            self.incoming_queue.put((incoming, socket))
            eventlet.sleep()

    def poll(self, timeout=None):
        try:
            return self.incoming_queue.get(timeout=timeout)
        except eventlet.queue.Empty:
            return None, None

    def close(self):
        for thread in self.thread_by_socket.values():
            thread.kill()

        self.thread_by_socket = {}


class GreenExecutor(zmq_poller.Executor):

    def __init__(self, method):
        self._method = method
        super(GreenExecutor, self).__init__(None)
        self._done = threading.Event()

    def _loop(self):
        while not self._done.is_set():
            self._method()
            eventlet.sleep()

    def execute(self):
        self.thread = eventlet.spawn(self._loop)

    def wait(self):
        if self.thread is not None:
            self.thread.wait()

    def stop(self):
        if self.thread is not None:
            self.thread.kill()

    def done(self):
        self._done.set()
