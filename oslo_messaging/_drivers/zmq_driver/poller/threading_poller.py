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

import zmq

from oslo_messaging._drivers.zmq_driver import zmq_poller

LOG = logging.getLogger(__name__)


class ThreadingPoller(zmq_poller.ZmqPoller):

    def __init__(self):
        self.poller = zmq.Poller()

    def register(self, socket):
        self.poller.register(socket, zmq.POLLOUT)

    def poll(self, timeout=None):
        socks = dict(self.poller.poll(timeout))
        for socket in socks:
            incoming = socket.recv()
            return incoming


class ThreadingExecutor(zmq_poller.Executor):

    def __init__(self, method):
        thread = threading.Thread(target=method)
        super(ThreadingExecutor, self).__init__(thread)

    def execute(self):
        self.thread.start()

    def wait(self):
        self.thread.join()
