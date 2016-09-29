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

import abc

import six


@six.add_metaclass(abc.ABCMeta)
class ZmqPoller(object):

    """Base poller interface

    Needed to poll on zmq sockets in green and native async manner.
    Native poller implementation wraps zmq.Poller helper class.
    Wrapping is needed to provide unified poller interface
    in zmq-driver (for both native and zmq pollers). It makes some
    difference with poller-helper from zmq library which doesn't actually
    receive message.

    The poller object should be obtained over:

        poller = zmq_async.get_poller()

    Then we have to register sockets for polling. We are able
    to provide specific receiving method. By default poller calls
    socket.recv_multipart.

        def receive_message(socket):
            id = socket.recv_string()
            ctxt = socket.recv_json()
            msg = socket.recv_json()
            return (id, ctxt, msg)

        poller.register(socket, recv_method=receive_message)

    Further to receive a message we should call:

        message, socket = poller.poll()

    The 'message' here contains (id, ctxt, msg) tuple.
    """

    @abc.abstractmethod
    def register(self, socket, recv_method=None):
        """Register socket to poll

        :param socket: Socket to subscribe for polling
        :type socket: ZmqSocket
        :param recv_method: Optional specific receiver procedure
                            Should return received message object
        :type recv_method: callable
        """
    @abc.abstractmethod
    def unregister(self, socket):
        """Unregister socket from poll

        :param socket: Socket to unsubscribe from polling
        :type socket: ZmqSocket
        """

    @abc.abstractmethod
    def poll(self, timeout=None):
        """Poll for messages

        :param timeout: Optional polling timeout
                        None or -1 means poll forever
                        any positive value means timeout in seconds
        :type timeout: int
        :returns: (message, socket) tuple
        """

    @abc.abstractmethod
    def close(self):
        """Terminate polling"""


@six.add_metaclass(abc.ABCMeta)
class Executor(object):
    """Base executor interface for threading/green async executors"""

    def __init__(self, thread):
        self.thread = thread

    @abc.abstractmethod
    def execute(self):
        """Run execution"""

    @abc.abstractmethod
    def stop(self):
        """Stop execution"""
