#    Copyright 2015-2016 Mirantis, Inc.
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

import oslo_messaging
from oslo_messaging._drivers.zmq_driver import zmq_async

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class PublisherBase(object):

    """Abstract publisher class

    Each publisher from zmq-driver client should implement
    this interface to serve as a messages publisher.

    Publisher can send request objects from zmq_request.
    """

    def __init__(self, sockets_manager, sender, receiver):

        """Construct publisher.

        Accept sockets manager, sender and receiver objects.

        :param sockets_manager: sockets manager object
        :type sockets_manager: zmq_sockets_manager.SocketsManager
        :param sender: request sender object
        :type sender: zmq_senders.RequestSenderBase
        :param receiver: response receiver object
        :type receiver: zmq_receivers.ReceiverBase
        """
        self.sockets_manager = sockets_manager
        self.conf = sockets_manager.conf
        self.matchmaker = sockets_manager.matchmaker
        self.sender = sender
        self.receiver = receiver

    @abc.abstractmethod
    def acquire_connection(self, request):
        """Get socket to publish request on it.

        :param request: request object
        :type senders: zmq_request.Request
        """

    @abc.abstractmethod
    def send_request(self, socket, request):
        """Publish request on a socket.

        :param socket: socket object to publish request on
        :type socket: zmq_socket.ZmqSocket
        :param request: request object
        :type senders: zmq_request.Request
        """

    @abc.abstractmethod
    def receive_reply(self, socket, request):
        """Wait for a reply via the socket used for sending the request.

        :param socket: socket object to receive reply from
        :type socket: zmq_socket.ZmqSocket
        :param request: request object
        :type senders: zmq_request.Request
        """

    @staticmethod
    def _raise_timeout(request):
        raise oslo_messaging.MessagingTimeout(
            "Timeout %(tout)s seconds was reached for message %(msg_id)s" %
            {"tout": request.timeout, "msg_id": request.message_id})

    def cleanup(self):
        """Cleanup publisher: stop receiving responses, close allocated
        connections etc.
        """
        self.receiver.stop()
