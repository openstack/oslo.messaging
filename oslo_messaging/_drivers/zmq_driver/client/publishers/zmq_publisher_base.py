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
import logging

import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class UnsupportedSendPattern(rpc_common.RPCException):

    """Exception to raise from publishers in case of unsupported
    sending pattern called.
    """

    def __init__(self, pattern_name):
        """Construct exception object

        :param pattern_name: Message type name from zmq_names
        :type pattern_name: str
        """
        errmsg = _LE("Sending pattern %s is unsupported.") % pattern_name
        super(UnsupportedSendPattern, self).__init__(errmsg)


@six.add_metaclass(abc.ABCMeta)
class PublisherBase(object):

    """Abstract publisher class

    Each publisher from zmq-driver client should implement
    this interface to serve as a messages publisher.

    Publisher can send request objects from zmq_request.
    """

    def __init__(self, sockets_manager, sender, receiver):

        """Construct publisher

        Accept sockets manager, sender and receiver objects.

        :param sockets_manager: sockets manager object
        :type sockets_manager: zmq_sockets_manager.SocketsManager
        :param senders: request sender object
        :type senders: zmq_senders.RequestSender
        :param receiver: reply receiver object
        :type receiver: zmq_receivers.ReplyReceiver
        """
        self.sockets_manager = sockets_manager
        self.conf = sockets_manager.conf
        self.matchmaker = sockets_manager.matchmaker
        self.sender = sender
        self.receiver = receiver

    @abc.abstractmethod
    def connect_socket(self, request):
        """Get connected socket ready for sending given request
        or None otherwise (i.e. if connection can't be established).
        """

    @abc.abstractmethod
    def send_call(self, request):
        pass

    @abc.abstractmethod
    def send_cast(self, request):
        pass

    @abc.abstractmethod
    def send_fanout(self, request):
        pass

    @abc.abstractmethod
    def send_notify(self, request):
        pass

    def cleanup(self):
        """Cleanup publisher. Close allocated connections."""
        self.receiver.stop()
        self.sockets_manager.cleanup()
