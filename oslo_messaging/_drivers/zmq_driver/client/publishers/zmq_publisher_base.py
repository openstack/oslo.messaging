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

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE


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

    def __init__(self, conf, matchmaker):

        """Construct publisher

        Accept configuration object and Name Service interface object.
        Create zmq.Context and connected sockets dictionary.

        :param conf: configuration object
        :type conf: oslo_config.CONF
        :param matchmaker: Name Service interface object
        :type matchmaker: matchmaker.MatchMakerBase
        """

        self.conf = conf
        self.zmq_context = zmq.Context()
        self.matchmaker = matchmaker
        self.outbound_sockets = {}
        super(PublisherBase, self).__init__()

    @abc.abstractmethod
    def send_request(self, request):
        """Send request to consumer

        :param request: Message data and destination container object
        :type request: zmq_request.Request
        """

    def _send_request(self, socket, request):
        """Send request to consumer.
        Helper private method which defines basic sending behavior.

        :param socket: Socket to publish message on
        :type socket: zmq.Socket
        :param request: Message data and destination container object
        :type request: zmq_request.Request
        """
        socket.send_string(request.msg_type, zmq.SNDMORE)
        socket.send_json(request.context, zmq.SNDMORE)
        socket.send_json(request.message)

    def cleanup(self):
        """Cleanup publisher. Close allocated connections."""
        for socket, hosts in self.outbound_sockets.values():
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
