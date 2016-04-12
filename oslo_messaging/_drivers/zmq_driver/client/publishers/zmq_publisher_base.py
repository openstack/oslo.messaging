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
import time

import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
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

    def __init__(self, sockets_manager):

        """Construct publisher

        Accept configuration object and Name Service interface object.
        Create zmq.Context and connected sockets dictionary.

        :param conf: configuration object
        :type conf: oslo_config.CONF
        """
        self.outbound_sockets = sockets_manager
        self.conf = sockets_manager.conf
        self.matchmaker = sockets_manager.matchmaker
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
        LOG.debug("Sending %(type)s message_id %(message)s to a target "
                  "%(target)s",
                  {"type": request.msg_type,
                   "message": request.message_id,
                   "target": request.target})
        socket.send_pyobj(request)

    def cleanup(self):
        """Cleanup publisher. Close allocated connections."""
        self.outbound_sockets.cleanup()


class SocketsManager(object):

    def __init__(self, conf, matchmaker, listener_type, socket_type):
        self.conf = conf
        self.matchmaker = matchmaker
        self.listener_type = listener_type
        self.socket_type = socket_type
        self.zmq_context = zmq.Context()
        self.outbound_sockets = {}

    def _track_socket(self, socket, target):
        self.outbound_sockets[str(target)] = (socket, time.time())

    def get_hosts(self, target):
        return self.matchmaker.get_hosts(
            target, zmq_names.socket_type_str(self.listener_type))

    def _get_hosts_and_connect(self, socket, target):
        hosts = self.get_hosts(target)
        self._connect_to_hosts(socket, target, hosts)

    def _connect_to_hosts(self, socket, target, hosts):
        for host in hosts:
            socket.connect_to_host(host)
        self._track_socket(socket, target)

    def _check_for_new_hosts(self, target):
        socket, tm = self.outbound_sockets[str(target)]
        if 0 <= self.conf.zmq_target_expire <= time.time() - tm:
            self._get_hosts_and_connect(socket, target)
        return socket

    def get_socket(self, target):
        if str(target) in self.outbound_sockets:
            socket = self._check_for_new_hosts(target)
        else:
            socket = zmq_socket.ZmqSocket(self.conf, self.zmq_context,
                                          self.socket_type)
            self._get_hosts_and_connect(socket, target)
        return socket

    def get_socket_to_hosts(self, target, hosts):
        key = str(target)
        if key in self.outbound_sockets:
            socket, tm = self.outbound_sockets[key]
        else:
            socket = zmq_socket.ZmqSocket(self.conf, self.zmq_context,
                                          self.socket_type)
            self._connect_to_hosts(socket, target, hosts)
        return socket

    def get_socket_to_publishers(self):
        socket = zmq_socket.ZmqSocket(self.conf, self.zmq_context,
                                      self.socket_type)
        publishers = self.matchmaker.get_publishers()
        for pub_address, router_address in publishers:
            socket.connect_to_host(router_address)
        return socket

    def get_socket_to_routers(self):
        socket = zmq_socket.ZmqSocket(self.conf, self.zmq_context,
                                      self.socket_type)
        routers = self.matchmaker.get_routers()
        for router_address in routers:
            socket.connect_to_host(router_address)
        return socket

    def cleanup(self):
        for socket, tm in self.outbound_sockets.values():
            socket.close()


class QueuedSender(PublisherBase):

    def __init__(self, sockets_manager, _do_send_request):
        super(QueuedSender, self).__init__(sockets_manager)
        self._do_send_request = _do_send_request
        self.queue, self.empty_except = zmq_async.get_queue()
        self.executor = zmq_async.get_executor(self.run_loop)
        self.executor.execute()

    def send_request(self, request):
        self.queue.put(request)

    def _connect_socket(self, target):
        return self.outbound_sockets.get_socket(target)

    def run_loop(self):
        try:
            request = self.queue.get(timeout=self.conf.rpc_poll_timeout)
        except self.empty_except:
            return

        socket = self._connect_socket(request.target)
        self._do_send_request(socket, request)

    def cleanup(self):
        self.executor.stop()
        super(QueuedSender, self).cleanup()
