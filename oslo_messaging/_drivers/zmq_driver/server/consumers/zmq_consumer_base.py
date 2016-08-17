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
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class ConsumerBase(object):

    def __init__(self, conf, poller, server):
        self.conf = conf
        self.poller = poller
        self.server = server
        self.sockets = []
        self.context = zmq.Context()

    def stop(self):
        """Stop consumer polling/updates"""
        pass

    @abc.abstractmethod
    def receive_message(self, target):
        """Method for poller - receiving message routine"""

    def cleanup(self):
        for socket in self.sockets:
            if not socket.handle.closed:
                socket.close()
        self.sockets = []


class SingleSocketConsumer(ConsumerBase):

    def __init__(self, conf, poller, server, socket_type):
        super(SingleSocketConsumer, self).__init__(conf, poller, server)
        self.matchmaker = server.matchmaker
        self.target = server.target
        self.socket_type = socket_type
        self.host = None
        self.socket = self.subscribe_socket(socket_type)
        self.target_updater = TargetUpdater(
            conf, self.matchmaker, self.target, self.host, socket_type)

    def stop(self):
        self.target_updater.stop()

    def subscribe_socket(self, socket_type):
        try:
            socket = zmq_socket.ZmqRandomPortSocket(
                self.conf, self.context, socket_type)
            self.sockets.append(socket)
            LOG.debug("Run %(stype)s consumer on %(addr)s:%(port)d",
                      {"stype": zmq_names.socket_type_str(socket_type),
                       "addr": socket.bind_address,
                       "port": socket.port})
            self.host = zmq_address.combine_address(
                self.conf.oslo_messaging_zmq.rpc_zmq_host, socket.port)
            self.poller.register(socket, self.receive_message)
            return socket
        except zmq.ZMQError as e:
            errmsg = _LE("Failed binding to port %(port)d: %(e)s")\
                % (self.port, e)
            LOG.error(_LE("Failed binding to port %(port)d: %(e)s"),
                      (self.port, e))
            raise rpc_common.RPCException(errmsg)

    @property
    def address(self):
        return self.socket.bind_address

    @property
    def port(self):
        return self.socket.port

    def cleanup(self):
        self.target_updater.cleanup()
        super(SingleSocketConsumer, self).cleanup()


class TargetUpdater(zmq_updater.UpdaterBase):
    """This entity performs periodic async updates
    to the matchmaker.
    """

    def __init__(self, conf, matchmaker, target, host, socket_type):
        self.target = target
        self.host = host
        self.socket_type = socket_type
        super(TargetUpdater, self).__init__(conf, matchmaker,
                                            self._update_target)

    def _update_target(self):
        self.matchmaker.register(
            self.target, self.host,
            zmq_names.socket_type_str(self.socket_type),
            expire=self.conf.oslo_messaging_zmq.zmq_target_expire)

    def stop(self):
        super(TargetUpdater, self).stop()
        self.matchmaker.unregister(
            self.target, self.host,
            zmq_names.socket_type_str(self.socket_type))
