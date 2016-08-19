#    Copyright 2016 Mirantis, Inc.
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

import time

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket

zmq = zmq_async.import_zmq()


class SocketsManager(object):

    def __init__(self, conf, matchmaker, listener_type, socket_type):
        self.conf = conf
        self.matchmaker = matchmaker
        self.listener_type = listener_type
        self.socket_type = socket_type
        self.zmq_context = zmq.Context()
        self.outbound_sockets = {}
        self.socket_to_publishers = None
        self.socket_to_routers = None

    def get_hosts(self, target):
        return self.matchmaker.get_hosts_retry(
            target, zmq_names.socket_type_str(self.listener_type))

    @staticmethod
    def _key_from_target(target):
        return target.topic if target.fanout else str(target)

    def _get_hosts_and_connect(self, socket, target):
        hosts = self.get_hosts(target)
        self._connect_to_hosts(socket, target, hosts)

    def _track_socket(self, socket, target):
        key = self._key_from_target(target)
        self.outbound_sockets[key] = (socket, time.time())

    def _connect_to_hosts(self, socket, target, hosts):
        for host in hosts:
            socket.connect_to_host(host)
        self._track_socket(socket, target)

    def _check_for_new_hosts(self, target):
        key = self._key_from_target(target)
        socket, tm = self.outbound_sockets[key]
        if 0 <= self.conf.oslo_messaging_zmq.zmq_target_expire \
                <= time.time() - tm:
            self._get_hosts_and_connect(socket, target)
        return socket

    def get_socket(self, target):
        key = self._key_from_target(target)
        if key in self.outbound_sockets:
            socket = self._check_for_new_hosts(target)
        else:
            socket = zmq_socket.ZmqSocket(self.conf, self.zmq_context,
                                          self.socket_type, immediate=False)
            self._get_hosts_and_connect(socket, target)
        return socket

    def get_socket_to_publishers(self):
        if self.socket_to_publishers is not None:
            return self.socket_to_publishers
        self.socket_to_publishers = zmq_socket.ZmqSocket(
            self.conf, self.zmq_context, self.socket_type)
        publishers = self.matchmaker.get_publishers()
        for pub_address, router_address in publishers:
            self.socket_to_publishers.connect_to_host(router_address)
        return self.socket_to_publishers

    def get_socket_to_routers(self):
        if self.socket_to_routers is not None:
            return self.socket_to_routers
        self.socket_to_routers = zmq_socket.ZmqSocket(
            self.conf, self.zmq_context, self.socket_type)
        routers = self.matchmaker.get_routers()
        for router_address in routers:
            self.socket_to_routers.connect_to_host(router_address)
        return self.socket_to_routers

    def cleanup(self):
        for socket, tm in self.outbound_sockets.values():
            socket.close()
