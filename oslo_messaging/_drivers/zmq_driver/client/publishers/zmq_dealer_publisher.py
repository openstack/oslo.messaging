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

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisher(zmq_publisher_base.PublisherBase):

    def send_request(self, request):

        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        dealer_socket, hosts = self._check_hosts_connections(request.target)

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(len(hosts)):
                self._send_request(dealer_socket, request)
        else:
            self._send_request(dealer_socket, request)

    def _send_request(self, socket, request):

        socket.send(b'', zmq.SNDMORE)
        super(DealerPublisher, self)._send_request(socket, request)

        LOG.info(_LI("Sending message %(message)s to a target %(target)s")
                 % {"message": request.message,
                    "target": request.target})

    def _check_hosts_connections(self, target):
        if str(target) in self.outbound_sockets:
            dealer_socket, hosts = self.outbound_sockets[str(target)]
        else:
            dealer_socket = zmq.Context().socket(zmq.DEALER)
            hosts = self.matchmaker.get_hosts(target)
            for host in hosts:
                self._connect_to_host(dealer_socket, host, target)
            self.outbound_sockets[str(target)] = (dealer_socket, hosts)
        return dealer_socket, hosts

    @staticmethod
    def _connect_to_host(socket, host, target):
        address = zmq_address.get_tcp_direct_address(host)
        try:
            LOG.info(_LI("Connecting DEALER to %(address)s for %(target)s")
                     % {"address": address,
                        "target": target})
            socket.connect(address)
        except zmq.ZMQError as e:
            errmsg = _LE("Failed connecting DEALER to %(address)s: %(e)s")\
                % (address, e)
            LOG.error(errmsg)
            raise rpc_common.RPCException(errmsg)
