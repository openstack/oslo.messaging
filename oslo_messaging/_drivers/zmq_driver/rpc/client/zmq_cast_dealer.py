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

from oslo_messaging._drivers.zmq_driver.rpc.client import zmq_cast_publisher
from oslo_messaging._drivers.zmq_driver.rpc.client.zmq_request import Request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class CastRequest(Request):

    def __init__(self, conf, target, context,
                 message, socket, address, timeout=None, retry=None):
        self.connect_address = address
        fanout_type = zmq_serializer.FANOUT_TYPE
        cast_type = zmq_serializer.CAST_TYPE
        msg_type = fanout_type if target.fanout else cast_type
        super(CastRequest, self).__init__(conf, target, context, message,
                                          socket, msg_type, timeout, retry)

    def __call__(self, *args, **kwargs):
        self.send_request()

    def send_request(self):
        self.socket.send(b'', zmq.SNDMORE)
        super(CastRequest, self).send_request()

    def receive_reply(self):
        # Ignore reply for CAST
        pass


class DealerCastPublisher(zmq_cast_publisher.CastPublisherBase):

    def __init__(self, conf, matchmaker):
        super(DealerCastPublisher, self).__init__(conf)
        self.matchmaker = matchmaker

    def cast(self, target, context,
             message, timeout=None, retry=None):
        host = self.matchmaker.get_single_host(target)
        connect_address = zmq_target.get_tcp_address_call(self.conf, host)
        dealer_socket = self._create_socket(connect_address)
        request = CastRequest(self.conf, target, context, message,
                              dealer_socket, connect_address, timeout, retry)
        request.send_request()

    def _create_socket(self, address):
        if address in self.outbound_sockets:
            return self.outbound_sockets[address]
        try:
            dealer_socket = self.zmq_context.socket(zmq.DEALER)
            LOG.info(_LI("Connecting DEALER to %s") % address)
            dealer_socket.connect(address)
            self.outbound_sockets[address] = dealer_socket
            return dealer_socket
        except zmq.ZMQError:
            LOG.error(_LE("Failed connecting DEALER to %s") % address)
            raise

    def cleanup(self):
        if self.outbound_sockets:
            for socket in self.outbound_sockets.values():
                socket.close()
