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
from oslo_messaging._drivers.zmq_driver.rpc.client import zmq_cast_publisher
from oslo_messaging._drivers.zmq_driver.rpc.client.zmq_request import Request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class CastRequest(Request):

    msg_type = zmq_serializer.CAST_TYPE

    def __call__(self, *args, **kwargs):
        self.send_request()

    def send_request(self):
        self.socket.send(b'', zmq.SNDMORE)
        super(CastRequest, self).send_request()

    def receive_reply(self):
        # Ignore reply for CAST
        pass


class FanoutRequest(CastRequest):

    msg_type = zmq_serializer.FANOUT_TYPE

    def __init__(self, *args, **kwargs):
        self.hosts_count = kwargs.pop("hosts_count")
        super(FanoutRequest, self).__init__(*args, **kwargs)

    def send_request(self):
        for _ in range(self.hosts_count):
            super(FanoutRequest, self).send_request()


class DealerCastPublisher(zmq_cast_publisher.CastPublisherBase):

    def __init__(self, conf, matchmaker):
        super(DealerCastPublisher, self).__init__(conf)
        self.matchmaker = matchmaker

    def cast(self, target, context,
             message, timeout=None, retry=None):
        if str(target) in self.outbound_sockets:
            dealer_socket, hosts = self.outbound_sockets[str(target)]
        else:
            dealer_socket = self.zmq_context.socket(zmq.DEALER)
            hosts = self.matchmaker.get_hosts(target)
            for host in hosts:
                self._connect_to_host(dealer_socket, host)
            self.outbound_sockets[str(target)] = (dealer_socket, hosts)

        if target.fanout:
            request = FanoutRequest(self.conf, target, context, message,
                                    dealer_socket, timeout, retry,
                                    hosts_count=len(hosts))
        else:
            request = CastRequest(self.conf, target, context, message,
                                  dealer_socket, timeout, retry)

        request.send_request()

    def _connect_to_host(self, socket, host):
        address = zmq_target.get_tcp_direct_address(host)
        try:
            LOG.info(_LI("Connecting DEALER to %s") % address)
            socket.connect(address)
        except zmq.ZMQError as e:
            errmsg = _LE("Failed connecting DEALER to %(address)s: %(e)s")\
                % (address, e)
            LOG.error(errmsg)
            raise rpc_common.RPCException(errmsg)

    def cleanup(self):
        for socket, hosts in self.outbound_sockets.values():
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
