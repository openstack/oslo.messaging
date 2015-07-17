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

from oslo_messaging._drivers.common import RPCException
import oslo_messaging._drivers.zmq_driver.broker.zmq_base_proxy as base_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerBackend(base_proxy.DirectBackendMatcher):

    def __init__(self, conf, context, poller=None):
        if poller is None:
            poller = zmq_async.get_poller(
                native_zmq=conf.rpc_zmq_native)
        super(DealerBackend, self).__init__(conf, poller, context)

    def _get_target(self, message):
        return zmq_serializer.get_target_from_call_message(message)

    def _get_ipc_address(self, target):
        return zmq_target.get_ipc_address_call(self.conf, target)

    def _send_message(self, backend, message, topic):
        # Empty needed for awaiting REP socket to work properly
        # (DEALER-REP usage specific)
        backend.send(b'', zmq.SNDMORE)
        backend.send(message.pop(0), zmq.SNDMORE)
        backend.send_string(message.pop(0), zmq.SNDMORE)
        message.pop(0)  # Drop target unneeded any more
        backend.send_multipart(message)

    def _create_backend(self, ipc_address):
        if ipc_address in self.backends:
            return self.backends[ipc_address]
        backend = self.context.socket(zmq.DEALER)
        backend.connect(ipc_address)
        self.poller.register(backend)
        self.backends[ipc_address] = backend
        return backend


class FrontendTcpRouter(base_proxy.BaseTcpFrontend):

    def __init__(self, conf, context, poller=None):
        if poller is None:
            poller = zmq_async.get_poller(
                native_zmq=conf.rpc_zmq_native)
        super(FrontendTcpRouter, self).__init__(
            conf, poller, context,
            socket_type=zmq.ROUTER,
            port_number=conf.rpc_zmq_port,
            receive_meth=self._receive_message)

    def _receive_message(self, socket):

        try:
            reply_id = socket.recv()
            empty = socket.recv()
            assert empty == b'', "Empty delimiter expected"
            msg_type = socket.recv_string()
            target_dict = socket.recv_json()
            target = zmq_target.target_from_dict(target_dict)
            other = socket.recv_multipart()
        except zmq.ZMQError as e:
            LOG.error(_LE("Error receiving message %s") % str(e))
            return None

        if msg_type == zmq_serializer.FANOUT_TYPE:
            other.insert(0, zmq_target.target_to_str(target).encode("utf-8"))

        return [reply_id, msg_type, target] + other

    @staticmethod
    def _reduce_empty(reply):
        reply.pop(0)
        return reply

    def redirect_outgoing_reply(self, reply):
        self._reduce_empty(reply)
        try:
            self.frontend.send_multipart(reply)
            LOG.info(_LI("Redirecting reply to client %s") % reply)
        except zmq.ZMQError:
            errmsg = _LE("Failed redirecting reply to client %s") % reply
            LOG.error(errmsg)
            raise RPCException(errmsg)
