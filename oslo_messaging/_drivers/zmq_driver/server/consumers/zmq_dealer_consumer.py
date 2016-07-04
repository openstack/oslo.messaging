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

import logging

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.client import zmq_sockets_manager
from oslo_messaging._drivers.zmq_driver.server.consumers \
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        self.sender = zmq_senders.ReplySenderProxy(conf)
        self.sockets_manager = zmq_sockets_manager.SocketsManager(
            conf, server.matchmaker, zmq.ROUTER, zmq.DEALER)
        self.host = None
        super(DealerConsumer, self).__init__(conf, poller, server, zmq.DEALER)
        self.connection_updater = ConsumerConnectionUpdater(
            conf, self.matchmaker, self.socket)
        LOG.info(_LI("[%s] Run DEALER consumer"), self.host)

    def subscribe_socket(self, socket_type):
        try:
            socket = self.sockets_manager.get_socket_to_routers()
            self.sockets.append(socket)
            self.host = socket.handle.identity
            self.poller.register(socket, self.receive_message)
            return socket
        except zmq.ZMQError as e:
            LOG.error(_LE("Failed connecting to ROUTER socket %(e)s") % e)
            raise rpc_common.RPCException(str(e))

    def receive_message(self, socket):
        try:
            empty = socket.recv()
            assert empty == b'', 'Bad format: empty delimiter expected'
            reply_id = socket.recv()
            message_type = int(socket.recv())
            message_id = socket.recv()
            context = socket.recv_loaded()
            message = socket.recv_loaded()
            LOG.debug("[%(host)s] Received %(msg_type)s message %(msg_id)s",
                      {"host": self.host,
                       "msg_type": zmq_names.message_type_str(message_type),
                       "msg_id": message_id})
            if message_type == zmq_names.CALL_TYPE:
                return zmq_incoming_message.ZmqIncomingMessage(
                    context, message, reply_id, message_id, socket, self.sender
                )
            elif message_type in zmq_names.NON_BLOCKING_TYPES:
                return zmq_incoming_message.ZmqIncomingMessage(context,
                                                               message)
            else:
                LOG.error(_LE("Unknown message type: %s"),
                          zmq_names.message_type_str(message_type))
        except (zmq.ZMQError, AssertionError) as e:
            LOG.error(_LE("Receiving message failure: %s"), str(e))

    def cleanup(self):
        LOG.info(_LI("[%s] Destroy DEALER consumer"), self.host)
        self.connection_updater.cleanup()
        super(DealerConsumer, self).cleanup()


class ConsumerConnectionUpdater(zmq_updater.ConnectionUpdater):

    def _update_connection(self):
        routers = self.matchmaker.get_routers()
        for router_address in routers:
            self.socket.connect_to_host(router_address)
