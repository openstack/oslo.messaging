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
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class RouterConsumer(object):

    def __init__(self, conf, poller, server):

        self.poller = poller
        self.server = server

        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.ROUTER)
            self.address = zmq_address.get_tcp_random_address(conf)
            self.port = self.socket.bind_to_random_port(self.address)
            LOG.info(_LI("Run ROUTER consumer on %(addr)s:%(port)d"),
                     {"addr": self.address,
                      "port": self.port})
        except zmq.ZMQError as e:
            errmsg = _LE("Failed binding to port %(port)d: %(e)s")\
                % (self.port, e)
            LOG.error(errmsg)
            raise rpc_common.RPCException(errmsg)

    def listen(self, target):
        LOG.info(_LI("Listen to target %s") % str(target))
        self.poller.register(self.socket, self._receive_message)

    def cleanup(self):
        if not self.socket.closed:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()

    def _receive_message(self, socket):

        try:
            reply_id = socket.recv()
            empty = socket.recv()
            assert empty == b'', 'Bad format: empty delimiter expected'
            msg_type = socket.recv_string()
            assert msg_type is not None, 'Bad format: msg type expected'
            context = socket.recv_json()
            message = socket.recv_json()
            LOG.debug("Received %s message %s" % (msg_type, str(message)))

            if msg_type == zmq_names.CALL_TYPE:
                return zmq_incoming_message.ZmqIncomingRequest(
                    self.server, context, message, socket, reply_id,
                    self.poller)
            elif msg_type in zmq_names.CAST_TYPES:
                return zmq_incoming_message.ZmqCastMessage(
                    self.server, context, message, socket, self.poller)
            elif msg_type in zmq_names.NOTIFY_TYPES:
                return zmq_incoming_message.ZmqNotificationMessage(
                    self.server, context, message, socket, self.poller)
            else:
                LOG.error(_LE("Unknown message type: %s") % msg_type)

        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed: %s") % str(e))
