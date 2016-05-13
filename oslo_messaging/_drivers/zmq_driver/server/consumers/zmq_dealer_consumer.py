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

import six

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, context, message):
        super(DealerIncomingMessage, self).__init__(context, message)

    def reply(self, reply=None, failure=None):
        """Reply is not needed for non-call messages"""

    def acknowledge(self):
        """Not sending acknowledge"""

    def requeue(self):
        """Requeue is not supported"""


class DealerIncomingRequest(base.RpcIncomingMessage):

    def __init__(self, socket, reply_id, message_id, context, message):
        super(DealerIncomingRequest, self).__init__(context, message)
        self.reply_socket = socket
        self.reply_id = reply_id
        self.message_id = message_id

    def reply(self, reply=None, failure=None):
        if failure is not None:
            failure = rpc_common.serialize_remote_exception(failure)
        response = zmq_response.Response(type=zmq_names.REPLY_TYPE,
                                         message_id=self.message_id,
                                         reply_id=self.reply_id,
                                         reply_body=reply,
                                         failure=failure)

        LOG.debug("Replying %s", self.message_id)

        self.reply_socket.send(b'', zmq.SNDMORE)
        self.reply_socket.send(six.b(str(zmq_names.REPLY_TYPE)), zmq.SNDMORE)
        self.reply_socket.send(self.reply_id, zmq.SNDMORE)
        self.reply_socket.send(self.message_id, zmq.SNDMORE)
        self.reply_socket.send_pyobj(response)

    def requeue(self):
        """Requeue is not supported"""


class DealerConsumer(zmq_consumer_base.ConsumerBase):

    def __init__(self, conf, poller, server):
        super(DealerConsumer, self).__init__(conf, poller, server)
        self.matchmaker = server.matchmaker
        self.target = server.target
        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, self.matchmaker, zmq.ROUTER, zmq.DEALER)
        self.socket = self.sockets_manager.get_socket_to_routers()
        self.poller.register(self.socket, self.receive_message)
        self.host = self.socket.handle.identity
        self.target_updater = zmq_consumer_base.TargetUpdater(
            conf, self.matchmaker, self.target, self.host,
            zmq.DEALER)
        LOG.info(_LI("[%s] Run DEALER consumer"), self.host)

    def receive_message(self, socket):
        try:
            empty = socket.recv()
            assert empty == b'', 'Bad format: empty delimiter expected'
            reply_id = socket.recv()
            message_type = int(socket.recv())
            message_id = socket.recv()
            context = socket.recv_pyobj()
            message = socket.recv_pyobj()
            LOG.debug("[%(host)s] Received message %(id)s",
                      {"host": self.host, "id": message_id})
            if message_type == zmq_names.CALL_TYPE:
                return DealerIncomingRequest(
                    socket, reply_id, message_id, context, message)
            elif message_type in zmq_names.NON_BLOCKING_TYPES:
                return DealerIncomingMessage(context, message)
            else:
                LOG.error(_LE("Unknown message type: %s"),
                          zmq_names.message_type_str(message_type))

        except (zmq.ZMQError, AssertionError) as e:
            LOG.error(_LE("Receiving message failure: %s"), str(e))
