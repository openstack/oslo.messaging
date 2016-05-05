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

    def __init__(self, context, message, msg_id):
        super(DealerIncomingMessage, self).__init__(context, message)
        self.msg_id = msg_id

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for non-call messages"""

    def acknowledge(self):
        LOG.debug("Not sending acknowledge for %s", self.msg_id)

    def requeue(self):
        """Requeue is not supported"""


class DealerIncomingRequest(base.RpcIncomingMessage):

    def __init__(self, socket, request, envelope):
        super(DealerIncomingRequest, self).__init__(request.context,
                                                    request.message)
        self.reply_socket = socket
        self.request = request
        self.envelope = envelope

    def reply(self, reply=None, failure=None, log_failure=True):
        if failure is not None:
            failure = rpc_common.serialize_remote_exception(failure,
                                                            log_failure)
        response = zmq_response.Response(type=zmq_names.REPLY_TYPE,
                                         message_id=self.request.message_id,
                                         reply_id=self.envelope.reply_id,
                                         reply_body=reply,
                                         failure=failure,
                                         log_failure=log_failure)

        LOG.debug("Replying %s", (str(self.request.message_id)))

        self.envelope.routing_key = self.envelope.reply_id
        self.envelope.msg_type = zmq_names.REPLY_TYPE

        self.reply_socket.send(b'', zmq.SNDMORE)
        self.reply_socket.send_pyobj(self.envelope, zmq.SNDMORE)
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
        self.socket = self.sockets_manager.get_socket_to_publishers()
        self.poller.register(self.socket, self.receive_message)
        self.host = self.socket.handle.identity
        self.target_updater = zmq_consumer_base.TargetUpdater(
            conf, self.matchmaker, self.target, self.host,
            zmq.DEALER)
        LOG.info(_LI("[%s] Run DEALER consumer"), self.host)

    def _receive_request(self, socket):
        empty = socket.recv()
        assert empty == b'', 'Bad format: empty delimiter expected'
        envelope = socket.recv_pyobj()
        request = socket.recv_pyobj()
        return request, envelope

    def receive_message(self, socket):
        try:
            request, envelope = self._receive_request(socket)
            LOG.debug("[%(host)s] Received %(type)s, %(id)s, %(target)s",
                      {"host": self.host,
                       "type": request.msg_type,
                       "id": request.message_id,
                       "target": request.target})

            if request.msg_type == zmq_names.CALL_TYPE:
                return DealerIncomingRequest(socket, request, envelope)
            elif request.msg_type in zmq_names.NON_BLOCKING_TYPES:
                return DealerIncomingMessage(request.context, request.message,
                                             request.message_id)
            else:
                LOG.error(_LE("Unknown message type: %s"), request.msg_type)

        except (zmq.ZMQError, AssertionError) as e:
            LOG.error(_LE("Receiving message failure: %s"), str(e))
