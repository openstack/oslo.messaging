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

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_base_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_topic as topic_utils
from oslo_messaging._i18n import _LE


LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqIncomingRequest(base.IncomingMessage):

    def __init__(self, listener, context, message, socket, rep_id, poller):
        super(ZmqIncomingRequest, self).__init__(listener, context, message)
        self.reply_socket = socket
        self.reply_id = rep_id
        self.received = None
        self.poller = poller

    def reply(self, reply=None, failure=None, log_failure=True):
        if failure is not None:
            failure = rpc_common.serialize_remote_exception(failure,
                                                            log_failure)
        message_reply = {zmq_serializer.FIELD_REPLY: reply,
                         zmq_serializer.FIELD_FAILURE: failure,
                         zmq_serializer.FIELD_LOG_FAILURE: log_failure}
        LOG.debug("Replying %s REP", (str(message_reply)))
        self.received = True
        self.reply_socket.send(self.reply_id, zmq.SNDMORE)
        self.reply_socket.send(b'', zmq.SNDMORE)
        self.reply_socket.send_json(message_reply)
        self.poller.resume_polling(self.reply_socket)

    def acknowledge(self):
        pass

    def requeue(self):
        pass


class CallResponder(zmq_base_consumer.ConsumerBase):

    def _receive_message(self, socket):
        try:
            reply_id = socket.recv()
            empty = socket.recv()
            assert empty == b'', 'Bad format: empty separator expected'
            msg_type = socket.recv_string()
            assert msg_type is not None, 'Bad format: msg type expected'
            topic = socket.recv_string()
            assert topic is not None, 'Bad format: topic string expected'
            msg_id = socket.recv_string()
            assert msg_id is not None, 'Bad format: message ID expected'
            context = socket.recv_json()
            message = socket.recv_json()
            LOG.debug("[Server] REP Received message %s" % str(message))
            incoming = ZmqIncomingRequest(self.listener,
                                          context,
                                          message, socket,
                                          reply_id,
                                          self.poller)
            return incoming
        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed ... {}"), e)

    def listen(self, target):
        topic = topic_utils.Topic.from_target(self.conf, target)
        ipc_rep_address = topic_utils.get_ipc_address_call(self.conf, topic)
        rep_socket = self.context.socket(zmq.REP)
        rep_socket.bind(ipc_rep_address)
        self.sockets_per_topic[str(topic)] = rep_socket
        self.poller.register(rep_socket, self._receive_message)
