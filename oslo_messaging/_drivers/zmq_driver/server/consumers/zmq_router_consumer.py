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

from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.server.consumers \
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class RouterConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        self.ack_sender = zmq_senders.AckSenderDirect(conf)
        self.reply_sender = zmq_senders.ReplySenderDirect(conf)
        super(RouterConsumer, self).__init__(conf, poller, server, zmq.ROUTER)
        LOG.info(_LI("[%s] Run ROUTER consumer"), self.host)

    def _receive_request(self, socket):
        reply_id = socket.recv()
        empty = socket.recv()
        assert empty == b'', 'Bad format: empty delimiter expected'
        msg_type = int(socket.recv())
        message_id = socket.recv_string()
        context, message = socket.recv_loaded()
        return reply_id, msg_type, message_id, context, message

    def receive_message(self, socket):
        try:
            reply_id, msg_type, message_id, context, message = \
                self._receive_request(socket)

            LOG.debug("[%(host)s] Received %(msg_type)s message %(msg_id)s",
                      {"host": self.host,
                       "msg_type": zmq_names.message_type_str(msg_type),
                       "msg_id": message_id})

            if msg_type == zmq_names.CALL_TYPE or \
                    msg_type in zmq_names.NON_BLOCKING_TYPES:
                ack_sender = self.ack_sender \
                    if self.conf.oslo_messaging_zmq.rpc_use_acks else None
                reply_sender = self.reply_sender \
                    if msg_type == zmq_names.CALL_TYPE else None
                return zmq_incoming_message.ZmqIncomingMessage(
                    context, message, reply_id, message_id, socket,
                    ack_sender, reply_sender
                )
            else:
                LOG.error(_LE("Unknown message type: %s"),
                          zmq_names.message_type_str(msg_type))
        except (zmq.ZMQError, AssertionError, ValueError) as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))

    def cleanup(self):
        LOG.info(_LI("[%s] Destroy ROUTER consumer"), self.host)
        super(RouterConsumer, self).cleanup()
