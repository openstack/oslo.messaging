#    Copyright 2015-2016 Mirantis, Inc.
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
from oslo_messaging._drivers.zmq_driver.client import zmq_response
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
        self.reply_sender = zmq_senders.ReplySenderDirect(conf)
        super(RouterConsumer, self).__init__(conf, poller, server, zmq.ROUTER)
        LOG.info(_LI("[%s] Run ROUTER consumer"), self.host)

    def _reply(self, rpc_message, reply, failure):
        if failure is not None:
            failure = rpc_common.serialize_remote_exception(failure)
        reply = zmq_response.Reply(message_id=rpc_message.message_id,
                                   reply_id=rpc_message.reply_id,
                                   reply_body=reply,
                                   failure=failure)
        self.reply_sender.send(rpc_message.socket, reply)
        return reply

    def _create_message(self, context, message, reply_id, message_id, socket,
                        message_type):
        if message_type == zmq_names.CALL_TYPE:
            message = zmq_incoming_message.ZmqIncomingMessage(
                context, message, reply_id=reply_id, message_id=message_id,
                socket=socket, reply_method=self._reply
            )
        else:
            message = zmq_incoming_message.ZmqIncomingMessage(context, message)

        LOG.debug("[%(host)s] Received %(msg_type)s message %(msg_id)s",
                  {"host": self.host,
                   "msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id})
        return message

    def receive_message(self, socket):
        try:
            reply_id = socket.recv()
            assert reply_id != b'', "Valid reply id expected!"
            empty = socket.recv()
            assert empty == b'', "Empty delimiter expected!"
            message_type = int(socket.recv())
            assert message_type in zmq_names.REQUEST_TYPES, \
                "Request message type expected!"
            message_id = socket.recv_string()
            assert message_id != '', "Valid message id expected!"
            context, message = socket.recv_loaded()

            return self._create_message(context, message, reply_id,
                                        message_id, socket, message_type)
        except (zmq.ZMQError, AssertionError, ValueError) as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))
            # NOTE(gdavoian): drop the left parts of a broken message
            if socket.getsockopt(zmq.RCVMORE):
                socket.recv_multipart()

    def cleanup(self):
        LOG.info(_LI("[%s] Destroy ROUTER consumer"), self.host)
        super(RouterConsumer, self).cleanup()
