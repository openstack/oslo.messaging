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
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver import zmq_async

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, context, message, reply_id=None, message_id=None,
                 socket=None, ack_sender=None, reply_sender=None):

        if ack_sender is not None or reply_sender is not None:
            assert socket is not None, "Valid socket expected!"
            assert message_id is not None, "Valid message ID expected!"
            assert reply_id is not None, "Valid reply ID expected!"

        super(ZmqIncomingMessage, self).__init__(context, message)

        self.reply_id = reply_id
        self.message_id = message_id
        self.socket = socket
        self.ack_sender = ack_sender
        self.reply_sender = reply_sender

    def acknowledge(self):
        if self.ack_sender is not None:
            ack = zmq_response.Ack(message_id=self.message_id,
                                   reply_id=self.reply_id)
            self.ack_sender.send(self.socket, ack)

    def reply(self, reply=None, failure=None):
        if self.reply_sender is not None:
            if failure is not None:
                failure = rpc_common.serialize_remote_exception(failure)
            reply = zmq_response.Reply(message_id=self.message_id,
                                       reply_id=self.reply_id,
                                       reply_body=reply,
                                       failure=failure)
            self.reply_sender.send(self.socket, reply)

    def requeue(self):
        """Requeue is not supported"""
