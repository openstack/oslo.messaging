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
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, context, message, reply_id=None, message_id=None,
                 socket=None, sender=None):

        if sender is not None:
            assert socket is not None, "Valid socket expected!"
            assert message_id is not None, "Valid message ID expected!"
            assert reply_id is not None, "Valid reply ID expected!"

        super(ZmqIncomingMessage, self).__init__(context, message)

        self.reply_id = reply_id
        self.message_id = message_id
        self.socket = socket
        self.sender = sender

    def acknowledge(self):
        """Not sending acknowledge"""

    def reply(self, reply=None, failure=None):
        if self.sender is not None:
            if failure is not None:
                failure = rpc_common.serialize_remote_exception(failure)
            reply = zmq_response.Response(msg_type=zmq_names.REPLY_TYPE,
                                          message_id=self.message_id,
                                          reply_id=self.reply_id,
                                          reply_body=reply,
                                          failure=failure)
            self.sender.send(self.socket, reply)

    def requeue(self):
        """Requeue is not supported"""
