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
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names


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
        message_reply = {zmq_names.FIELD_REPLY: reply,
                         zmq_names.FIELD_FAILURE: failure,
                         zmq_names.FIELD_LOG_FAILURE: log_failure}

        LOG.info("Replying %s REP", (str(message_reply)))

        self.received = True
        self.reply_socket.send(self.reply_id, zmq.SNDMORE)
        self.reply_socket.send(b'', zmq.SNDMORE)
        self.reply_socket.send_pyobj(message_reply)
        self.poller.resume_polling(self.reply_socket)

    def requeue(self):
        """Requeue is not supported"""
