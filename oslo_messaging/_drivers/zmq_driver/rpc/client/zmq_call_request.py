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

import oslo_messaging
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.rpc.client.zmq_request import Request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class CallRequest(Request):

    msg_type = zmq_serializer.CALL_TYPE

    def __init__(self, conf, target, context, message, timeout=None,
                 retry=None, allowed_remote_exmods=None, matchmaker=None):
        self.allowed_remote_exmods = allowed_remote_exmods or []
        self.matchmaker = matchmaker
        self.reply_poller = zmq_async.get_reply_poller()

        try:
            self.zmq_context = zmq.Context()
            socket = self.zmq_context.socket(zmq.REQ)
            super(CallRequest, self).__init__(conf, target, context,
                                              message, socket,
                                              timeout, retry)
            self.host = self.matchmaker.get_single_host(self.target)
            self.connect_address = zmq_target.get_tcp_direct_address(
                self.host)
            LOG.info(_LI("Connecting REQ to %s") % self.connect_address)
            self.socket.connect(self.connect_address)
            self.reply_poller.register(
                self.socket, recv_method=lambda socket: socket.recv_json())

        except zmq.ZMQError as e:
            errmsg = _LE("Error connecting to socket: %s") % str(e)
            LOG.error(errmsg)
            raise rpc_common.RPCException(errmsg)

    def close(self):
        self.reply_poller.close()
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()

    def receive_reply(self):
        # NOTE(ozamiatin): Check for retry here (no retries now)
        reply, socket = self.reply_poller.poll(timeout=self.timeout)
        if reply is None:
            raise oslo_messaging.MessagingTimeout(
                "Timeout %s seconds was reached" % self.timeout)

        if reply[zmq_serializer.FIELD_FAILURE]:
            raise rpc_common.deserialize_remote_exception(
                reply[zmq_serializer.FIELD_FAILURE],
                self.allowed_remote_exmods)
        else:
            return reply[zmq_serializer.FIELD_REPLY]
