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

from oslo_messaging._drivers.zmq_driver.rpc.client.zmq_request import Request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_topic
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class CallRequest(Request):

    def __init__(self, conf, target, context, message, timeout=None,
                 retry=None):
        try:
            self.zmq_context = zmq.Context()
            socket = self.zmq_context.socket(zmq.REQ)

            super(CallRequest, self).__init__(conf, target, context,
                                              message, socket, timeout, retry)

            self.connect_address = zmq_topic.get_tcp_address_call(conf,
                                                                  self.topic)
            LOG.info(_LI("Connecting REQ to %s") % self.connect_address)
            self.socket.connect(self.connect_address)
        except zmq.ZMQError as e:
            LOG.error(_LE("Error connecting to socket: %s") % str(e))

    def receive_reply(self):
        # NOTE(ozamiatin): Check for retry here (no retries now)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        reply = self.socket.recv_json()
        return reply[u'reply']
