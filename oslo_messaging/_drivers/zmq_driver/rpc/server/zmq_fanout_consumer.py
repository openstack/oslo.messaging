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

import six

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_base_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_target as topic_utils
from oslo_messaging._i18n import _LE


LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqFanoutMessage(base.IncomingMessage):

    def __init__(self, listener, context, message, socket, poller):
        super(ZmqFanoutMessage, self).__init__(listener, context, message)
        poller.resume_polling(socket)

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for fanout(cast) messages"""

    def acknowledge(self):
        pass

    def requeue(self):
        pass


class FanoutConsumer(zmq_base_consumer.ConsumerBase):

    def _receive_message(self, socket):
        try:
            topic = socket.recv_string()
            assert topic is not None, 'Bad format: Topic is expected'
            msg_id = socket.recv_string()
            assert msg_id is not None, 'Bad format: message ID expected'
            context = socket.recv_json()
            message = socket.recv_json()
            LOG.debug("[Server] REP Received message %s" % str(message))
            incoming = ZmqFanoutMessage(self.listener, context, message,
                                        socket, self.poller)
            return incoming
        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed ... {}"), e)

    def listen(self, target):
        topic = topic_utils.target_to_str(target)
        ipc_address = topic_utils.get_ipc_address_fanout(self.conf)
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(ipc_address)
        if six.PY3:
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
        else:
            sub_socket.setsockopt(zmq.SUBSCRIBE, str(topic))
        self.poller.register(sub_socket, self._receive_message)
