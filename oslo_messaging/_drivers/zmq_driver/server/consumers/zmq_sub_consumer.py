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
import uuid

import six

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class SubIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, request, socket):
        super(SubIncomingMessage, self).__init__(
            request.context, request.message)
        self.socket = socket
        self.msg_id = request.message_id

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for non-call messages."""

    def acknowledge(self):
        LOG.debug("Not sending acknowledge for %s", self.msg_id)

    def requeue(self):
        """Requeue is not supported"""


class SubConsumer(zmq_consumer_base.ConsumerBase):

    def __init__(self, conf, poller, server):
        super(SubConsumer, self).__init__(conf, poller, server)
        self.matchmaker = server.matchmaker
        self.target = server.target
        self.subscriptions = set()
        self.socket = zmq_socket.ZmqSocket(self.conf, self.context, zmq.SUB)
        self.sockets.append(self.socket)
        self.id = uuid.uuid4()
        self._subscribe_on_target(self.target)
        self.on_publishers(self.matchmaker.get_publishers())
        self.poller.register(self.socket, self.receive_message)

    def on_publishers(self, publishers):
        for host, sync in publishers:
            self.socket.connect(zmq_address.get_tcp_direct_address(host))
        LOG.debug("[%s] SUB consumer connected to publishers %s",
                  self.id, publishers)

    def _subscribe_on_target(self, target):
        # NOTE(ozamiatin): No locks needed here, because this is called
        # before the async updater loop started
        topic_filter = zmq_address.target_to_subscribe_filter(target)
        if target.topic:
            self.socket.setsockopt(zmq.SUBSCRIBE, six.b(target.topic))
            self.subscriptions.add(six.b(target.topic))
        if target.server:
            self.socket.setsockopt(zmq.SUBSCRIBE, six.b(target.server))
            self.subscriptions.add(six.b(target.server))
        if target.topic and target.server:
            self.socket.setsockopt(zmq.SUBSCRIBE, topic_filter)
            self.subscriptions.add(topic_filter)

        LOG.debug("[%(host)s] Subscribing to topic %(filter)s",
                  {"host": self.id, "filter": topic_filter})

    def _receive_request(self, socket):
        topic_filter = socket.recv()
        LOG.debug("[%(id)s] Received %(topic_filter)s topic",
                  {'id': self.id, 'topic_filter': topic_filter})
        assert topic_filter in self.subscriptions
        request = socket.recv_pyobj()
        return request

    def receive_message(self, socket):
        try:
            request = self._receive_request(socket)
            if not request:
                return None
            LOG.debug("Received %(type)s, %(id)s, %(target)s",
                      {"type": request.msg_type,
                       "id": request.message_id,
                       "target": request.target})

            if request.msg_type not in zmq_names.MULTISEND_TYPES:
                LOG.error(_LE("Unknown message type: %s"), request.msg_type)
            else:
                return SubIncomingMessage(request, socket)
        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))

    def cleanup(self):
        super(SubConsumer, self).cleanup()
