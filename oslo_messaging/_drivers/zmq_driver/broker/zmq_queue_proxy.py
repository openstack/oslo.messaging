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

from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_pub_publisher
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._i18n import _LI

zmq = zmq_async.import_zmq(zmq_concurrency='native')
LOG = logging.getLogger(__name__)


class UniversalQueueProxy(object):

    def __init__(self, conf, context, matchmaker):
        self.conf = conf
        self.context = context
        super(UniversalQueueProxy, self).__init__()
        self.matchmaker = matchmaker
        self.poller = zmq_async.get_poller(zmq_concurrency='native')

        self.fe_router_socket = zmq_socket.ZmqRandomPortSocket(
            conf, context, zmq.ROUTER)
        self.be_router_socket = zmq_socket.ZmqRandomPortSocket(
            conf, context, zmq.ROUTER)

        self.poller.register(self.fe_router_socket.handle,
                             self._receive_in_request)
        self.poller.register(self.be_router_socket.handle,
                             self._receive_in_request)

        self.fe_router_address = zmq_address.combine_address(
            self.conf.rpc_zmq_host, self.fe_router_socket.port)
        self.be_router_address = zmq_address.combine_address(
            self.conf.rpc_zmq_host, self.fe_router_socket.port)

        self.pub_publisher = zmq_pub_publisher.PubPublisherProxy(
            conf, matchmaker)

        self.matchmaker.register_publisher(
            (self.pub_publisher.host, self.fe_router_address))
        LOG.info(_LI("[PUB:%(pub)s, ROUTER:%(router)s] Run PUB publisher"),
                 {"pub": self.pub_publisher.host,
                  "router": self.fe_router_address})
        self.matchmaker.register_router(self.be_router_address)
        LOG.info(_LI("[Backend ROUTER:%(router)s] Run ROUTER"),
                 {"router": self.be_router_address})

    def run(self):
        message, socket = self.poller.poll(self.conf.rpc_poll_timeout)
        if message is None:
            return

        msg_type = message[0]
        if self.conf.use_pub_sub and msg_type in (zmq_names.CAST_FANOUT_TYPE,
                                                  zmq_names.NOTIFY_TYPE):
            self.pub_publisher.send_request(message)
        elif msg_type in zmq_names.DIRECT_TYPES:
            self._redirect_message(self.be_router_socket
                                   if socket is self.fe_router_socket
                                   else self.fe_router_socket, message)

    @staticmethod
    def _receive_in_request(socket):
        try:
            reply_id = socket.recv()
            assert reply_id is not None, "Valid id expected"
            empty = socket.recv()
            assert empty == b'', "Empty delimiter expected"
            msg_type = int(socket.recv())
            routing_key = socket.recv()
            payload = socket.recv_multipart()
            payload.insert(0, reply_id)
            payload.insert(0, routing_key)
            payload.insert(0, msg_type)
            return payload
        except (AssertionError, zmq.ZMQError):
            LOG.error("Received message with wrong format")
            return None

    @staticmethod
    def _redirect_message(socket, multipart_message):
        message_type = multipart_message.pop(0)
        routing_key = multipart_message.pop(0)
        reply_id = multipart_message.pop(0)
        message_id = multipart_message[0]
        socket.send(routing_key, zmq.SNDMORE)
        socket.send(b'', zmq.SNDMORE)
        socket.send(reply_id, zmq.SNDMORE)
        socket.send(six.b(str(message_type)), zmq.SNDMORE)
        LOG.debug("Redirecting message %s" % message_id)
        socket.send_multipart(multipart_message)

    def cleanup(self):
        self.fe_router_socket.close()
        self.be_router_socket.close()
        self.pub_publisher.cleanup()
        self.matchmaker.unregister_publisher(
            (self.pub_publisher.host, self.fe_router_address))
        self.matchmaker.unregister_router(self.be_router_address)
