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

        self.router_socket = zmq_socket.ZmqRandomPortSocket(
            conf, context, zmq.ROUTER)

        self.poller.register(self.router_socket.handle,
                             self._receive_in_request)

        self.router_address = zmq_address.combine_address(
            self.conf.rpc_zmq_host, self.router_socket.port)

        self.pub_publisher = zmq_pub_publisher.PubPublisherProxy(
            conf, matchmaker)

        self.matchmaker.register_publisher(
            (self.pub_publisher.host, self.router_address))
        LOG.info(_LI("[PUB:%(pub)s, ROUTER:%(router)s] Run PUB publisher"),
                 {"pub": self.pub_publisher.host,
                  "router": self.router_address})

    def run(self):
        message, socket = self.poller.poll(self.conf.rpc_poll_timeout)
        if message is None:
            return

        envelope = message[zmq_names.MULTIPART_IDX_ENVELOPE]
        if self.conf.use_pub_sub and envelope.is_mult_send:
            LOG.debug("-> Redirecting request %s to TCP publisher", envelope)
            self.pub_publisher.send_request(message)
        elif not envelope.is_mult_send:
            self._redirect_message(message)

    @staticmethod
    def _receive_in_request(socket):
        reply_id = socket.recv()
        assert reply_id is not None, "Valid id expected"
        empty = socket.recv()
        assert empty == b'', "Empty delimiter expected"
        envelope = socket.recv_pyobj()
        payload = socket.recv_multipart()
        payload.insert(zmq_names.MULTIPART_IDX_ENVELOPE, envelope)
        return payload

    def _redirect_message(self, multipart_message):
        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]
        LOG.debug("<-> Route message: %s", envelope)
        response_binary = multipart_message[zmq_names.MULTIPART_IDX_BODY]

        self.router_socket.send(envelope.routing_key, zmq.SNDMORE)
        self.router_socket.send(b'', zmq.SNDMORE)
        self.router_socket.send_pyobj(envelope, zmq.SNDMORE)
        self.router_socket.send(response_binary)

    def cleanup(self):
        self.router_socket.close()
        self.pub_publisher.cleanup()
        self.matchmaker.unregister_publisher(
            (self.pub_publisher.host, self.router_address))
