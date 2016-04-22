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
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()

LOG = logging.getLogger(__name__)


class DealerPublisherProxy(object):

    def __init__(self, conf, matchmaker, poller):
        super(DealerPublisherProxy, self).__init__()
        self.conf = conf
        self.matchmaker = matchmaker
        self.poller = poller
        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)

    def send_request(self, multipart_message):
        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]
        if envelope.is_mult_send:
            raise zmq_publisher_base.UnsupportedSendPattern(envelope.msg_type)
        if not envelope.target_hosts:
            raise Exception("Target hosts are expected!")

        dealer_socket = self.sockets_manager.get_socket_to_hosts(
            envelope.target, envelope.target_hosts)
        self.poller.register(dealer_socket.handle, self.receive_reply)

        LOG.debug("Sending message %(message)s to a target %(target)s"
                  % {"message": envelope.message_id,
                     "target": envelope.target})

        # Empty delimiter - DEALER socket specific
        dealer_socket.send(b'', zmq.SNDMORE)
        dealer_socket.send_pyobj(envelope, zmq.SNDMORE)
        dealer_socket.send(multipart_message[zmq_names.MULTIPART_IDX_BODY])

    def receive_reply(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty expected!"
        envelope = socket.recv_pyobj()
        assert envelope is not None, "Invalid envelope!"
        reply = socket.recv()
        LOG.debug("Received reply %s", envelope)
        return [envelope, reply]

    def cleanup(self):
        self.sockets_manager.cleanup()
