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

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LI, _LW

zmq = zmq_async.import_zmq()

LOG = logging.getLogger(__name__)


class DealerPublisherProxy(zmq_dealer_publisher.DealerPublisher):

    def __init__(self, conf, matchmaker, reply_receiver):
        super(DealerPublisherProxy, self).__init__(conf, matchmaker)
        self.reply_receiver = reply_receiver

    def send_request(self, multipart_message):

        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]

        LOG.debug("Envelope: %s" % envelope)

        target = envelope[zmq_names.FIELD_TARGET]
        dealer_socket = self._check_hosts_connections(
            target, zmq_names.socket_type_str(zmq.ROUTER))

        if not dealer_socket.connections:
            # NOTE(ozamiatin): Here we can provide
            # a queue for keeping messages to send them later
            # when some listener appears. However such approach
            # being more reliable will consume additional memory.
            LOG.warning(_LW("Request %s was dropped because no connection")
                        % envelope[zmq_names.FIELD_MSG_TYPE])
            return

        self.reply_receiver.track_socket(dealer_socket.handle)

        LOG.debug("Sending message %(message)s to a target %(target)s"
                  % {"message": envelope[zmq_names.FIELD_MSG_ID],
                     "target": envelope[zmq_names.FIELD_TARGET]})

        if envelope[zmq_names.FIELD_MSG_TYPE] in zmq_names.MULTISEND_TYPES:
            for _ in range(dealer_socket.connections_count()):
                self._send_request(dealer_socket, multipart_message)
        else:
            self._send_request(dealer_socket, multipart_message)

    def _send_request(self, socket, multipart_message):

        socket.send(b'', zmq.SNDMORE)
        socket.send_pyobj(
            multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE],
            zmq.SNDMORE)
        socket.send(multipart_message[zmq_names.MULTIPART_IDX_BODY])


class ReplyReceiver(object):

    def __init__(self, poller):
        self.poller = poller
        LOG.info(_LI("Reply waiter created in broker"))

    def _receive_reply(self, socket):
        return socket.recv_multipart()

    def track_socket(self, socket):
        self.poller.register(socket, self._receive_reply)

    def cleanup(self):
        self.poller.close()
