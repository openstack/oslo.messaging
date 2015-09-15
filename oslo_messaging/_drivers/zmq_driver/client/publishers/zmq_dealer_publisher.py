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

from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LI, _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisher(zmq_publisher_base.PublisherMultisend):

    def __init__(self, conf, matchmaker):
        super(DealerPublisher, self).__init__(conf, matchmaker, zmq.DEALER)
        self.ack_receiver = AcknowledgementReceiver()

    def send_request(self, request):

        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        dealer_socket, hosts = self._check_hosts_connections(request.target)

        if not dealer_socket.connections:
            # NOTE(ozamiatin): Here we can provide
            # a queue for keeping messages to send them later
            # when some listener appears. However such approach
            # being more reliable will consume additional memory.
            LOG.warning(_LW("Request %s was dropped because no connection")
                        % request.msg_type)
            return

        self.ack_receiver.track_socket(dealer_socket.handle)

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(dealer_socket.connections_count()):
                self._send_request(dealer_socket, request)
        else:
            self._send_request(dealer_socket, request)

    def _send_request(self, socket, request):

        message_id = str(uuid.uuid1())

        socket.send(b'', zmq.SNDMORE)
        socket.send_string(request.msg_type, zmq.SNDMORE)
        socket.send_string(message_id, zmq.SNDMORE)
        socket.send_pyobj(request.context, zmq.SNDMORE)
        socket.send_pyobj(request.message)

        LOG.info(_LI("Sending message %(message)s to a target %(target)s")
                 % {"message": request.message,
                    "target": request.target})

    def cleanup(self):
        self.ack_receiver.cleanup()
        super(DealerPublisher, self).cleanup()


class AcknowledgementReceiver(object):

    def __init__(self):
        self.poller = zmq_async.get_poller()
        self.thread = zmq_async.get_executor(self.poll_for_acknowledgements)
        self.thread.execute()

    def _receive_acknowledgement(self, socket):
        empty = socket.recv()
        assert empty == b"", "Empty delimiter expected"
        ack_message = socket.recv_pyobj()
        return ack_message

    def track_socket(self, socket):
        self.poller.register(socket, self._receive_acknowledgement)

    def poll_for_acknowledgements(self):
        ack_message, socket = self.poller.poll()
        LOG.info(_LI("Message %s acknowledged")
                 % ack_message[zmq_names.FIELD_ID])

    def cleanup(self):
        self.thread.stop()
        self.poller.close()
