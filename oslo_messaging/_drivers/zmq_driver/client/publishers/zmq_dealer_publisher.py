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

    def send_request(self, request):

        self._check_request_pattern(request)

        dealer_socket, hosts = self._check_hosts_connections(
            request.target, zmq_names.socket_type_str(zmq.ROUTER))

        if not dealer_socket.connections:
            # NOTE(ozamiatin): Here we can provide
            # a queue for keeping messages to send them later
            # when some listener appears. However such approach
            # being more reliable will consume additional memory.
            LOG.warning(_LW("Request %s was dropped because no connection")
                        % request.msg_type)
            return

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(dealer_socket.connections_count()):
                self._send_request(dealer_socket, request)
        else:
            self._send_request(dealer_socket, request)

    def _check_request_pattern(self, request):
        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

    def _send_request(self, socket, request):

        socket.send(b'', zmq.SNDMORE)
        socket.send_pyobj(request)

        LOG.info(_LI("Sending message_id %(message)s to a target %(target)s")
                 % {"message": request.message_id,
                    "target": request.target})

    def cleanup(self):
        super(DealerPublisher, self).cleanup()


class DealerPublisherLight(zmq_publisher_base.PublisherBase):

    def __init__(self, conf, address):
        super(DealerPublisherLight, self).__init__(conf)
        self.socket = self.zmq_context.socket(zmq.DEALER)
        self.socket.connect(address)

    def send_request(self, request):

        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        envelope = request.create_envelope()

        self.socket.send(b'', zmq.SNDMORE)
        self.socket.send_pyobj(envelope, zmq.SNDMORE)
        self.socket.send_pyobj(request)

    def cleanup(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()


class DealerPublisherProxy(DealerPublisher):

    def __init__(self, conf, matchmaker, reply_receiver):
        super(DealerPublisherProxy, self).__init__(conf, matchmaker)
        self.reply_receiver = reply_receiver

    def send_request(self, multipart_message):

        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]

        LOG.info(_LI("Envelope: %s") % envelope)

        target = envelope[zmq_names.FIELD_TARGET]
        dealer_socket, hosts = self._check_hosts_connections(
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

        LOG.info(_LI("Sending message %(message)s to a target %(target)s")
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
