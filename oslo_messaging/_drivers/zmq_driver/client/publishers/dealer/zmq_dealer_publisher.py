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
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisher(zmq_publisher_base.QueuedSender):

    def __init__(self, conf, matchmaker):

        def _send_message_data(socket, request):
            socket.send(b'', zmq.SNDMORE)
            socket.send_pyobj(request)

            LOG.debug("Sent message_id %(message)s to a target %(target)s",
                      {"message": request.message_id,
                       "target": request.target})

        def _do_send_request(socket, request):
            if request.msg_type in zmq_names.MULTISEND_TYPES:
                for _ in range(socket.connections_count()):
                    _send_message_data(socket, request)
            else:
                _send_message_data(socket, request)

        sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)
        super(DealerPublisher, self).__init__(sockets_manager,
                                              _do_send_request)

    def send_request(self, request):
        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)
        super(DealerPublisher, self).send_request(request)

    def cleanup(self):
        super(DealerPublisher, self).cleanup()


class DealerPublisherLight(zmq_publisher_base.QueuedSender):
    """Used when publishing to proxy. """

    def __init__(self, conf, matchmaker):

        def _do_send_request(socket, request):
            if request.msg_type == zmq_names.CALL_TYPE:
                raise zmq_publisher_base.UnsupportedSendPattern(
                    request.msg_type)

            envelope = request.create_envelope()

            socket.send(b'', zmq.SNDMORE)
            socket.send_pyobj(envelope, zmq.SNDMORE)
            socket.send_pyobj(request)

            LOG.debug("->[proxy:%(addr)s] Sending message_id %(message)s to "
                      "a target %(target)s",
                      {"message": request.message_id,
                       "target": request.target,
                       "addr": zmq_address.get_broker_address(conf)})

        sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)
        super(DealerPublisherLight, self).__init__(
            sockets_manager, _do_send_request)
        self.socket = self.outbound_sockets.get_socket_to_broker()

    def _connect_socket(self, target):
        return self.socket

    def cleanup(self):
        self.socket.close()
