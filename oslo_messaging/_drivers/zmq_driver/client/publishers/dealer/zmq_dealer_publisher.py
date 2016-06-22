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

from concurrent import futures
import logging

import retrying

import oslo_messaging
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisher(zmq_publisher_base.PublisherBase):
    """Non-CALL publisher using direct connections."""

    def send_request(self, request):
        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(
                zmq_names.message_type_str(request.msg_type)
            )

        try:
            socket = self.sockets_manager.get_socket(request.target)
        except retrying.RetryError:
            return

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(socket.connections_count()):
                self.sender.send(socket, request)
        else:
            self.sender.send(socket, request)


class DealerCallPublisher(zmq_publisher_base.PublisherBase):
    """CALL publisher using direct connections."""

    def __init__(self, sockets_manager, sender, reply_receiver):
        super(DealerCallPublisher, self).__init__(sockets_manager, sender)
        self.reply_receiver = reply_receiver

    @staticmethod
    def _raise_timeout(request):
        raise oslo_messaging.MessagingTimeout(
            "Timeout %(tout)s seconds was reached for message %(msg_id)s" %
            {"tout": request.timeout, "msg_id": request.message_id}
        )

    def send_request(self, request):
        if request.msg_type != zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(
                zmq_names.message_type_str(request.msg_type)
            )

        try:
            socket = self._connect_socket(request.target)
        except retrying.RetryError:
            self._raise_timeout(request)

        self.sender.send(socket, request)
        self.reply_receiver.register_socket(socket)
        return self._recv_reply(request)

    def _connect_socket(self, target):
        return self.sockets_manager.get_socket(target)

    def _recv_reply(self, request):
        reply_future, = self.reply_receiver.track_request(request)

        try:
            _, reply = reply_future.result(timeout=request.timeout)
        except AssertionError:
            LOG.error(_LE("Message format error in reply for %s"),
                      request.message_id)
            return None
        except futures.TimeoutError:
            self._raise_timeout(request)
        finally:
            self.reply_receiver.untrack_request(request)

        if reply.failure:
            raise rpc_common.deserialize_remote_exception(
                reply.failure, request.allowed_remote_exmods
            )
        else:
            return reply.reply_body

    def cleanup(self):
        self.reply_receiver.stop()
        super(DealerCallPublisher, self).cleanup()
