#    Copyright 2016 Mirantis, Inc.
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

import abc
from concurrent import futures
import logging

import oslo_messaging
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver.client import zmq_sockets_manager
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherBase(zmq_publisher_base.PublisherBase):
    """Abstract DEALER-publisher."""

    def __init__(self, conf, matchmaker, sender, receiver):
        sockets_manager = zmq_sockets_manager.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER
        )
        super(DealerPublisherBase, self).__init__(sockets_manager, sender,
                                                  receiver)

    @staticmethod
    def _check_pattern(request, supported_pattern):
        if request.msg_type != supported_pattern:
            raise zmq_publisher_base.UnsupportedSendPattern(
                zmq_names.message_type_str(request.msg_type)
            )

    @staticmethod
    def _raise_timeout(request):
        raise oslo_messaging.MessagingTimeout(
            "Timeout %(tout)s seconds was reached for message %(msg_id)s" %
            {"tout": request.timeout, "msg_id": request.message_id}
        )

    def _recv_reply(self, request):
        reply_future = \
            self.receiver.track_request(request)[zmq_names.REPLY_TYPE]

        try:
            _, reply = reply_future.result(timeout=request.timeout)
            assert isinstance(reply, zmq_response.Reply), "Reply expected!"
        except AssertionError:
            LOG.error(_LE("Message format error in reply for %s"),
                      request.message_id)
            return None
        except futures.TimeoutError:
            self._raise_timeout(request)
        finally:
            self.receiver.untrack_request(request)

        if reply.failure:
            raise rpc_common.deserialize_remote_exception(
                reply.failure, request.allowed_remote_exmods
            )
        else:
            return reply.reply_body

    def send_call(self, request):
        self._check_pattern(request, zmq_names.CALL_TYPE)

        socket = self.connect_socket(request)
        if not socket:
            self._raise_timeout(request)

        self.sender.send(socket, request)
        self.receiver.register_socket(socket)
        return self._recv_reply(request)

    @abc.abstractmethod
    def _send_non_blocking(self, request):
        pass

    def send_cast(self, request):
        self._check_pattern(request, zmq_names.CAST_TYPE)
        self._send_non_blocking(request)

    def send_fanout(self, request):
        self._check_pattern(request, zmq_names.CAST_FANOUT_TYPE)
        self._send_non_blocking(request)

    def send_notify(self, request):
        self._check_pattern(request, zmq_names.NOTIFY_TYPE)
        self._send_non_blocking(request)
