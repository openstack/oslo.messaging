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

from concurrent import futures
import logging

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver.client import zmq_sockets_manager
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherBase(zmq_publisher_base.PublisherBase):
    """Abstract DEALER-publisher."""

    def __init__(self, conf, matchmaker, sender, receiver):
        sockets_manager = zmq_sockets_manager.SocketsManager(
            conf, matchmaker, zmq.DEALER)
        super(DealerPublisherBase, self).__init__(
            sockets_manager, sender, receiver)

    def _check_reply(self, reply, request):
        assert isinstance(reply, zmq_response.Reply), "Reply expected!"

    def _finally_unregister(self, socket, request):
        self.receiver.untrack_request(request)

    def receive_reply(self, socket, request):
        self.receiver.register_socket(socket)
        _, reply_future = self.receiver.track_request(request)

        try:
            reply = reply_future.result(timeout=request.timeout)
            self._check_reply(reply, request)
        except AssertionError:
            LOG.error(_LE("Message format error in reply for %s"),
                      request.message_id)
            return None
        except futures.TimeoutError:
            self._raise_timeout(request)
        finally:
            self._finally_unregister(socket, request)

        if reply.failure:
            raise rpc_common.deserialize_remote_exception(
                reply.failure, request.allowed_remote_exmods)
        else:
            return reply.reply_body

    def cleanup(self):
        super(DealerPublisherBase, self).cleanup()
        self.sockets_manager.cleanup()
