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

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class PushPublisher(object):

    def __init__(self, conf, matchmaker):
        super(PushPublisher, self).__init__()
        sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.PULL, zmq.PUSH)

        def _do_send_request(push_socket, request):
            push_socket.send_pyobj(request)

            LOG.debug("Sending message_id %(message)s to a target %(target)s",
                      {"message": request.message_id,
                       "target": request.target})

        self.sender = zmq_publisher_base.QueuedSender(
            sockets_manager, _do_send_request)

    def send_request(self, request):

        if request.msg_type != zmq_names.CAST_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        self.sender.send_request(request)

    def cleanup(self):
        self.sender.cleanup()
