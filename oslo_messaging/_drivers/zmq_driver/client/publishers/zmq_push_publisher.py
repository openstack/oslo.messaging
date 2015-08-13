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


class PushPublisher(zmq_publisher_base.PublisherMultisend):

    def __init__(self, conf, matchmaker):
        super(PushPublisher, self).__init__(conf, matchmaker, zmq.PUSH)

    def send_request(self, request):

        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        push_socket, hosts = self._check_hosts_connections(request.target)

        if not push_socket.connections:
            LOG.warning(_LW("Request %s was dropped because no connection")
                        % request.msg_type)
            return

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(push_socket.connections_count()):
                self._send_request(push_socket, request)
        else:
            self._send_request(push_socket, request)

    def _send_request(self, socket, request):

        super(PushPublisher, self)._send_request(socket, request)

        LOG.info(_LI("Publishing message %(message)s to a target %(target)s")
                 % {"message": request.message,
                    "target": request.target})
