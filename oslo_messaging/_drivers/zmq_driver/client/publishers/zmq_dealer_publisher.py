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

        if request.msg_type == zmq_names.CALL_TYPE:
            raise zmq_publisher_base.UnsupportedSendPattern(request.msg_type)

        dealer_socket, hosts = self._check_hosts_connections(request.target)

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(dealer_socket.connections_count()):
                self._send_request(dealer_socket, request)
        else:
            self._send_request(dealer_socket, request)

    def _send_request(self, socket, request):

        if not socket.connections:
            # NOTE(ozamiatin): Here we can provide
            # a queue for keeping messages to send them later
            # when some listener appears. However such approach
            # being more reliable will consume additional memory.
            LOG.warning(_LW("Request %s was dropped because no connection")
                        % request.msg_type)
            return

        socket.send(b'', zmq.SNDMORE)
        super(DealerPublisher, self)._send_request(socket, request)

        LOG.info(_LI("Sending message %(message)s to a target %(target)s")
                 % {"message": request.message,
                    "target": request.target})
