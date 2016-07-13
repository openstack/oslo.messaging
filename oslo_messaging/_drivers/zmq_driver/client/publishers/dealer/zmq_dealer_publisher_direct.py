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

import retrying

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherDirect(zmq_dealer_publisher_base.DealerPublisherBase):
    """DEALER-publisher using direct connections."""

    def __init__(self, conf, matchmaker):
        sender = zmq_senders.RequestSenderDirect(conf)
        receiver = zmq_receivers.ReplyReceiverDirect(conf)
        super(DealerPublisherDirect, self).__init__(conf, matchmaker, sender,
                                                    receiver)

    def _connect_socket(self, request):
        return self.sockets_manager.get_socket(request.target)

    def _send_non_blocking(self, request):
        try:
            socket = self._connect_socket(request)
        except retrying.RetryError:
            return

        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(socket.connections_count()):
                self.sender.send(socket, request)
        else:
            self.sender.send(socket, request)
