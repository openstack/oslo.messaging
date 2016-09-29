#    Copyright 2015-2016 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_routing_table
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherDirect(zmq_dealer_publisher_base.DealerPublisherBase):
    """DEALER-publisher using direct connections.

    Publishing directly to remote services assumes the following:
        -   All direct connections are dynamic - so they live per message,
            thus each message send executes the following:
                *   Open a new socket
                *   Connect to some host got from the RoutingTable
                *   Send message(s)
                *   Close connection, destroy socket
        -   RoutingTable/RoutingTableUpdater implements local cache of
            matchmaker (e.g. Redis) for target resolution to the list of
            available hosts. Cache updates in a background thread.
        -   Caching of connections is not appropriate for directly connected
            OS services, because finally it results in a full-mesh of
            connections between services.
        -   Yes we lose on performance opening and closing connections
            for each message, but that is done intentionally to implement
            the dynamic connections concept. The key thought here is to
            have minimum number of connected services at the moment.
        -   Using the local RoutingTable cache is done to optimise access
            to the matchmaker so we don't call the matchmaker per each message
    """

    def __init__(self, conf, matchmaker):
        sender = zmq_senders.RequestSenderDirect(conf)
        if conf.oslo_messaging_zmq.rpc_use_acks:
            receiver = zmq_receivers.AckAndReplyReceiverDirect(conf)
        else:
            receiver = zmq_receivers.ReplyReceiverDirect(conf)
        super(DealerPublisherDirect, self).__init__(conf, matchmaker, sender,
                                                    receiver)

        self.routing_table = zmq_routing_table.RoutingTableAdaptor(
            conf, matchmaker, zmq.ROUTER)

    def _get_round_robin_host_connection(self, target, socket):
        host = self.routing_table.get_round_robin_host(target)
        socket.connect_to_host(host)

    def _get_fanout_connection(self, target, socket):
        for host in self.routing_table.get_fanout_hosts(target):
            socket.connect_to_host(host)

    def acquire_connection(self, request):
        socket = self.sockets_manager.get_socket()
        if request.msg_type in zmq_names.DIRECT_TYPES:
            self._get_round_robin_host_connection(request.target, socket)
        elif request.msg_type in zmq_names.MULTISEND_TYPES:
            self._get_fanout_connection(request.target, socket)
        return socket

    def _finally_unregister(self, socket, request):
        super(DealerPublisherDirect, self)._finally_unregister(socket, request)
        self.receiver.unregister_socket(socket)

    def send_request(self, socket, request):
        if request.msg_type in zmq_names.MULTISEND_TYPES:
            for _ in range(socket.connections_count()):
                self.sender.send(socket, request)
        else:
            self.sender.send(socket, request)

    def cleanup(self):
        self.routing_table.cleanup()
        super(DealerPublisherDirect, self).cleanup()
