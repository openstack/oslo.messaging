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

import tenacity

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_routing_table
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.client import zmq_sockets_manager
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names


LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherDirect(zmq_dealer_publisher_base.DealerPublisherBase):
    """DEALER-publisher using direct dynamic connections.

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
        sender = zmq_senders.RequestSenderDirect(conf, use_async=True)
        receiver = zmq_receivers.ReceiverDirect(conf)
        super(DealerPublisherDirect, self).__init__(conf, matchmaker,
                                                    sender, receiver)

        self.routing_table = zmq_routing_table.RoutingTableAdaptor(
            conf, matchmaker, zmq.ROUTER)

    def _get_round_robin_host_connection(self, target, socket):
        host = self.routing_table.get_round_robin_host(target)
        socket.connect_to_host(host)
        failover_hosts = self.routing_table.get_all_round_robin_hosts(target)
        upper_bound = self.conf.oslo_messaging_zmq.zmq_failover_connections
        for host in failover_hosts[:upper_bound]:
            socket.connect_to_host(host)

    def _get_fanout_connection(self, target, socket):
        for host in self.routing_table.get_fanout_hosts(target):
            socket.connect_to_host(host)

    def acquire_connection(self, request):
        if request.msg_type in zmq_names.DIRECT_TYPES:
            socket = self.sockets_manager.get_socket()
            self._get_round_robin_host_connection(request.target, socket)
            return socket
        elif request.msg_type in zmq_names.MULTISEND_TYPES:
            socket = self.sockets_manager.get_socket(immediate=False)
            self._get_fanout_connection(request.target, socket)
            return socket

    def _finally_unregister(self, socket, request):
        super(DealerPublisherDirect, self)._finally_unregister(socket, request)
        self.receiver.unregister_socket(socket)

    def send_request(self, socket, request):
        if hasattr(request, 'timeout'):
            _stop = tenacity.stop_after_delay(request.timeout)
        elif request.retry is not None and request.retry > 0:
            # no rpc_response_timeout option if notification
            _stop = tenacity.stop_after_attempt(request.retry)
        else:
            # well, now what?
            _stop = tenacity.stop_after_delay(60)

        @tenacity.retry(retry=tenacity.retry_if_exception_type(zmq.Again),
                        stop=_stop)
        def send_retrying():
            if request.msg_type in zmq_names.MULTISEND_TYPES:
                for _ in range(socket.connections_count()):
                    self.sender.send(socket, request)
            else:
                self.sender.send(socket, request)
        return send_retrying()

    def cleanup(self):
        self.routing_table.cleanup()
        super(DealerPublisherDirect, self).cleanup()


class DealerPublisherDirectStatic(DealerPublisherDirect):
    """DEALER-publisher using direct static connections.

    For some reason direct static connections may be also useful.
    Assume a case when some agents are not connected with control services
    over RPC (Ironic or Cinder+Ceph), and RPC is used only between controllers.
    In this case number of RPC connections doesn't matter (very small) so we
    can use static connections without fear and have all performance benefits
    from it.
    """

    def __init__(self, conf, matchmaker):
        super(DealerPublisherDirectStatic, self).__init__(conf, matchmaker)
        self.fanout_sockets = zmq_sockets_manager.SocketsManager(
            conf, matchmaker, zmq.DEALER)

    def acquire_connection(self, request):
        target_key = zmq_address.target_to_key(
            request.target, zmq_names.socket_type_str(zmq.ROUTER))
        if request.msg_type in zmq_names.MULTISEND_TYPES:
            hosts = self.routing_table.get_fanout_hosts(request.target)
            return self.fanout_sockets.get_cached_socket(target_key, hosts,
                                                         immediate=False)
        else:
            hosts = self.routing_table.get_all_round_robin_hosts(
                request.target)
            return self.sockets_manager.get_cached_socket(target_key, hosts)

    def _finally_unregister(self, socket, request):
        self.receiver.untrack_request(request)

    def cleanup(self):
        self.fanout_sockets.cleanup()
        super(DealerPublisherDirectStatic, self).cleanup()
