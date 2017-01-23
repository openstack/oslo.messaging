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

import random
import uuid

import six

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_routing_table
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater

zmq = zmq_async.import_zmq()


class DealerPublisherProxy(zmq_dealer_publisher_base.DealerPublisherBase):
    """DEALER-publisher via proxy."""

    def __init__(self, conf, matchmaker):
        sender = zmq_senders.RequestSenderProxy(conf)
        receiver = zmq_receivers.ReceiverProxy(conf)
        super(DealerPublisherProxy, self).__init__(conf, matchmaker,
                                                   sender, receiver)

        self.socket = self.sockets_manager.get_socket_to_publishers(
            self._generate_identity())
        self.routing_table = zmq_routing_table.RoutingTableAdaptor(
            conf, matchmaker, zmq.DEALER)
        self.connection_updater = PublisherConnectionUpdater(
            self.conf, self.matchmaker, self.socket)

    def _generate_identity(self):
        return six.b(self.conf.oslo_messaging_zmq.rpc_zmq_host + "/" +
                     str(uuid.uuid4()))

    def _check_reply(self, reply, request):
        super(DealerPublisherProxy, self)._check_reply(reply, request)
        assert reply.reply_id == request.routing_key, \
            "Reply from recipient expected!"

    def _get_routing_keys(self, request):
        if request.msg_type in zmq_names.DIRECT_TYPES:
            return [self.routing_table.get_round_robin_host(request.target)]
        else:
            return \
                [zmq_address.target_to_subscribe_filter(request.target)] \
                if self.conf.oslo_messaging_zmq.use_pub_sub else \
                self.routing_table.get_fanout_hosts(request.target)

    def acquire_connection(self, request):
        return self.socket

    def send_request(self, socket, request):
        for routing_key in self._get_routing_keys(request):
            request.routing_key = routing_key
            self.sender.send(socket, request)

    def cleanup(self):
        self.connection_updater.stop()
        self.routing_table.cleanup()
        super(DealerPublisherProxy, self).cleanup()


class PublisherConnectionUpdater(zmq_updater.ConnectionUpdater):

    def _update_connection(self):
        publishers = self.matchmaker.get_publishers()
        for pub_address, fe_router_address in publishers:
            self.socket.connect_to_host(fe_router_address)


class DealerPublisherProxyDynamic(
        zmq_dealer_publisher_base.DealerPublisherBase):

    def __init__(self, conf, matchmaker):
        sender = zmq_senders.RequestSenderProxy(conf)
        receiver = zmq_receivers.ReceiverDirect(conf)
        super(DealerPublisherProxyDynamic, self).__init__(conf, matchmaker,
                                                          sender, receiver)

        self.publishers = set()
        self.updater = DynamicPublishersUpdater(conf, matchmaker,
                                                self.publishers)
        self.updater.update_publishers()

    def acquire_connection(self, request):
        if not self.publishers:
            raise zmq_matchmaker_base.MatchmakerUnavailable()
        socket = self.sockets_manager.get_socket()
        publishers = list(self.publishers)
        random.shuffle(publishers)
        for publisher in publishers:
            socket.connect_to_host(publisher)
        return socket

    def send_request(self, socket, request):
        request.routing_key = \
            zmq_address.target_to_subscribe_filter(request.target)
        self.sender.send(socket, request)

    def cleanup(self):
        self.updater.cleanup()
        super(DealerPublisherProxyDynamic, self).cleanup()


class DynamicPublishersUpdater(zmq_updater.UpdaterBase):

    def __init__(self, conf, matchmaker, publishers):
        super(DynamicPublishersUpdater, self).__init__(
            conf, matchmaker, self.update_publishers,
            sleep_for=conf.oslo_messaging_zmq.zmq_target_update
        )
        self.publishers = publishers

    def update_publishers(self):
        publishers = self.matchmaker.get_publishers()
        for pub_address, fe_router_address in publishers:
            self.publishers.add(fe_router_address)
