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
import uuid

import six

from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_routing_table
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerPublisherProxy(zmq_dealer_publisher_base.DealerPublisherBase):
    """DEALER-publisher via proxy."""

    def __init__(self, conf, matchmaker):
        sender = zmq_senders.RequestSenderProxy(conf)
        if conf.oslo_messaging_zmq.rpc_use_acks:
            receiver = zmq_receivers.AckAndReplyReceiverProxy(conf)
        else:
            receiver = zmq_receivers.ReplyReceiverProxy(conf)
        super(DealerPublisherProxy, self).__init__(conf, matchmaker, sender,
                                                   receiver)
        self.socket = self.sockets_manager.get_socket_to_publishers(
            self._generate_identity())
        self.routing_table = zmq_routing_table.RoutingTable(self.conf,
                                                            self.matchmaker)
        self.connection_updater = \
            PublisherConnectionUpdater(self.conf, self.matchmaker, self.socket)

    def _generate_identity(self):
        return six.b(self.conf.oslo_messaging_zmq.rpc_zmq_host + "/" +
                     str(uuid.uuid4()))

    def _check_received_data(self, reply_id, reply, request):
        super(DealerPublisherProxy, self)._check_received_data(reply_id, reply,
                                                               request)
        assert reply_id == request.routing_key, \
            "Reply from recipient expected!"

    def _get_routing_keys(self, request):
        if request.msg_type in zmq_names.DIRECT_TYPES:
            return [self.routing_table.get_routable_host(request.target)]
        else:
            return \
                [zmq_address.target_to_subscribe_filter(request.target)] \
                if self.conf.oslo_messaging_zmq.use_pub_sub else \
                self.routing_table.get_all_hosts(request.target)

    def _send_request(self, request):
        routing_keys = [routing_key
                        for routing_key in self._get_routing_keys(request)
                        if routing_key is not None]
        if not routing_keys:
            LOG.warning(_LW("Matchmaker contains no records for specified "
                            "target %(target)s. Dropping message %(msg_id)s.")
                        % {"target": request.target,
                           "msg_id": request.message_id})
            return None
        for routing_key in routing_keys:
            request.routing_key = routing_key
            self.sender.send(self.socket, request)
        return self.socket

    def cleanup(self):
        super(DealerPublisherProxy, self).cleanup()
        self.connection_updater.stop()
        self.socket.close()


class PublisherConnectionUpdater(zmq_updater.ConnectionUpdater):

    def _update_connection(self):
        publishers = self.matchmaker.get_publishers()
        for pub_address, router_address in publishers:
            self.socket.connect_to_host(router_address)
