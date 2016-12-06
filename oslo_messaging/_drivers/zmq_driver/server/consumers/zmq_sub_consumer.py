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
import uuid

import six

from oslo_messaging._drivers.zmq_driver.server.consumers \
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class SubConsumer(zmq_consumer_base.ConsumerBase):

    def __init__(self, conf, poller, server):
        super(SubConsumer, self).__init__(conf, poller, server)
        self.matchmaker = SubscriptionMatchmakerWrapper(conf,
                                                        server.matchmaker)
        self.target = server.target
        self.socket = zmq_socket.ZmqSocket(self.conf, self.context, zmq.SUB,
                                           immediate=False,
                                           identity=self._generate_identity())
        self.sockets.append(self.socket)
        self.host = self.socket.handle.identity
        self._subscribe_to_topic()
        self._receive_request_versions = \
            zmq_version.get_method_versions(self, 'receive_request')
        self.connection_updater = SubscriberConnectionUpdater(
            conf, self.matchmaker, self.socket)
        self.poller.register(self.socket, self.receive_request)
        LOG.info(_LI("[%s] Run SUB consumer"), self.host)

    def _generate_identity(self):
        return six.b(self.conf.oslo_messaging_zmq.rpc_zmq_host + '/') + \
            zmq_address.target_to_subscribe_filter(self.target) + \
            six.b('/' + str(uuid.uuid4()))

    def _subscribe_to_topic(self):
        topic_filter = zmq_address.target_to_subscribe_filter(self.target)
        self.socket.setsockopt(zmq.SUBSCRIBE, topic_filter)
        LOG.debug("[%(host)s] Subscribing to topic %(filter)s",
                  {"host": self.host, "filter": topic_filter})

    def _get_receive_request_version(self, version):
        receive_request_version = self._receive_request_versions.get(version)
        if receive_request_version is None:
            raise zmq_version.UnsupportedMessageVersionError(version)
        return receive_request_version

    def _receive_request_v_1_0(self, topic_filter, socket):
        message_type = int(socket.recv())
        assert message_type in zmq_names.MULTISEND_TYPES, "Fanout expected!"
        message_id = socket.recv()
        context, message = socket.recv_loaded()
        LOG.debug("[%(host)s] Received on topic %(filter)s message %(msg_id)s "
                  "(v%(msg_version)s)",
                  {'host': self.host,
                   'filter': topic_filter,
                   'msg_id': message_id,
                   'msg_version': '1.0'})
        return context, message

    def receive_request(self, socket):
        try:
            topic_filter = socket.recv()
            message_version = socket.recv_string()
            receive_request_version = \
                self._get_receive_request_version(message_version)
            context, message = receive_request_version(topic_filter, socket)
            return zmq_incoming_message.ZmqIncomingMessage(context, message)
        except (zmq.ZMQError, AssertionError, ValueError,
                zmq_version.UnsupportedMessageVersionError) as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))
            # NOTE(gdavoian): drop the left parts of a broken message
            if socket.getsockopt(zmq.RCVMORE):
                socket.recv_multipart()

    def cleanup(self):
        LOG.info(_LI("[%s] Destroy SUB consumer"), self.host)
        self.connection_updater.cleanup()
        super(SubConsumer, self).cleanup()


class SubscriptionMatchmakerWrapper(object):

    def __init__(self, conf, matchmaker):
        self.conf = conf
        self.matchmaker = matchmaker

    def get_publishers(self):
        conf_publishers = self.conf.oslo_messaging_zmq.subscribe_on
        LOG.debug("Publishers taken from configuration %s", conf_publishers)
        if conf_publishers:
            return [(publisher, None) for publisher in conf_publishers]
        return self.matchmaker.get_publishers()


class SubscriberConnectionUpdater(zmq_updater.ConnectionUpdater):

    def _update_connection(self):
        publishers = self.matchmaker.get_publishers()
        for publisher_address, router_address in publishers:
            self.socket.connect_to_host(publisher_address)
        LOG.debug("[%s] SUB consumer connected to publishers %s",
                  self.socket.handle.identity, publishers)
