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
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class PubPublisherProxy(object):
    """PUB/SUB based request publisher

        The publisher intended to be used for Fanout and Notify
        multi-sending patterns.

        It differs from direct publishers like DEALER or PUSH based
        in a way it treats matchmaker. Here all publishers register
        in the matchmaker. Subscribers (server-side) take the list
        of publishers and connect to all of them but subscribe
        only to a specific topic-filtering tag generated from the
        Target object.
    """

    def __init__(self, conf, matchmaker):
        super(PubPublisherProxy, self).__init__()
        self.conf = conf
        self.zmq_context = zmq.Context()
        self.matchmaker = matchmaker

        self.socket = zmq_socket.ZmqRandomPortSocket(
            self.conf, self.zmq_context, zmq.PUB)

        self.host = zmq_address.combine_address(self.conf.rpc_zmq_host,
                                                self.socket.port)

        self.sync_channel = SyncChannel(conf, matchmaker, self.zmq_context)

        LOG.info(_LI("[PUB:%(pub)s, PULL:%(pull)s] Run PUB publisher"),
                 {"pub": self.host,
                  "pull": self.sync_channel.sync_host})

        self.matchmaker.register_publisher(
            (self.host, self.sync_channel.sync_host))

    def send_request(self, multipart_message):

        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]
        msg_type = envelope[zmq_names.FIELD_MSG_TYPE]
        target = envelope[zmq_names.FIELD_TARGET]
        message_id = envelope[zmq_names.FIELD_MSG_ID]
        if msg_type not in zmq_names.MULTISEND_TYPES:
            raise zmq_publisher_base.UnsupportedSendPattern(msg_type)

        topic_filter = zmq_address.target_to_subscribe_filter(target)

        self.socket.send(topic_filter, zmq.SNDMORE)
        self.socket.send(multipart_message[zmq_names.MULTIPART_IDX_BODY])

        LOG.debug("Publishing message [%(topic)s] %(message_id)s to "
                  "a target %(target)s ",
                  {"message_id": message_id,
                   "target": target,
                   "topic": topic_filter})

    def cleanup(self):
        self.matchmaker.unregister_publisher(
            (self.host, self.sync_channel.sync_host))
        self.socket.close()


class SyncChannel(object):
    """Subscribers synchronization channel

        As far as PUB/SUB is one directed way pattern we need some
        backwards channel to have a possibility of subscribers
        to talk back to publisher.

        May be used for heartbeats or some kind of acknowledgments etc.
    """

    def __init__(self, conf, matchmaker, context):
        self.conf = conf
        self.matchmaker = matchmaker
        self.context = context
        self._ready = None

        #  NOTE(ozamiatin): May be used for heartbeats when we
        #  implement them
        self.sync_socket = zmq_socket.ZmqRandomPortSocket(
            self.conf, self.context, zmq.PULL)
        self.poller = zmq_async.get_poller()
        self.poller.register(self.sync_socket)

        self.sync_host = zmq_address.combine_address(self.conf.rpc_zmq_host,
                                                     self.sync_socket.port)

    def is_ready(self):
        LOG.debug("[%s] Waiting for ready from first subscriber",
                  self.sync_host)
        if self._ready is None:
            self._ready = self.poller.poll()
            LOG.debug("[%s] Received ready from first subscriber",
                      self.sync_host)
        return self._ready is not None
