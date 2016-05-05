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

    def send_request(self, multipart_message):

        envelope = multipart_message[zmq_names.MULTIPART_IDX_ENVELOPE]
        if not envelope.is_mult_send:
            raise zmq_publisher_base.UnsupportedSendPattern(envelope.msg_type)

        topic_filter = envelope.topic_filter

        self.socket.send(topic_filter, zmq.SNDMORE)
        self.socket.send(multipart_message[zmq_names.MULTIPART_IDX_BODY])

        LOG.debug("Publishing message [%(topic)s] %(message_id)s to "
                  "a target %(target)s ",
                  {"message_id": envelope.message_id,
                   "target": envelope.target,
                   "topic": topic_filter})

    def cleanup(self):
        self.socket.close()
