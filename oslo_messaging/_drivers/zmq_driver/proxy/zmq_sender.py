#    Copyright 2016 Mirantis, Inc.
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

import abc
import logging

import six

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class Sender(object):

    @abc.abstractmethod
    def send_message(self, socket, multipart_message):
        """Send message to a socket from multipart list"""


class CentralRouterSender(Sender):

    def send_message(self, socket, multipart_message):
        message_type = int(multipart_message[zmq_names.MESSAGE_TYPE_IDX])
        routing_key = multipart_message[zmq_names.ROUTING_KEY_IDX]
        reply_id = multipart_message[zmq_names.REPLY_ID_IDX]
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]

        socket.send(routing_key, zmq.SNDMORE)
        socket.send(b'', zmq.SNDMORE)
        socket.send(reply_id, zmq.SNDMORE)
        socket.send(multipart_message[zmq_names.MESSAGE_TYPE_IDX], zmq.SNDMORE)
        socket.send_multipart(multipart_message[zmq_names.MESSAGE_ID_IDX:])

        LOG.debug("Dispatching %(msg_type)s message %(msg_id)s - from %(rid)s "
                  "-> to %(rkey)s",
                  {"msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id,
                   "rkey": routing_key,
                   "rid": reply_id})


class CentralAckSender(Sender):

    def send_message(self, socket, multipart_message):
        message_type = zmq_names.ACK_TYPE
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]
        routing_key = socket.handle.identity
        reply_id = multipart_message[zmq_names.REPLY_ID_IDX]

        socket.send(reply_id, zmq.SNDMORE)
        socket.send(b'', zmq.SNDMORE)
        socket.send(routing_key, zmq.SNDMORE)
        socket.send(six.b(str(message_type)), zmq.SNDMORE)
        socket.send_string(message_id)

        LOG.debug("Sending %(msg_type)s for %(msg_id)s to %(rid)s "
                  "[from %(rkey)s]",
                  {"msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id,
                   "rid": reply_id,
                   "rkey": routing_key})


class CentralPublisherSender(Sender):

    def send_message(self, socket, multipart_message):
        message_type = int(multipart_message[zmq_names.MESSAGE_TYPE_IDX])
        assert message_type in zmq_names.MULTISEND_TYPES, "Fanout expected!"
        topic_filter = multipart_message[zmq_names.ROUTING_KEY_IDX]
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]

        socket.send(topic_filter, zmq.SNDMORE)
        socket.send_multipart(multipart_message[zmq_names.MESSAGE_ID_IDX:])

        LOG.debug("Publishing message %(message_id)s on [%(topic)s]",
                  {"topic": topic_filter,
                   "message_id": message_id})
