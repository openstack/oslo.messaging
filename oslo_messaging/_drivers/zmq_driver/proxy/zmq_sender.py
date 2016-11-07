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
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class Sender(object):

    @abc.abstractmethod
    def send_message(self, socket, multipart_message):
        """Send message to a socket from a multipart list."""


class CentralSender(Sender):

    def __init__(self):
        self._send_message_versions = \
            zmq_version.get_method_versions(self, 'send_message')

    def send_message(self, socket, multipart_message):
        message_version = multipart_message[zmq_names.MESSAGE_VERSION_IDX]
        if six.PY3:
            message_version = message_version.decode('utf-8')

        send_message_version = self._send_message_versions.get(message_version)
        if send_message_version is None:
            LOG.warning(_LW("Dropping message with unsupported version %s"),
                        message_version)
            return

        send_message_version(socket, multipart_message)


class LocalSender(Sender):
    pass


class CentralRouterSender(CentralSender):

    def _send_message_v_1_0(self, socket, multipart_message):
        message_type = int(multipart_message[zmq_names.MESSAGE_TYPE_IDX])
        routing_key = multipart_message[zmq_names.ROUTING_KEY_IDX]
        reply_id = multipart_message[zmq_names.REPLY_ID_IDX]
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]
        message_version = multipart_message[zmq_names.MESSAGE_VERSION_IDX]

        socket.send(routing_key, zmq.SNDMORE)
        socket.send(b'', zmq.SNDMORE)
        socket.send(message_version, zmq.SNDMORE)
        socket.send(reply_id, zmq.SNDMORE)
        socket.send(multipart_message[zmq_names.MESSAGE_TYPE_IDX], zmq.SNDMORE)
        socket.send_multipart(multipart_message[zmq_names.MESSAGE_ID_IDX:])

        LOG.debug("Dispatching %(msg_type)s message %(msg_id)s - from %(rid)s "
                  "-> to %(rkey)s (v%(msg_version)s)",
                  {"msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id,
                   "rkey": routing_key,
                   "rid": reply_id,
                   "msg_version": message_version})


class CentralAckSender(CentralSender):

    def _send_message_v_1_0(self, socket, multipart_message):
        message_type = zmq_names.ACK_TYPE
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]
        routing_key = socket.handle.identity
        reply_id = multipart_message[zmq_names.REPLY_ID_IDX]
        message_version = multipart_message[zmq_names.MESSAGE_VERSION_IDX]

        socket.send(reply_id, zmq.SNDMORE)
        socket.send(b'', zmq.SNDMORE)
        socket.send(message_version, zmq.SNDMORE)
        socket.send(routing_key, zmq.SNDMORE)
        socket.send(six.b(str(message_type)), zmq.SNDMORE)
        socket.send_string(message_id)

        LOG.debug("Sending %(msg_type)s for %(msg_id)s to %(rid)s "
                  "[from %(rkey)s] (v%(msg_version)s)",
                  {"msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id,
                   "rid": reply_id,
                   "rkey": routing_key,
                   "msg_version": message_version})


class CentralPublisherSender(CentralSender):

    def _send_message_v_1_0(self, socket, multipart_message):
        message_type = int(multipart_message[zmq_names.MESSAGE_TYPE_IDX])
        assert message_type in zmq_names.MULTISEND_TYPES, "Fanout expected!"
        topic_filter = multipart_message[zmq_names.ROUTING_KEY_IDX]
        message_id = multipart_message[zmq_names.MESSAGE_ID_IDX]
        message_version = multipart_message[zmq_names.MESSAGE_VERSION_IDX]

        socket.send(topic_filter, zmq.SNDMORE)
        socket.send(message_version, zmq.SNDMORE)
        socket.send(six.b(str(message_type)), zmq.SNDMORE)
        socket.send_multipart(multipart_message[zmq_names.MESSAGE_ID_IDX:])

        LOG.debug("Publishing message %(msg_id)s on [%(topic)s] "
                  "(v%(msg_version)s)",
                  {"topic": topic_filter,
                   "msg_id": message_id,
                   "msg_version": message_version})


class LocalPublisherSender(LocalSender):

    TOPIC_IDX = 0
    MSG_VERSION_IDX = 1
    MSG_TYPE_IDX = 2
    MSG_ID_IDX = 3

    def send_message(self, socket, multipart_message):
        socket.send_multipart(multipart_message)

        LOG.debug("Publishing message %(msg_id)s on [%(topic)s] "
                  "(v%(msg_version)s)",
                  {"topic": multipart_message[self.TOPIC_IDX],
                   "msg_id": multipart_message[self.MSG_ID_IDX],
                   "msg_version": multipart_message[self.MSG_VERSION_IDX]})
