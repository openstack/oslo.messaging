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

import abc
from abc import abstractmethod
import logging
import uuid

import six

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class Request(object):

    def __init__(self, conf, target, context, message,
                 socket, msg_type, timeout=None, retry=None):

        assert msg_type in zmq_serializer.MESSAGE_TYPES, "Unknown msg type!"

        if message['method'] is None:
            errmsg = _LE("No method specified for RPC call")
            LOG.error(errmsg)
            raise KeyError(errmsg)

        self.msg_id = uuid.uuid4().hex
        self.msg_type = msg_type
        self.target = target
        self.context = context
        self.message = message
        self.timeout = timeout or conf.rpc_response_timeout
        self.retry = retry
        self.reply = None
        self.socket = socket

    @property
    def is_replied(self):
        return self.reply is not None

    @property
    def is_timed_out(self):
        return False

    def send_request(self):
        self.socket.send_string(self.msg_type, zmq.SNDMORE)
        self.socket.send_json(self.target.__dict__, zmq.SNDMORE)
        self.socket.send_string(self.msg_id, zmq.SNDMORE)
        self.socket.send_json(self.context, zmq.SNDMORE)
        self.socket.send_json(self.message)

    def __call__(self):
        self.send_request()
        return self.receive_reply()

    @abstractmethod
    def receive_reply(self):
        "Receive reply from server side"
