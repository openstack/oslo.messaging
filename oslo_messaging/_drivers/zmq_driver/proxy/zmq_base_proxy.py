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

import logging
import uuid

import six

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._i18n import _LI, _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


def check_message_format(func):
    def _check_message_format(socket):
        try:
            return func(socket)
        except Exception as e:
            LOG.error(_LE("Received message with wrong format: %r. "
                          "Dropping invalid message"), e)
            # NOTE(gdavoian): drop the left parts of a broken message, since
            # they most likely will break the order of next messages' parts
            if socket.getsockopt(zmq.RCVMORE):
                socket.recv_multipart()
    return _check_message_format


def create_socket(conf, context, port, socket_type):
    host = conf.zmq_proxy_opts.host
    identity = six.b(host) + b"/zmq-proxy/" + six.b(str(uuid.uuid4()))
    if port != 0:
        return zmq_socket.ZmqFixedPortSocket(conf, context, socket_type,
                                             host, port, identity=identity)
    else:
        return zmq_socket.ZmqRandomPortSocket(conf, context, socket_type,
                                              host, identity=identity)


class ProxyBase(object):

    PROXY_TYPE = "UNDEFINED"

    def __init__(self, conf, context, matchmaker):
        self.conf = conf
        self.context = context
        self.matchmaker = matchmaker

        LOG.info(_LI("Running %s proxy") % self.PROXY_TYPE)

        self.poller = zmq_async.get_poller()

    @staticmethod
    @check_message_format
    def _receive_message(socket):
        message = socket.recv_multipart()
        assert message[zmq_names.EMPTY_IDX] == b'', "Empty delimiter expected!"
        message_type = int(message[zmq_names.MESSAGE_TYPE_IDX])
        assert message_type in zmq_names.MESSAGE_TYPES, \
            "Known message type expected!"
        assert len(message) > zmq_names.MESSAGE_ID_IDX, \
            "At least %d parts expected!" % (zmq_names.MESSAGE_ID_IDX + 1)
        return message

    def cleanup(self):
        self.poller.close()
