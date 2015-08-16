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

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class PullIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, context, message):
        super(PullIncomingMessage, self).__init__(listener, context, message)

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for non-call messages."""

    def acknowledge(self):
        """Acknowledgments are not supported by this type of consumer."""

    def requeue(self):
        """Requeueing is not supported."""


class PullConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        super(PullConsumer, self).__init__(conf, poller, server, zmq.PULL)

    def listen(self, target):
        LOG.info(_LI("Listen to target %s") % str(target))
        #  Do nothing here because we have a single socket

    def receive_message(self, socket):
        try:
            msg_type = socket.recv_string()
            assert msg_type is not None, 'Bad format: msg type expected'
            context = socket.recv_pyobj()
            message = socket.recv_pyobj()
            LOG.info(_LI("Received %(msg_type)s message %(msg)s")
                     % {"msg_type": msg_type,
                        "msg": str(message)})

            if msg_type in (zmq_names.CAST_TYPES + zmq_names.NOTIFY_TYPES):
                return PullIncomingMessage(self.server, context, message)
            else:
                LOG.error(_LE("Unknown message type: %s") % msg_type)

        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed: %s") % str(e))
