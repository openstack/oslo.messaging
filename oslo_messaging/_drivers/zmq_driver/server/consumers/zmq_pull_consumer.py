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


class PullIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, context, message):
        super(PullIncomingMessage, self).__init__(context, message)

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for non-call messages."""

    def acknowledge(self):
        """Acknowledgments are not supported by this type of consumer."""

    def requeue(self):
        """Requeueing is not supported."""


class PullConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        super(PullConsumer, self).__init__(conf, poller, server, zmq.PULL)
        LOG.info(_LI("[%s] Run PULL consumer"), self.host)

    def receive_message(self, socket):
        try:
            request = socket.recv_pyobj()
            msg_type = request.msg_type
            assert msg_type is not None, 'Bad format: msg type expected'
            context = request.context
            message = request.message
            LOG.debug("[%(host)s] Received %(type)s, %(id)s, %(target)s",
                      {"host": self.host,
                       "type": request.msg_type,
                       "id": request.message_id,
                       "target": request.target})

            if msg_type in (zmq_names.CAST_TYPES + zmq_names.NOTIFY_TYPES):
                return PullIncomingMessage(context, message)
            else:
                LOG.error(_LE("Unknown message type: %s"), msg_type)

        except (zmq.ZMQError, AssertionError) as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))
