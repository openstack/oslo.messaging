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

from oslo_messaging._drivers.common import RPCException
import oslo_messaging._drivers.zmq_driver.broker.zmq_base_proxy as base_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_topic
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class CallProxy(base_proxy.BaseProxy):

    def __init__(self, conf, context):
        super(CallProxy, self).__init__(conf, context)
        self.tcp_frontend = FrontendTcpRouter(self.conf, context)
        self.backend_matcher = DealerBackend(self.conf, context)
        LOG.info(_LI("Starting call proxy thread"))

    def run(self):
        message = self.tcp_frontend.receive_incoming()
        if message is not None:
            self.backend_matcher.redirect_to_backend(message)

        reply, socket = self.backend_matcher.receive_outgoing_reply()
        if reply is not None:
            self.tcp_frontend.redirect_outgoing_reply(reply)


class DealerBackend(base_proxy.DirectBackendMatcher):

    def __init__(self, conf, context):
        super(DealerBackend, self).__init__(conf,
                                            zmq_async.get_poller(),
                                            context)
        self.backend = self.context.socket(zmq.DEALER)
        self.poller.register(self.backend)

    def receive_outgoing_reply(self):
        reply_message = self.poller.poll(1)
        return reply_message

    def _get_topic(self, message):
        topic, server = zmq_serializer.get_topic_from_call_message(message)
        return zmq_topic.Topic(self.conf, topic, server)

    def _get_ipc_address(self, topic):
        return zmq_topic.get_ipc_address_call(self.conf, topic)

    def _send_message(self, backend, message, topic):
        # Empty needed for awaiting REP socket to work properly
        # (DEALER-REP usage specific)
        backend.send(b'', zmq.SNDMORE)
        backend.send_multipart(message)

    def _create_backend(self, ipc_address):
        self.backend.connect(ipc_address)
        self.backends[str(ipc_address)] = True


class FrontendTcpRouter(base_proxy.BaseTcpFrontend):

    def __init__(self, conf, context):
        super(FrontendTcpRouter, self).__init__(conf,
                                                zmq_async.get_poller(),
                                                context,
                                                socket_type=zmq.ROUTER,
                                                port_number=conf.rpc_zmq_port)

    @staticmethod
    def _reduce_empty(reply):
        reply.pop(0)
        return reply

    def redirect_outgoing_reply(self, reply):
        self._reduce_empty(reply)
        try:
            self.frontend.send_multipart(reply)
            LOG.info(_LI("Redirecting reply to client %s") % reply)
        except zmq.ZMQError:
            errmsg = _LE("Failed redirecting reply to client %s") % reply
            LOG.error(errmsg)
            raise RPCException(errmsg)
