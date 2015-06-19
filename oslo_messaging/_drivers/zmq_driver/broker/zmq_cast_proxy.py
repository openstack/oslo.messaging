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

import oslo_messaging._drivers.zmq_driver.broker.zmq_base_proxy as base_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_topic
from oslo_messaging._i18n import _LI

zmq = zmq_async.import_zmq()

LOG = logging.getLogger(__name__)


class CastProxy(base_proxy.BaseProxy):

    def __init__(self, conf, context):
        super(CastProxy, self).__init__(conf, context)
        self.tcp_frontend = FrontendTcpPull(self.conf, context)
        self.backend_matcher = CastPushBackendMatcher(self.conf, context)
        LOG.info(_LI("Starting cast proxy thread"))

    def run(self):
        message = self.tcp_frontend.receive_incoming()
        if message is not None:
            self.backend_matcher.redirect_to_backend(message)


class FrontendTcpPull(base_proxy.BaseTcpFrontend):

    def __init__(self, conf, context):
        super(FrontendTcpPull, self).__init__(conf, zmq_async.get_poller(),
                                              context)
        self.frontend = self.context.socket(zmq.PULL)
        address = zmq_topic.get_tcp_bind_address(conf.rpc_zmq_fanout_port)
        LOG.info(_LI("Binding to TCP PULL %s") % address)
        self.frontend.bind(address)
        self.poller.register(self.frontend)

    def _receive_message(self):
        message = self.poller.poll()
        return message


class CastPushBackendMatcher(base_proxy.BaseBackendMatcher):

    def __init__(self, conf, context):
        super(CastPushBackendMatcher, self).__init__(conf,
                                                     zmq_async.get_poller(),
                                                     context)
        self.backend = self.context.socket(zmq.PUSH)

    def _get_topic(self, message):
        topic, server = zmq_serializer.get_topic_from_cast_message(message)
        return zmq_topic.Topic(self.conf, topic, server)

    def _get_ipc_address(self, topic):
        return zmq_topic.get_ipc_address_cast(self.conf, topic)

    def _send_message(self, backend, message, topic):
        backend.send_multipart(message)

    def _create_backend(self, ipc_address):
        LOG.debug("[Cast Proxy] Creating PUSH backend %s", ipc_address)
        self.backend.connect(ipc_address)
        self.backends[str(ipc_address)] = True
