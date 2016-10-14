#    Copyright 2015-2016 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver.proxy.central \
    import zmq_publisher_proxy
from oslo_messaging._drivers.zmq_driver.proxy \
    import zmq_base_proxy
from oslo_messaging._drivers.zmq_driver.proxy import zmq_sender
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class SingleRouterProxy(zmq_base_proxy.ProxyBase):

    PROXY_TYPE = "ROUTER"

    def __init__(self, conf, context, matchmaker):
        super(SingleRouterProxy, self).__init__(conf, context, matchmaker)

        port = conf.zmq_proxy_opts.frontend_port
        self.fe_router_socket = zmq_base_proxy.create_socket(
            conf, context, port, zmq.ROUTER)

        self.poller.register(self.fe_router_socket, self._receive_message)

        self.publisher = zmq_publisher_proxy.PublisherProxy(conf, matchmaker)

        self.router_sender = zmq_sender.CentralRouterSender()
        self.ack_sender = zmq_sender.CentralAckSender()

        self._router_updater = self._create_router_updater()

    def run(self):
        message, socket = self.poller.poll()
        if message is None:
            return

        message_type = int(message[zmq_names.MESSAGE_TYPE_IDX])
        if self.conf.oslo_messaging_zmq.use_pub_sub and \
                message_type in zmq_names.MULTISEND_TYPES:
            self.publisher.send_request(message)
            if socket is self.fe_router_socket and \
                    self.conf.zmq_proxy_opts.ack_pub_sub:
                self.ack_sender.send_message(socket, message)
        else:
            self.router_sender.send_message(
                self._get_socket_to_dispatch_on(socket), message)

    def _create_router_updater(self):
        return RouterUpdater(
            self.conf, self.matchmaker, self.publisher.host,
            self.fe_router_socket.connect_address,
            self.fe_router_socket.connect_address)

    def _get_socket_to_dispatch_on(self, socket):
        return self.fe_router_socket

    def cleanup(self):
        super(SingleRouterProxy, self).cleanup()
        self._router_updater.cleanup()
        self.fe_router_socket.close()
        self.publisher.cleanup()


class DoubleRouterProxy(SingleRouterProxy):

    PROXY_TYPE = "ROUTER-ROUTER"

    def __init__(self, conf, context, matchmaker):
        port = conf.zmq_proxy_opts.backend_port
        self.be_router_socket = zmq_base_proxy.create_socket(
            conf, context, port, zmq.ROUTER)
        super(DoubleRouterProxy, self).__init__(conf, context, matchmaker)
        self.poller.register(self.be_router_socket, self._receive_message)

    def _create_router_updater(self):
        return RouterUpdater(
            self.conf, self.matchmaker, self.publisher.host,
            self.fe_router_socket.connect_address,
            self.be_router_socket.connect_address)

    def _get_socket_to_dispatch_on(self, socket):
        return self.be_router_socket \
            if socket is self.fe_router_socket \
            else self.fe_router_socket

    def cleanup(self):
        super(DoubleRouterProxy, self).cleanup()
        self.be_router_socket.close()


class RouterUpdater(zmq_updater.UpdaterBase):
    """This entity performs periodic async updates
    from router proxy to the matchmaker.
    """

    def __init__(self, conf, matchmaker, publisher_address, fe_router_address,
                 be_router_address):
        self.publisher_address = publisher_address
        self.fe_router_address = fe_router_address
        self.be_router_address = be_router_address
        super(RouterUpdater, self).__init__(
            conf, matchmaker, self._update_records,
            conf.oslo_messaging_zmq.zmq_target_update)

    def _update_records(self):
        self.matchmaker.register_publisher(
            (self.publisher_address, self.fe_router_address),
            expire=self.conf.oslo_messaging_zmq.zmq_target_expire)
        LOG.info(_LI("[PUB:%(pub)s, ROUTER:%(router)s] Update PUB publisher"),
                 {"pub": self.publisher_address,
                  "router": self.fe_router_address})
        self.matchmaker.register_router(
            self.be_router_address,
            expire=self.conf.oslo_messaging_zmq.zmq_target_expire)
        LOG.info(_LI("[Backend ROUTER:%(router)s] Update ROUTER"),
                 {"router": self.be_router_address})

    def cleanup(self):
        super(RouterUpdater, self).cleanup()
        self.matchmaker.unregister_publisher(
            (self.publisher_address, self.fe_router_address))
        self.matchmaker.unregister_router(
            self.be_router_address)
