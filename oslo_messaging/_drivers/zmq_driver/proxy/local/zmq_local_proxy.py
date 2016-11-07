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

from oslo_messaging._drivers.zmq_driver.proxy.central \
    import zmq_publisher_proxy
from oslo_messaging._drivers.zmq_driver.proxy \
    import zmq_base_proxy
from oslo_messaging._drivers.zmq_driver.proxy import zmq_sender
from oslo_messaging._drivers.zmq_driver.server.consumers \
    import zmq_sub_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_socket


zmq = zmq_async.import_zmq()


class LocalPublisherProxy(zmq_base_proxy.ProxyBase):

    PROXY_TYPE = "L-PUBLISHER"

    def __init__(self, conf, context, matchmaker):
        wrapper = zmq_sub_consumer.SubscriptionMatchmakerWrapper(conf,
                                                                 matchmaker)
        super(LocalPublisherProxy, self).__init__(conf, context, wrapper)
        self.fe_sub = zmq_socket.ZmqSocket(conf, context, zmq.SUB, False)
        self.fe_sub.setsockopt(zmq.SUBSCRIBE, b'')
        self.connection_updater = zmq_sub_consumer.SubscriberConnectionUpdater(
            conf, self.matchmaker, self.fe_sub)
        self.poller.register(self.fe_sub, self.receive_message)
        self.publisher = zmq_publisher_proxy.PublisherProxy(
            conf, matchmaker, sender=zmq_sender.LocalPublisherSender())

    def run(self):
        message, socket = self.poller.poll()
        if message is None:
            return
        self.publisher.send_request(message)

    @staticmethod
    def receive_message(socket):
        return socket.recv_multipart()

    def cleanup(self):
        super(LocalPublisherProxy, self).cleanup()
        self.fe_sub.close()
        self.connection_updater.cleanup()
        self.publisher.cleanup()
