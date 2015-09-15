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

import copy
import logging

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_router_consumer
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqServer(base.Listener):

    def __init__(self, driver, conf, matchmaker=None):
        super(ZmqServer, self).__init__(driver)
        self.matchmaker = matchmaker
        self.poller = zmq_async.get_poller()
        self.rpc_consumer = zmq_router_consumer.RouterConsumer(
            conf, self.poller, self)
        self.notify_consumer = self.rpc_consumer
        self.consumers = [self.rpc_consumer]

    def poll(self, timeout=None):
        message, socket = self.poller.poll(
            timeout or self.conf.rpc_poll_timeout)
        return message

    def stop(self):
        consumer = self.rpc_consumer
        LOG.info("Stop server %s:%d" % (consumer.address, consumer.port))

    def cleanup(self):
        self.poller.close()
        for consumer in self.consumers:
            consumer.cleanup()

    def listen(self, target):

        consumer = self.rpc_consumer
        consumer.listen(target)

        LOG.info("Listen to target %s on %s:%d" %
                 (target, consumer.address, consumer.port))

        host = zmq_address.combine_address(self.conf.rpc_zmq_host,
                                           consumer.port)
        self.matchmaker.register(target=target,
                                 hostname=host)

    def listen_notification(self, targets_and_priorities):

        consumer = self.notify_consumer

        LOG.info("Listen for notifications on %s:%d"
                 % (consumer.address, consumer.port))

        for target, priority in targets_and_priorities:
            host = zmq_address.combine_address(self.conf.rpc_zmq_host,
                                               consumer.port)
            t = copy.deepcopy(target)
            t.topic = target.topic + '.' + priority
            self.matchmaker.register(target=t, hostname=host)
            consumer.listen(t)
