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
    import zmq_pull_consumer
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_router_consumer
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_sub_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqServer(base.Listener):

    def __init__(self, driver, conf, matchmaker=None):
        super(ZmqServer, self).__init__()
        self.driver = driver
        self.conf = conf
        self.matchmaker = matchmaker
        self.poller = zmq_async.get_poller()
        self.router_consumer = zmq_router_consumer.RouterConsumer(
            conf, self.poller, self)
        self.pull_consumer = zmq_pull_consumer.PullConsumer(
            conf, self.poller, self)
        self.sub_consumer = zmq_sub_consumer.SubConsumer(
            conf, self.poller, self) if conf.use_pub_sub else None
        self.notify_consumer = self.sub_consumer if conf.use_pub_sub \
            else self.router_consumer

        self.consumers = [self.router_consumer, self.pull_consumer]
        if self.sub_consumer:
            self.consumers.append(self.sub_consumer)

    @base.batch_poll_helper
    def poll(self, timeout=None):
        message, socket = self.poller.poll(
            timeout or self.conf.rpc_poll_timeout)
        return message

    def stop(self):
        consumer = self.router_consumer
        LOG.info(_LI("Stop server %(address)s:%(port)s"),
                 {'address': consumer.address, 'port': consumer.port})

    def cleanup(self):
        self.poller.close()
        for consumer in self.consumers:
            consumer.cleanup()

    def listen(self, target):
        self.router_consumer.listen(target)
        self.pull_consumer.listen(target)
        if self.sub_consumer:
            self.sub_consumer.listen(target)

    def listen_notification(self, targets_and_priorities):
        consumer = self.notify_consumer
        for target, priority in targets_and_priorities:
            t = copy.deepcopy(target)
            t.topic = target.topic + '.' + priority
            consumer.listen(t)
