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

import copy
import logging

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_dealer_consumer
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_router_consumer
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_sub_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqServer(base.PollStyleListener):

    def __init__(self, driver, conf, matchmaker, target, poller=None):
        super(ZmqServer, self).__init__()
        self.driver = driver
        self.conf = conf
        self.matchmaker = matchmaker
        self.target = target
        self.poller = poller or zmq_async.get_poller()

        LOG.info(_LI('[%(host)s] Run server %(target)s'),
                 {'host': self.conf.oslo_messaging_zmq.rpc_zmq_host,
                  'target': self.target})

        if conf.oslo_messaging_zmq.use_router_proxy:
            self.router_consumer = None
            dealer_consumer_cls = \
                zmq_dealer_consumer.DealerConsumerWithAcks \
                if conf.oslo_messaging_zmq.rpc_use_acks else \
                zmq_dealer_consumer.DealerConsumer
            self.dealer_consumer = dealer_consumer_cls(conf, self.poller, self)
        else:
            self.router_consumer = \
                zmq_router_consumer.RouterConsumer(conf, self.poller, self)
            self.dealer_consumer = None

        self.sub_consumer = \
            zmq_sub_consumer.SubConsumer(conf, self.poller, self) \
            if conf.oslo_messaging_zmq.use_pub_sub else None

        self.consumers = []
        if self.router_consumer is not None:
            self.consumers.append(self.router_consumer)
        if self.dealer_consumer is not None:
            self.consumers.append(self.dealer_consumer)
        if self.sub_consumer is not None:
            self.consumers.append(self.sub_consumer)

    @base.batch_poll_helper
    def poll(self, timeout=None):
        message, socket = self.poller.poll(
            timeout or self.conf.oslo_messaging_zmq.rpc_poll_timeout)
        return message

    def stop(self):
        self.poller.close()
        for consumer in self.consumers:
            consumer.stop()

        LOG.info(_LI('[%(host)s] Stop server %(target)s'),
                 {'host': self.conf.oslo_messaging_zmq.rpc_zmq_host,
                  'target': self.target})

    def cleanup(self):
        self.poller.close()
        for consumer in self.consumers:
            consumer.cleanup()

        LOG.info(_LI('[%(host)s] Destroy server %(target)s'),
                 {'host': self.conf.oslo_messaging_zmq.rpc_zmq_host,
                  'target': self.target})


class ZmqNotificationServer(base.PollStyleListener):

    def __init__(self, driver, conf, matchmaker, targets_and_priorities):
        super(ZmqNotificationServer, self).__init__()
        self.driver = driver
        self.conf = conf
        self.matchmaker = matchmaker
        self.servers = []
        self.poller = zmq_async.get_poller()
        self._listen(targets_and_priorities)

    def _listen(self, targets_and_priorities):
        for target, priority in targets_and_priorities:
            t = copy.deepcopy(target)
            t.topic = target.topic + '.' + priority
            self.servers.append(ZmqServer(
                self.driver, self.conf, self.matchmaker, t, self.poller))

    @base.batch_poll_helper
    def poll(self, timeout=None):
        message, socket = self.poller.poll(
            timeout or self.conf.oslo_messaging_zmq.rpc_poll_timeout)
        return message

    def stop(self):
        for server in self.servers:
            server.stop()

    def cleanup(self):
        for server in self.servers:
            server.cleanup()
