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
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_call_responder
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_fanout_consumer
from oslo_messaging._drivers.zmq_driver import zmq_async

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqServer(base.Listener):

    def __init__(self, conf, matchmaker=None):
        LOG.info("[Server] __init__")
        self.conf = conf
        self.context = zmq.Context()
        self.poller = zmq_async.get_reply_poller()
        self.matchmaker = matchmaker
        self.call_resp = zmq_call_responder.CallResponder(self, conf,
                                                          self.poller,
                                                          self.context)
        self.fanout_resp = zmq_fanout_consumer.FanoutConsumer(self, conf,
                                                              self.poller,
                                                              self.context)

    def poll(self, timeout=None):
        incoming = self.poller.poll(timeout or self.conf.rpc_poll_timeout)
        return incoming[0]

    def stop(self):
        LOG.info("[Server] Stop")

    def cleanup(self):
        self.poller.close()
        self.call_resp.cleanup()
        self.fanout_resp.cleanup()

    def listen(self, target):
        LOG.info("[Server] Listen to Target %s" % target)

        self.matchmaker.register(target=target,
                                 hostname=self.conf.rpc_zmq_host)
        if target.fanout:
            self.fanout_resp.listen(target)
        else:
            self.call_resp.listen(target)
