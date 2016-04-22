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

from stevedore import driver

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LI

zmq = zmq_async.import_zmq(zmq_concurrency='native')
LOG = logging.getLogger(__name__)


class ZmqProxy(object):
    """Wrapper class for Publishers and Routers proxies.
       The main reason to have a proxy is high complexity of TCP sockets number
       growth with direct connections (when services connect directly to
       each other). The general complexity for ZeroMQ+Openstack deployment
       with direct connections may be square(N) (where N is a number of nodes
       in deployment). With proxy the complexity is reduced to k*N where
       k is a number of services.

       Currently there are 2 types of proxy, they are Publishers and Routers.
       Publisher proxy serves for PUB-SUB pattern implementation where
       Publisher is a server which performs broadcast to subscribers.
       Router is used for direct message types in case of number of TCP socket
       connections is critical for specific deployment. Generally 3 publishers
       is enough for deployment.

       Router is used for direct messages in order to reduce the number of
       allocated TCP sockets in controller. The list of requirements to Router:

       1. There may be any number of routers in the deployment. Routers are
          registered in a name-server and client connects dynamically to all of
          them performing load balancing.
       2. Routers should be transparent for clients and servers. Which means
          it doesn't change the way of messaging between client and the final
          target by hiding the target from a client.
       3. Router may be restarted or get down at any time loosing all messages
          in its queue. Smart retrying (based on acknowledgements from server
          side) and load balancing between other Router instances from the
          client side should handle the situation.
       4. Router takes all the routing information from message envelope and
          doesn't perform Target-resolution in any way.
       5. Routers don't talk to each other and no synchronization is needed.
       6. Load balancing is performed by the client in a round-robin fashion.

       Those requirements should limit the performance impact caused by using
       of proxies making proxies as lightweight as possible.

    """

    def __init__(self, conf, proxy_cls):
        super(ZmqProxy, self).__init__()
        self.conf = conf
        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.conf.rpc_zmq_matchmaker,
        ).driver(self.conf)
        self.context = zmq.Context()
        self.proxy = proxy_cls(conf, self.context, self.matchmaker)

    def run(self):
        self.proxy.run()

    def close(self):
        LOG.info(_LI("Proxy shutting down ..."))
        self.proxy.cleanup()
