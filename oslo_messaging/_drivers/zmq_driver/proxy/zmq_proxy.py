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

import argparse
import logging
import socket

from oslo_config import cfg
from stevedore import driver

from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver.proxy.central import zmq_central_proxy
from oslo_messaging._drivers.zmq_driver.proxy.local import zmq_local_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LI
from oslo_messaging import transport

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


USAGE = """ Usage: ./zmq-proxy.py [-h] [] ...

Usage example:
 python oslo_messaging/_cmd/zmq-proxy.py"""


zmq_proxy_opts = [
    cfg.StrOpt('host', default=socket.gethostname(),
               help='Hostname (FQDN) of current proxy'
                    ' an ethernet interface, or IP address.'),

    cfg.IntOpt('frontend_port', default=0,
               help='Front-end ROUTER port number. Zero means random.'),

    cfg.IntOpt('backend_port', default=0,
               help='Back-end ROUTER port number. Zero means random.'),

    cfg.IntOpt('publisher_port', default=0,
               help='Publisher port number. Zero means random.'),

    cfg.BoolOpt('local_publisher', default=False,
                help='Specify publisher/subscriber local proxy.'),

    cfg.BoolOpt('ack_pub_sub', default=False,
                help='Use acknowledgements for notifying senders about '
                     'receiving their fanout messages. '
                     'The option is ignored if PUB/SUB is disabled.'),

    cfg.StrOpt('url', default='zmq://127.0.0.1:6379/',
               help='ZMQ-driver transport URL with additional configurations')
]


def parse_command_line_args(conf):
    parser = argparse.ArgumentParser(
        description='ZeroMQ proxy service',
        usage=USAGE
    )

    parser.add_argument('-c', '--config-file', dest='config_file', type=str,
                        help='Path to configuration file')
    parser.add_argument('-l', '--log-file', dest='log_file', type=str,
                        help='Path to log file')

    parser.add_argument('-H', '--host', dest='host', type=str,
                        help='Host FQDN for current proxy')
    parser.add_argument('-f', '--frontend-port', dest='frontend_port',
                        type=int,
                        help='Front-end ROUTER port number')
    parser.add_argument('-b', '--backend-port', dest='backend_port', type=int,
                        help='Back-end ROUTER port number')
    parser.add_argument('-p', '--publisher-port', dest='publisher_port',
                        type=int,
                        help='Back-end PUBLISHER port number')
    parser.add_argument('-lp', '--local-publisher', dest='local_publisher',
                        action='store_true',
                        help='Specify publisher/subscriber local proxy.')
    parser.add_argument('-a', '--ack-pub-sub', dest='ack_pub_sub',
                        action='store_true',
                        help='Acknowledge PUB/SUB messages')
    parser.add_argument('-u', '--url', dest='url', type=str,
                        help='Transport URL with configurations')

    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Turn on DEBUG logging level instead of INFO')

    args = parser.parse_args()

    if args.config_file:
        conf(['--config-file', args.config_file])

    log_kwargs = {'level': logging.DEBUG if args.debug else logging.INFO,
                  'format': '%(asctime)s %(name)s %(levelname)-8s %(message)s'}
    if args.log_file:
        log_kwargs.update({'filename': args.log_file})
    logging.basicConfig(**log_kwargs)

    if args.host:
        conf.set_override('host', args.host, group='zmq_proxy_opts')
    if args.frontend_port:
        conf.set_override('frontend_port', args.frontend_port,
                          group='zmq_proxy_opts')
    if args.backend_port:
        conf.set_override('backend_port', args.backend_port,
                          group='zmq_proxy_opts')
    if args.publisher_port:
        conf.set_override('publisher_port', args.publisher_port,
                          group='zmq_proxy_opts')
    if args.local_publisher:
        conf.set_override('local_publisher', args.local_publisher,
                          group='zmq_proxy_opts')
    if args.ack_pub_sub:
        conf.set_override('ack_pub_sub', args.ack_pub_sub,
                          group='zmq_proxy_opts')
    if args.url:
        conf.set_override('url', args.url, group='zmq_proxy_opts')


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
       3. Router may be restarted or shut down at any time losing all messages
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

    def __init__(self, conf):
        super(ZmqProxy, self).__init__()
        self.conf = conf
        url = transport.TransportURL.parse(
            self.conf, url=self.conf.zmq_proxy_opts.url
        )
        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            impl_zmq.ZmqDriver.get_matchmaker_backend(self.conf, url)
        ).driver(self.conf, url=url)
        self.context = zmq.Context()
        self.proxy = self._choose_proxy_implementation()

    def _choose_proxy_implementation(self):
        if self.conf.zmq_proxy_opts.local_publisher:
            return zmq_local_proxy.LocalPublisherProxy(self.conf, self.context,
                                                       self.matchmaker)
        elif self.conf.zmq_proxy_opts.frontend_port != 0 and \
                self.conf.zmq_proxy_opts.backend_port == 0:
            return zmq_central_proxy.SingleRouterProxy(self.conf, self.context,
                                                       self.matchmaker)
        else:
            return zmq_central_proxy.DoubleRouterProxy(self.conf, self.context,
                                                       self.matchmaker)

    def run(self):
        self.proxy.run()

    def close(self):
        LOG.info(_LI("Proxy shutting down ..."))
        self.proxy.cleanup()
