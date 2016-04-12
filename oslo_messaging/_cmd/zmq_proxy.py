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

import argparse
import logging

from oslo_config import cfg

from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver.broker import zmq_proxy
from oslo_messaging._drivers.zmq_driver.broker import zmq_queue_proxy
from oslo_messaging import server

CONF = cfg.CONF
CONF.register_opts(impl_zmq.zmq_opts)
CONF.register_opts(server._pool_opts)
CONF.rpc_zmq_native = True


USAGE = """ Usage: ./zmq-proxy.py --type {PUBLISHER,ROUTER} [-h] [] ...

Usage example:
 python oslo_messaging/_cmd/zmq-proxy.py\
 --type PUBLISHER"""


PUBLISHER = 'PUBLISHER'
ROUTER = 'ROUTER'
PROXY_TYPES = (PUBLISHER, ROUTER)


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        description='ZeroMQ proxy service',
        usage=USAGE
    )

    parser.add_argument('--type', dest='proxy_type', type=str,
                        default=PUBLISHER,
                        help='Proxy type PUBLISHER or ROUTER')
    parser.add_argument('--config-file', dest='config_file', type=str,
                        help='Path to configuration file')
    args = parser.parse_args()

    if args.config_file:
        cfg.CONF(["--config-file", args.config_file])

    if args.proxy_type not in PROXY_TYPES:
        raise Exception("Bad proxy type %s, should be one of %s" %
                        (args.proxy_type, PROXY_TYPES))

    reactor = zmq_proxy.ZmqProxy(CONF, zmq_queue_proxy.PublisherProxy) \
        if args.proxy_type == PUBLISHER \
        else zmq_proxy.ZmqProxy(CONF, zmq_queue_proxy.RouterProxy)

    try:
        while True:
            reactor.run()
    except KeyboardInterrupt:
        reactor.close()

if __name__ == "__main__":
    main()
