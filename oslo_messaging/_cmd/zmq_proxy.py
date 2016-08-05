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

from oslo_messaging._drivers.zmq_driver.proxy import zmq_proxy
from oslo_messaging._drivers.zmq_driver.proxy import zmq_queue_proxy
from oslo_messaging._drivers.zmq_driver import zmq_options

CONF = cfg.CONF

zmq_options.register_opts(CONF)

opt_group = cfg.OptGroup(name='zmq_proxy_opts',
                         title='ZeroMQ proxy options')
CONF.register_opts(zmq_proxy.zmq_proxy_opts, group=opt_group)


USAGE = """ Usage: ./zmq-proxy.py [-h] [] ...

Usage example:
 python oslo_messaging/_cmd/zmq-proxy.py"""


def main():
    parser = argparse.ArgumentParser(
        description='ZeroMQ proxy service',
        usage=USAGE
    )

    parser.add_argument('--config-file', dest='config_file', type=str,
                        help='Path to configuration file')

    parser.add_argument('--host', dest='host', type=str,
                        help='Host FQDN for current proxy')
    parser.add_argument('--frontend-port', dest='frontend_port', type=int,
                        help='Front-end ROUTER port number')
    parser.add_argument('--backend-port', dest='backend_port', type=int,
                        help='Back-end ROUTER port number')
    parser.add_argument('--publisher-port', dest='publisher_port', type=int,
                        help='Back-end PUBLISHER port number')

    parser.add_argument('-d', '--debug', dest='debug', type=bool,
                        default=False,
                        help="Turn on DEBUG logging level instead of INFO")

    args = parser.parse_args()

    if args.config_file:
        cfg.CONF(["--config-file", args.config_file])

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(name)s '
                               '%(levelname)-8s %(message)s')

    if args.host:
        CONF.zmq_proxy_opts.host = args.host
    if args.frontend_port:
        CONF.set_override('frontend_port', args.frontend_port,
                          group='zmq_proxy_opts')
    if args.backend_port:
        CONF.set_override('backend_port', args.backend_port,
                          group='zmq_proxy_opts')
    if args.publisher_port:
        CONF.set_override('publisher_port', args.publisher_port,
                          group='zmq_proxy_opts')

    reactor = zmq_proxy.ZmqProxy(CONF, zmq_queue_proxy.UniversalQueueProxy)

    try:
        while True:
            reactor.run()
    except (KeyboardInterrupt, SystemExit):
        reactor.close()

if __name__ == "__main__":
    main()
