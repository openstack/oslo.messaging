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
import sys
import time

from oslo_config import cfg

from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver.broker import zmq_broker
from oslo_messaging import server

CONF = cfg.CONF
CONF.register_opts(impl_zmq.zmq_opts)
CONF.register_opts(server._pool_opts)
CONF.rpc_zmq_native = True


def main():
    CONF(sys.argv[1:], project='oslo')
    logging.basicConfig(level=logging.DEBUG)

    reactor = zmq_broker.ZmqBroker(CONF)
    reactor.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
