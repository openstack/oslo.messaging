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

import contextlib
import logging
import sys

from oslo_config import cfg

from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver.broker import zmq_broker
from oslo_messaging._executors import impl_pooledexecutor

CONF = cfg.CONF
CONF.register_opts(impl_zmq.zmq_opts)
CONF.register_opts(impl_pooledexecutor._pool_opts)
# TODO(ozamiatin): Move this option assignment to an external config file
# Use efficient zmq poller in real-world deployment
CONF.rpc_zmq_native = True


def main():
    CONF(sys.argv[1:], project='oslo')
    logging.basicConfig(level=logging.DEBUG)

    with contextlib.closing(zmq_broker.ZmqBroker(CONF)) as reactor:
        reactor.start()
        reactor.wait()

if __name__ == "__main__":
    main()
