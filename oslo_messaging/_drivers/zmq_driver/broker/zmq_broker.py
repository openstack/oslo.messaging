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
import os

from oslo_utils import excutils
from stevedore import driver

from oslo_messaging._drivers.zmq_driver.broker import zmq_queue_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE, _LI

zmq = zmq_async.import_zmq(zmq_concurrency='native')
LOG = logging.getLogger(__name__)


class ZmqBroker(object):
    """Local messaging IPC broker (nodes are still peers).
       The main purpose is to have native zeromq application.
       Benefits of such approach are following:

        1. No risk to block the main thread of the process by unpatched
           native parts of the libzmq (c-library is completely monkey-patch
           unfriendly)
        2. Making use of standard zmq approaches as async pollers,
           devices, queues etc.
        3. Possibility to implement queue persistence not touching existing
           clients (staying in a separate process).
    """

    def __init__(self, conf):
        super(ZmqBroker, self).__init__()
        self.conf = conf
        self._create_ipc_dirs()
        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.conf.rpc_zmq_matchmaker,
        ).driver(self.conf)

        self.context = zmq.Context()
        self.proxies = [zmq_queue_proxy.UniversalQueueProxy(
            conf, self.context, self.matchmaker)
        ]

    def _create_ipc_dirs(self):
        ipc_dir = self.conf.rpc_zmq_ipc_dir
        try:
            os.makedirs("%s/fanout" % ipc_dir)
        except os.error:
            if not os.path.isdir(ipc_dir):
                with excutils.save_and_reraise_exception():
                    LOG.error(_LE("Required IPC directory does not exist at"
                                  " %s"), ipc_dir)

    def start(self):
        for proxy in self.proxies:
            proxy.start()

    def wait(self):
        for proxy in self.proxies:
            proxy.wait()

    def close(self):
        LOG.info(_LI("Broker shutting down ..."))
        for proxy in self.proxies:
            proxy.stop()
