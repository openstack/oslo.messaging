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

from oslo_messaging._drivers.zmq_driver.broker.zmq_call_proxy import CallProxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE, _LI


LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqBroker(object):
    """Local messaging IPC broker (nodes are still peers).

        The main purpose is to have one TCP connection
        (one TCP port assigned for ZMQ messaging) per node.
        There could be a number of services running on a node.
        Without such broker a number of opened TCP ports used for
        messaging become unpredictable for the engine.

        All messages are coming to TCP ROUTER socket and then
        distributed between their targets by topic via IPC.
    """

    def __init__(self, conf):
        super(ZmqBroker, self).__init__()
        self.conf = conf
        self.context = zmq.Context()
        self.proxies = [CallProxy(conf, self.context)]
        self._create_ipc_dirs()

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
