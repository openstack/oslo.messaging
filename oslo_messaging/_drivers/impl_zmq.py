#    Copyright 2011 Cloudscaling Group, Inc
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
import pprint
import socket

from oslo_config import cfg
from stevedore import driver

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.rpc.client import zmq_client
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_server
from oslo_messaging._executors import base as executor_base


pformat = pprint.pformat
LOG = logging.getLogger(__name__)
RPCException = rpc_common.RPCException

zmq_opts = [
    cfg.StrOpt('rpc_zmq_bind_address', default='*',
               help='ZeroMQ bind address. Should be a wildcard (*), '
                    'an ethernet interface, or IP. '
                    'The "host" option should point or resolve to this '
                    'address.'),

    # The module.Class to use for matchmaking.
    cfg.StrOpt(
        'rpc_zmq_matchmaker',
        default='dummy',
        help='MatchMaker driver.',
    ),

    cfg.BoolOpt('rpc_zmq_all_req_rep',
                default=True,
                help='Use REQ/REP pattern for all methods CALL/CAST/FANOUT.'),

    cfg.BoolOpt('rpc_zmq_native',
                default=False,
                help='Switches ZeroMQ eventlet/threading way of usage.'
                     'Affects pollers, executors etc.'),

    # The following port is unassigned by IANA as of 2012-05-21
    cfg.IntOpt('rpc_zmq_port', default=9501,
               help='ZeroMQ receiver listening port.'),

    cfg.IntOpt('rpc_zmq_contexts', default=1,
               help='Number of ZeroMQ contexts, defaults to 1.'),

    cfg.IntOpt('rpc_zmq_topic_backlog',
               help='Maximum number of ingress messages to locally buffer '
                    'per topic. Default is unlimited.'),

    cfg.StrOpt('rpc_zmq_ipc_dir', default='/var/run/openstack',
               help='Directory for holding IPC sockets.'),

    cfg.StrOpt('rpc_zmq_host', default=socket.gethostname(),
               sample_default='localhost',
               help='Name of this node. Must be a valid hostname, FQDN, or '
                    'IP address. Must match "host" option, if running Nova.'),

    cfg.IntOpt('rpc_cast_timeout',
               default=30,
               help='Seconds to wait before a cast expires (TTL). '
                    'Only supported by impl_zmq.'),

    cfg.IntOpt('rpc_poll_timeout',
               default=1,
               help='The default number of seconds that poll should wait. '
                    'Poll raises timeout exception when timeout expired.'),
]


class ZmqDriver(base.BaseDriver):
    """ZeroMQ Driver

    See :doc:`zmq_driver` for details.

    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        conf.register_opts(zmq_opts)
        conf.register_opts(executor_base._pool_opts)
        self.conf = conf

        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.conf.rpc_zmq_matchmaker,
        ).driver(self.conf)

        self.server = zmq_server.ZmqServer(self.conf, self.matchmaker)
        self.client = zmq_client.ZmqClient(self.conf, self.matchmaker,
                                           allowed_remote_exmods)
        super(ZmqDriver, self).__init__(conf, url, default_exchange,
                                        allowed_remote_exmods)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        if wait_for_reply:
            return self.client.call(target, ctxt, message, timeout, retry)
        else:
            self.client.cast(target, ctxt, message, timeout, retry)
        return None

    def send_notification(self, target, ctxt, message, version, retry=None):
        return None

    def listen(self, target):
        self.server.listen(target)
        return self.server

    def listen_for_notifications(self, targets_and_priorities, pool):
        return None

    def cleanup(self):
        self.client.cleanup()
        self.server.cleanup()
