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
import threading

from oslo_config import cfg
from stevedore import driver

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client import zmq_client
from oslo_messaging._drivers.zmq_driver.server import zmq_server
from oslo_messaging._executors import impl_pooledexecutor  # FIXME(markmc)


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
        default='redis',
        help='MatchMaker driver.',
    ),

    cfg.BoolOpt('rpc_zmq_all_req_rep',
                default=True,
                help='Use REQ/REP pattern for all methods CALL/CAST/FANOUT.'),

    cfg.StrOpt('rpc_zmq_concurrency', default='eventlet',
               help='Type of concurrency used. Either "native" or "eventlet"'),

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


class LazyDriverItem(object):

    def __init__(self, item_cls, *args, **kwargs):
        self._lock = threading.Lock()
        self.item = None
        self.item_class = item_cls
        self.args = args
        self.kwargs = kwargs

    def get(self):
        #  NOTE(ozamiatin): Lazy initialization.
        #  All init stuff moved closer to usage point - lazy init.
        #  Better design approach is to initialize in the driver's
        # __init__, but 'fork' extensively used by services
        #  breaks all things.

        if self.item is not None:
            return self.item

        self._lock.acquire()
        if self.item is None:
            self.item = self.item_class(*self.args, **self.kwargs)
        self._lock.release()
        return self.item

    def cleanup(self):
        if self.item:
            self.item.cleanup()


class ZmqDriver(base.BaseDriver):

    """ZeroMQ Driver implementation.

    Provides implementation of RPC and Notifier APIs by means
    of ZeroMQ library.

    See :doc:`zmq_driver` for details.
    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        """Construct ZeroMQ driver.

        Intialize driver options.

        Construct matchmaker - pluggable interface to targets management
        Name Service

        Construct client and server controllers

        :param conf: oslo messaging configuration object
        :type conf: oslo_config.CONF
        :param url: transport URL
        :type url: TransportUrl
        :param default_exchange: Not used in zmq implementation
        :type default_exchange: None
        :param allowed_remote_exmods: remote exception passing options
        :type allowed_remote_exmods: list
        """
        conf.register_opts(zmq_opts)
        conf.register_opts(impl_pooledexecutor._pool_opts)
        conf.register_opts(base.base_opts)
        self.conf = conf
        self.allowed_remote_exmods = allowed_remote_exmods

        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.conf.rpc_zmq_matchmaker,
        ).driver(self.conf)

        self.server = LazyDriverItem(
            zmq_server.ZmqServer, self, self.conf, self.matchmaker)

        self.notify_server = LazyDriverItem(
            zmq_server.ZmqServer, self, self.conf, self.matchmaker)

        self.client = LazyDriverItem(
            zmq_client.ZmqClient, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        self.notifier = LazyDriverItem(
            zmq_client.ZmqClient, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        super(ZmqDriver, self).__init__(conf, url, default_exchange,
                                        allowed_remote_exmods)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             retry=None):
        """Send RPC message to server

        :param target: Message destination target
        :type target: oslo_messaging.Target
        :param ctxt: Message context
        :type ctxt: dict
        :param message: Message payload to pass
        :type message: dict
        :param wait_for_reply: Waiting for reply flag
        :type wait_for_reply: bool
        :param timeout: Reply waiting timeout in seconds
        :type timeout: int
        :param retry: an optional default connection retries configuration
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        client = self.client.get()
        timeout = timeout or self.conf.rpc_response_timeout
        if wait_for_reply:
            return client.send_call(target, ctxt, message, timeout, retry)
        elif target.fanout:
            client.send_fanout(target, ctxt, message, timeout, retry)
        else:
            client.send_cast(target, ctxt, message, timeout, retry)

    def send_notification(self, target, ctxt, message, version, retry=None):
        """Send notification to server

        :param target: Message destination target
        :type target: oslo_messaging.Target
        :param ctxt: Message context
        :type ctxt: dict
        :param message: Message payload to pass
        :type message: dict
        :param version: Messaging API version
        :type version: str
        :param retry: an optional default connection retries configuration
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        client = self.notifier.get()
        if target.fanout:
            client.send_notify_fanout(target, ctxt, message, version, retry)
        else:
            client.send_notify(target, ctxt, message, version, retry)

    def listen(self, target):
        """Listen to a specified target on a server side

        :param target: Message destination target
        :type target: oslo_messaging.Target
        """
        server = self.server.get()
        server.listen(target)
        return server

    def listen_for_notifications(self, targets_and_priorities, pool):
        """Listen to a specified list of targets on a server side

        :param targets_and_priorities: List of pairs (target, priority)
        :type targets_and_priorities: list
        :param pool: Not used for zmq implementation
        :type pool: object
        """
        server = self.notify_server.get()
        server.listen_notification(targets_and_priorities)
        return server

    def cleanup(self):
        """Cleanup all driver's connections finally
        """
        self.client.cleanup()
        self.server.cleanup()
        self.notify_server.cleanup()
        self.notifier.cleanup()
