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
import os
import socket
import threading

from oslo_config import cfg
from stevedore import driver

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client import zmq_client
from oslo_messaging._drivers.zmq_driver.server import zmq_server
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE
from oslo_messaging import server


RPCException = rpc_common.RPCException
_MATCHMAKER_BACKENDS = ('redis', 'dummy')
_MATCHMAKER_DEFAULT = 'redis'
_CONCURRENCY_CHOICES = ('eventlet', 'native')
_CONCURRENCY_DEFAULT = 'eventlet'
LOG = logging.getLogger(__name__)


zmq_opts = [
    cfg.StrOpt('rpc_zmq_bind_address', default='*',
               help='ZeroMQ bind address. Should be a wildcard (*), '
                    'an ethernet interface, or IP. '
                    'The "host" option should point or resolve to this '
                    'address.'),

    cfg.StrOpt('rpc_zmq_matchmaker', default=_MATCHMAKER_DEFAULT,
               choices=_MATCHMAKER_BACKENDS,
               help='MatchMaker driver.'),

    cfg.StrOpt('rpc_zmq_concurrency', default=_CONCURRENCY_DEFAULT,
               choices=_CONCURRENCY_CHOICES,
               help='Type of concurrency used. Either "native" or "eventlet"'),

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

    cfg.IntOpt('rpc_cast_timeout', default=-1,
               help='Seconds to wait before a cast expires (TTL). '
                    'The default value of -1 specifies an infinite linger '
                    'period. The value of 0 specifies no linger period. '
                    'Pending messages shall be discarded immediately '
                    'when the socket is closed. Only supported by impl_zmq.'),

    cfg.IntOpt('rpc_poll_timeout', default=1,
               help='The default number of seconds that poll should wait. '
                    'Poll raises timeout exception when timeout expired.'),

    cfg.IntOpt('zmq_target_expire', default=120,
               help='Expiration timeout in seconds of a name service record '
                    'about existing target ( < 0 means no timeout).'),

    cfg.BoolOpt('use_pub_sub', default=True,
                help='Use PUB/SUB pattern for fanout methods. '
                     'PUB/SUB always uses proxy.'),

    cfg.BoolOpt('use_router_proxy', default=True,
                help='Use ROUTER remote proxy for direct methods.'),

    cfg.PortOpt('rpc_zmq_min_port',
                default=49153,
                help='Minimal port number for random ports range.'),

    cfg.IntOpt('rpc_zmq_max_port',
               min=1,
               max=65536,
               default=65536,
               help='Maximal port number for random ports range.'),

    cfg.IntOpt('rpc_zmq_bind_port_retries',
               default=100,
               help='Number of retries to find free port number before '
                    'fail with ZMQBindError.')
]


class LazyDriverItem(object):

    def __init__(self, item_cls, *args, **kwargs):
        self._lock = threading.Lock()
        self.item = None
        self.item_class = item_cls
        self.args = args
        self.kwargs = kwargs
        self.process_id = os.getpid()

    def get(self):
        #  NOTE(ozamiatin): Lazy initialization.
        #  All init stuff moved closer to usage point - lazy init.
        #  Better design approach is to initialize in the driver's
        # __init__, but 'fork' extensively used by services
        #  breaks all things.

        if self.item is not None and os.getpid() == self.process_id:
            return self.item

        with self._lock:
            if self.item is None or os.getpid() != self.process_id:
                self.process_id = os.getpid()
                self.item = self.item_class(*self.args, **self.kwargs)
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

        Initialize driver options.

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
        zmq = zmq_async.import_zmq()
        if zmq is None:
            raise ImportError(_LE("ZeroMQ is not available!"))

        conf.register_opts(zmq_opts)
        conf.register_opts(server._pool_opts)
        conf.register_opts(base.base_opts)
        self.conf = conf
        self.allowed_remote_exmods = allowed_remote_exmods

        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.get_matchmaker_backend(url),
        ).driver(self.conf, url=url)

        self.client = LazyDriverItem(
            zmq_client.ZmqClient, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        self.notifier = LazyDriverItem(
            zmq_client.ZmqClient, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        super(ZmqDriver, self).__init__(conf, url, default_exchange,
                                        allowed_remote_exmods)

    def get_matchmaker_backend(self, url):
        zmq_transport, p, matchmaker_backend = url.transport.partition('+')
        assert zmq_transport == 'zmq', "Needs to be zmq for this transport!"
        if not matchmaker_backend:
            return self.conf.rpc_zmq_matchmaker
        elif matchmaker_backend not in _MATCHMAKER_BACKENDS:
            raise rpc_common.RPCException(
                _LE("Incorrect matchmaker backend name %(backend_name)s!"
                    "Available names are: %(available_names)s") %
                {"backend_name": matchmaker_backend,
                 "available_names": _MATCHMAKER_BACKENDS})
        return matchmaker_backend

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
        if wait_for_reply:
            return client.send_call(target, ctxt, message, timeout, retry)
        elif target.fanout:
            client.send_fanout(target, ctxt, message, retry)
        else:
            client.send_cast(target, ctxt, message, retry)

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
        client.send_notify(target, ctxt, message, version, retry)

    def listen(self, target, batch_size, batch_timeout):
        """Listen to a specified target on a server side

        :param target: Message destination target
        :type target: oslo_messaging.Target
        """
        listener = zmq_server.ZmqServer(self, self.conf, self.matchmaker,
                                        target)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Listen to a specified list of targets on a server side

        :param targets_and_priorities: List of pairs (target, priority)
        :type targets_and_priorities: list
        :param pool: Not used for zmq implementation
        :type pool: object
        """
        listener = zmq_server.ZmqNotificationServer(
            self, self.conf, self.matchmaker, targets_and_priorities)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def cleanup(self):
        """Cleanup all driver's connections finally
        """
        self.client.cleanup()
        self.notifier.cleanup()
