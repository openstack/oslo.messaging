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

import os
import threading

from debtcollector import removals
from stevedore import driver

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client import zmq_client
from oslo_messaging._drivers.zmq_driver.server import zmq_server
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_options
from oslo_messaging._i18n import _LE


RPCException = rpc_common.RPCException


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


@removals.removed_class('ZmqDriver', version='Rocky', removal_version='Stein',
                        message='The ZeroMQ driver is no longer supported')
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

        conf = zmq_options.register_opts(conf, url)
        self.conf = conf
        self.allowed_remote_exmods = allowed_remote_exmods

        self.matchmaker = driver.DriverManager(
            'oslo.messaging.zmq.matchmaker',
            self.get_matchmaker_backend(self.conf, url),
        ).driver(self.conf, url=url)

        client_cls = zmq_client.ZmqClientProxy
        if conf.oslo_messaging_zmq.use_pub_sub and not \
                conf.oslo_messaging_zmq.use_router_proxy:
            client_cls = zmq_client.ZmqClientMixDirectPubSub
        elif not conf.oslo_messaging_zmq.use_pub_sub and not \
                conf.oslo_messaging_zmq.use_router_proxy:
            client_cls = zmq_client.ZmqClientDirect

        self.client = LazyDriverItem(
            client_cls, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        self.notifier = LazyDriverItem(
            client_cls, self.conf, self.matchmaker,
            self.allowed_remote_exmods)

        super(ZmqDriver, self).__init__(conf, url, default_exchange,
                                        allowed_remote_exmods)

    @staticmethod
    def get_matchmaker_backend(conf, url):
        zmq_transport, _, matchmaker_backend = url.transport.partition('+')
        assert zmq_transport == 'zmq', "Needs to be zmq for this transport!"
        if not matchmaker_backend:
            return conf.oslo_messaging_zmq.rpc_zmq_matchmaker
        if matchmaker_backend not in zmq_options.MATCHMAKER_BACKENDS:
            raise rpc_common.RPCException(
                _LE("Incorrect matchmaker backend name %(backend_name)s! "
                    "Available names are: %(available_names)s") %
                {"backend_name": matchmaker_backend,
                 "available_names": zmq_options.MATCHMAKER_BACKENDS})
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
