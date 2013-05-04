
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
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

import abc

import eventlet
import greenlet

from openstack.common.gettextutils import _
from openstack.common import log as logging
from openstack.common.messaging import _utils as utils

_LOG = logging.getLogger(__name__)


class RPCServerError(Exception):
    pass


class NoSuchMethodError(RPCServerError, AttributeError):

    def __init__(self, method):
        self.method = method

    def __str__(self):
        return _("Endpoint does not support RPC method %s") % self.method


class UnsupportedVersion(RPCServerError):

    def __init__(self, version):
        self.version = version

    def __str__(self):
        return _("Endpoint does not support RPC version %s") % self.version


class RPCExecutor(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, conf, listener, callback):
        self.conf = conf
        self.listener = listener
        self.callback = callback

    def _process_one_message(self):
        (target, message) = self.listener.poll()
        try:
            self.callback(target, message)
        except Exception:
            _LOG.exception(_("Failed to process message... skipping it."))
        finally:
            self.listener.done(message)

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def wait(self):
        pass


class BlockingRPCExecutor(RPCExecutor):

    def __init__(self, conf, listener, callback):
        super(BlockingRPCExecutor, self).__init__(conf, listener, callback)
        self._running = False

    def start(self):
        self._running = True
        while self._running:
            self._process_one_message()

    def stop(self):
        self._running = False

    def wait(self):
        pass


class EventletRPCExecutor(RPCExecutor):

    def __init__(self, conf, listener, callback):
        super(EventletRPCExecutor, self).__init__(conf, listener, callback)
        self._thread = None

    def start(self):
        if self._thread is not None:
            return

        def _executor_thread():
            try:
                while True:
            except greenlet.GreenletExit:
                return

        self._thread = eventlet.spawn(_executor_thread)

    def stop(self):
        if self._thread is None:
            return
        self._thread.kill()

    def wait(self):
        if self._thread is None:
            return
        try:
            self._thread.wait()
        except greenlet.GreenletExit:
            pass
        self._thread = None


class RPCServer(object):

    def __init__(self, transport, target, endpoints, executor_cls):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.endpoints = endpoints

        self._executor_cls = executor_cls
        self._executor = None

        super(RPCServer, self).__init__()

    @staticmethod
    def _is_compatible(endpoint, version):
        endpoint_version = endpoint.target.version or '1.0'
        return utils.version_is_compatible(endpoint_version, version)

    def _dispatch(self, target, message):
        method = message.get('method')
        args = message.get('args', {})

        version = target.version or '1.0'

        found_compatible = False
        for endpoint in self.endpoints:
            if target.namespace != endpoint.target.namespace:
                continue

            is_compatible = self._is_compatible(endpoint, version)

            if is_compatible and hasattr(endpoint, method):
                return getattr(endpoint, method)(**kwargs)

            found_compatible = found_compatible or is_compatible

        if found_compatible:
            raise NoSuchMethodError(method)
        else:
            raise UnsupportedVersion(version)

    def start():
        if self._executor is not None:
            return
        listener = self.transport.listen(self.target)
        self._executor = self._executor_cls(self.conf, listener,
                                            self._dispatch)
        self._executor.start()

    def stop(self):
        if self._executor is not None:
            self._executor.stop()

    def wait(self):
        if self._executor is not None:
            self._executor.wait()
        self._executor = None


def get_blocking_server(transport, target, endpoints):
    return RPCServer(transport, target, endpoints, BlockingRPCExecutor)


def get_eventlet_server(transport, target, endpoints):
    return RPCServer(transport, target, endpoints, EventletRPCExecutor)
