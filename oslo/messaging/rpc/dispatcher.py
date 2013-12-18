# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
# Copyright 2013 New Dream Network, LLC (DreamHost)
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

__all__ = [
    'NoSuchMethod',
    'RPCDispatcher',
    'RPCDispatcherError',
    'UnsupportedVersion',
]

import six

from oslo.messaging import _utils as utils
from oslo.messaging import localcontext
from oslo.messaging import serializer as msg_serializer
from oslo.messaging import server as msg_server
from oslo.messaging import target


class RPCDispatcherError(msg_server.MessagingServerError):
    "A base class for all RPC dispatcher exceptions."


class NoSuchMethod(RPCDispatcherError, AttributeError):
    "Raised if there is no endpoint which exposes the requested method."

    def __init__(self, method):
        msg = "Endpoint does not support RPC method %s" % method
        super(NoSuchMethod, self).__init__(msg)
        self.method = method


class UnsupportedVersion(RPCDispatcherError):
    "Raised if there is no endpoint which supports the requested version."

    def __init__(self, version):
        msg = "Endpoint does not support RPC version %s" % version
        super(UnsupportedVersion, self).__init__(msg)
        self.version = version


class RPCDispatcher(object):
    """A message dispatcher which understands RPC messages.

    A MessageHandlingServer is constructed by passing a callable dispatcher
    which is invoked with context and message dictionaries each time a message
    is received.

    RPCDispatcher is one such dispatcher which understands the format of RPC
    messages. The dispatcher looks at the namespace, version and method values
    in the message and matches those against a list of available endpoints.

    Endpoints may have a target attribute describing the namespace and version
    of the methods exposed by that object. All public methods on an endpoint
    object are remotely invokable by clients.
    """

    def __init__(self, endpoints, serializer):
        self.endpoints = endpoints
        self.serializer = serializer or msg_serializer.NoOpSerializer()
        self._default_target = target.Target()

    @staticmethod
    def _is_namespace(target, namespace):
        return namespace == target.namespace

    @staticmethod
    def _is_compatible(target, version):
        endpoint_version = target.version or '1.0'
        return utils.version_is_compatible(endpoint_version, version)

    def _dispatch(self, endpoint, method, ctxt, args):
        ctxt = self.serializer.deserialize_context(ctxt)
        new_args = dict()
        for argname, arg in six.iteritems(args):
            new_args[argname] = self.serializer.deserialize_entity(ctxt, arg)
        result = getattr(endpoint, method)(ctxt, **new_args)
        return self.serializer.serialize_entity(ctxt, result)

    def __call__(self, ctxt, message):
        """Dispatch an RPC message to the appropriate endpoint method.

        :param ctxt: the request context
        :type ctxt: dict
        :param message: the message payload
        :type message: dict
        :raises: NoSuchMethod, UnsupportedVersion
        """
        method = message.get('method')
        args = message.get('args', {})
        namespace = message.get('namespace')
        version = message.get('version', '1.0')

        found_compatible = False
        for endpoint in self.endpoints:
            target = getattr(endpoint, 'target', None)
            if not target:
                target = self._default_target

            if not (self._is_namespace(target, namespace) and
                    self._is_compatible(target, version)):
                continue

            if hasattr(endpoint, method):
                localcontext.set_local_context(ctxt)
                try:
                    return self._dispatch(endpoint, method, ctxt, args)
                finally:
                    localcontext.clear_local_context()

            found_compatible = True

        if found_compatible:
            raise NoSuchMethod(method)
        else:
            raise UnsupportedVersion(version)
