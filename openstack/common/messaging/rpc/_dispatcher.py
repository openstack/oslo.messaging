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

from openstack.common.gettextutils import _
from openstack.common import log as logging
from openstack.common.messaging import serializer as msg_serializer
from openstack.common.messaging import _server as server
from openstack.common.messaging import target
from openstack.common.messaging import _utils as utils

_LOG = logging.getLogger(__name__)


class RPCDispatcherError(server.MessagingServerError):
    pass


class NoSuchMethodError(RPCDispatcherError, AttributeError):

    def __init__(self, method):
        self.method = method

    def __str__(self):
        return _("Endpoint does not support RPC method %s") % self.method


class UnsupportedVersion(RPCDispatcherError):

    def __init__(self, version):
        self.version = version

    def __str__(self):
        return _("Endpoint does not support RPC version %s") % self.version


class RPCDispatcher(object):
    "Pass messages to the API objects for processing."

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
        new_args = dict()
        for argname, arg in args.iteritems():
            new_args[argname] = self.serializer.deserialize_entity(ctxt, arg)
        result = getattr(endpoint, method)(ctxt, **new_args)
        return self.serializer.serialize_entity(ctxt, result)

    def __call__(self, ctxt, message):
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
                return self._dispatch(endpoint, method, ctxt, args)

            found_compatible = True

        if found_compatible:
            raise NoSuchMethodError(method)
        else:
            raise UnsupportedVersion(version)
