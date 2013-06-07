
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

import inspect

from oslo.config import cfg

from openstack.common.gettextutils import _
from openstack.common import local
from openstack.common import log as logging
from openstack.common.messaging import serializer as msg_serializer
from openstack.common.messaging import target
from openstack.common.messaging import _utils as utils

_client_opts = [
    cfg.IntOpt('rpc_response_timeout',
               default=60,
               help='Seconds to wait for a response from a call'),
]

_LOG = logging.getLogger(__name__)


class RpcVersionCapError(Exception):

    def __init__(self, version, version_cap):
        self.version = version
        self.version_cap = version_cap

    def __str__(self):
        return (_("Specified RPC version cap, %(version_cap)s, is too low. "
                  "Needs to be higher than %(version)s.") %
                dict(version=self.version, version_cap=self.version_cap))


class _CallContext(object):

    def __init__(self, transport, target, serializer,
                 timeout=None, check_for_lock=None, version_cap=None):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.serializer = serializer
        self.timeout = timeout
        self.check_for_lock = check_for_lock
        self.version_cap = version_cap

        super(_CallContext, self).__init__()

    def _make_message(self, ctxt, method, args):
        msg = dict(method=method)

        msg['args'] = dict()
        for argname, arg in args.iteritems():
            msg['args'][argname] = self.serializer.serialize_entity(ctxt, arg)

        if self.target.namespace is not None:
            msg['namespace'] = self.target.namespace
        if self.target.version is not None:
            msg['version'] = self.target.version

        return msg

    def cast(self, ctxt, method, **kwargs):
        msg = self._make_message(ctxt, method, kwargs)
        self.transport._send(target, ctxt, msg)

    def _check_for_lock(self):
        if not self.conf.debug:
            return None

        if (hasattr(local.strong_store, 'locks_held') and
            local.strong_store.locks_held):
            stack = ' :: '.join([frame[3] for frame in inspect.stack()])
            _LOG.warn(_('An RPC is being made while holding a lock. The locks '
                        'currently held are %(locks)s. This is probably a bug. '
                        'Please report it. Include the following: [%(stack)s].'),
                      {'locks': local.strong_store.locks_held, 'stack': stack})

    def _check_version_cap(self, version):
        if not utils.version_is_compatible(self.version_cap, v):
            raise RpcVersionCapError(version=version,
                                     version_cap=self.version_cap)

    def call(self, ctxt, method, **kwargs):
        msg = self._make_message(ctxt, method, kwargs)

        timeout = self.timeout
        if self.timeout is None:
            timeout = self.conf.rpc_response_timeout

        if self.check_for_lock:
            self._check_for_lock()
        if self.version_cap:
            self._check_version_cap(msg.get('version'))

        result = self.transport._send(self.target, ctxt, msg,
                                      wait_for_reply=True, timeout=timeout)
        return self.serializer.deserialize_entity(ctxt, result)


class RPCClient(object):

    def __init__(self, transport, target,
                 timeout=None, check_for_lock=None,
                 version_cap=None, serializer=None):
        self.conf = transport.conf
        self.conf.register_opts(_client_opts)

        self.transport = transport
        self.target = target
        self.timeout = timeout
        self.check_for_lock = check_for_lock
        self.version_cap = version_cap
        self.serializer = serializer or msg_serializer.NoOpSerializer()

        super(RPCClient, self).__init__()

    def prepare(self, exchange=None, topic=None, namespace=None,
                version=None, server=None, fanout=None,
                timeout=None, check_for_lock=None, version_cap=None):
        target = self.target(exchange=exchange,
                             topic=topic,
                             namespace=namespace,
                             version=version,
                             server=server,
                             fanout=fanout)

        if timeout is None:
            timeout = self.timeout
        if check_for_lock is None:
            check_for_lock = self.check_for_lock
        if version_cap is None:
            version_cap = self.version_cap

        return _CallContext(self.transport, target,
                            self.serializer,
                            timeout, check_for_lock,
                            version_cap)

    def cast(self, ctxt, method, **kwargs):
        self.prepare().cast(ctxt, method, **kwargs)

    def call(self, ctxt, method, **kwargs):
        return self.prepare().call(ctxt, method, **kwargs)
