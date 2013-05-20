
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
from openstack.common.messaging import target

_client_opts = [
    cfg.IntOpt('rpc_response_timeout',
               default=60,
               help='Seconds to wait for a response from a call'),
]

_LOG = logging.getLogger(__name__)


class _CallContext(object):

    def __init__(self, transport, target, timeout=None, check_for_lock=None):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.timeout = timeout
        self.check_for_lock = check_for_lock

        super(_CallContext, self).__init__()

    def _make_message(self, method, args):
        msg = dict(method=method, args=args)
        if self.target.namespace is not None:
            msg['namespace'] = self.target.namespace
        if self.target.version is not None:
            msg['version'] = self.target.version
        return msg

    def cast(self, ctxt, method, **kwargs):
        msg = self._make_message(method, kwargs)
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

    def call(self, ctxt, method, **kwargs):
        msg = self._make_message(method, kwargs)

        timeout = self.timeout
        if self.timeout is None:
            timeout = self.conf.rpc_response_timeout

        if self.check_for_lock:
            self._check_for_lock()

        return self.transport._send(self.target, ctxt, msg,
                                    wait_for_reply=True, timeout=timeout)


class RPCClient(object):

    def __init__(self, transport, target, timeout=None, check_for_lock=None):
        self.conf = transport.conf
        self.conf.register_opts(_client_opts)

        self.transport = transport
        self.target = target
        self.timeout = timeout
        self.check_for_lock = check_for_lock

        super(RPCClient, self).__init__()

    def prepare(self, server=None, version=None,
                timeout=None, check_for_lock=None):
        target = self.target(server=server, version=version)

        if timeout is None:
            timeout = self.timeout
        if check_for_lock is None:
            check_for_lock = self.check_for_lock

        return _CallContext(self.transport, target, timeout, check_for_lock)

    def cast(self, ctxt, method, **kwargs):
        self.prepare().cast(ctxt, method, **kwargs)

    def call(self, ctxt, method, **kwargs):
        return self.prepare().call(ctxt, method, **kwargs)
