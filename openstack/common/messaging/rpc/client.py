
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
        """Invoke a method and return immediately. See RPCClient.cast()."""
        msg = self._make_message(ctxt, method, kwargs)
        self.transport._send(self.target, ctxt, msg)

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
        if not utils.version_is_compatible(self.version_cap, version):
            raise RpcVersionCapError(version=version,
                                     version_cap=self.version_cap)

    def call(self, ctxt, method, **kwargs):
        """Invoke a method and wait for a reply. See RPCClient.call()."""
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

    """A class for invoking methods on remote servers.

    The RPCClient class is responsible for sending method invocations to remote
    servers via a messaging transport.

    A default target is supplied to the RPCClient constructor, but target
    attributes can be overridden for individual method invocations using the
    prepare() method.

    A method invocation consists of a request context dictionary, a method name
    and a dictionary of arguments. A cast() invocation just sends the request
    and returns immediately. A call() invocation waits for the server to send
    a return value.

    This class is intended to be used by wrapping it in another class which
    provides methods on the subclass to perform the remote invocation using
    call() or cast()::

        class TestClient(object):

            def __init__(self, transport):
                target = messaging.Target(topic='testtopic', version='2.0')
                self._client = messaging.RPCClient(transport, target)

            def test(self, ctxt, arg):
                return self._client.call(ctxt, 'test', arg=arg)

    An example of using the prepare() method to override some attributes of the
    default target::

        def test(self, ctxt, arg):
            cctxt = self._client.prepare(version='2.5')
            return cctxt.call(ctxt, 'test', arg=arg)

    RPCClient have a number of other properties - timeout, check_for_lock and
    version_cap - which may make sense to override for some method invocations,
    so they too can be passed to prepare()::

        def test(self, ctxt, arg):
            cctxt = self._client.prepare(check_for_lock=False, timeout=10)
            return cctxt.call(ctxt, 'test', arg=arg)

    However, this class can be used directly without wrapping it another class.
    For example:

        transport = messaging.get_transport(cfg.CONF)
        target = messaging.Target(topic='testtopic', version='2.0')
        client = messaging.RPCClient(transport, target)
        client.call(ctxt, 'test', arg=arg)

    but this is probably only useful in limited circumstances as a wrapper
    class will usually help to make the code much more obvious.
    """

    def __init__(self, transport, target,
                 timeout=None, check_for_lock=None,
                 version_cap=None, serializer=None):
        """Construct an RPC client.

        :param transport: a messaging transport handle
        :type transport: Transport
        :param target: the default target for invocations
        :type target: Target
        :param timeout: an optional default timeout (in seconds) for call()s
        :type timeout: int or float
        :param check_for_lock: warn if a lockutils.synchronized lock is held
        :type check_for_lock: bool
        :param version_cap: raise a RpcVersionCapError version exceeds this cap
        :type version_cap: str
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        """
        self.conf = transport.conf
        self.conf.register_opts(_client_opts)

        self.transport = transport
        self.target = target
        self.timeout = timeout
        self.check_for_lock = check_for_lock
        self.version_cap = version_cap
        self.serializer = serializer or msg_serializer.NoOpSerializer()

        super(RPCClient, self).__init__()

    _marker = object()

    def prepare(self, exchange=_marker, topic=_marker, namespace=_marker,
                version=_marker, server=_marker, fanout=_marker,
                timeout=_marker, check_for_lock=_marker, version_cap=_marker):
        """Prepare a method invocation context.

        Use this method to override client properties for an individual method
        invocation. For example::

            def test(self, ctxt, arg):
                cctxt = self.prepare(version='2.5')
                return cctxt.call(ctxt, 'test', arg=arg)

        :param exchange: see Target.exchange
        :type exchange: str
        :param topic: see Target.topic
        :type topic: str
        :param namespace: see Target.namespace
        :type namespace: str
        :param version: requirement the server must support, see Target.version
        :type version: str
        :param server: send to a specific server, see Target.server
        :type server: str
        :param fanout: send to all servers on topic, see Target.fanout
        :type fanout: bool
        :param timeout: an optional default timeout (in seconds) for call()s
        :type timeout: int or float
        :param check_for_lock: warn if a lockutils.synchronized lock is held
        :type check_for_lock: bool
        :param version_cap: raise a RpcVersionCapError version exceeds this cap
        :type version_cap: str
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        """
        kwargs = dict(
            exchange=exchange,
            topic=topic,
            namespace=namespace,
            version=version,
            server=server,
            fanout=fanout)
        kwargs = dict([(k, v) for k, v in kwargs.items() if v is not self._marker])
        target = self.target(**kwargs)

        if timeout is self._marker:
            timeout = self.timeout
        if check_for_lock is self._marker:
            check_for_lock = self.check_for_lock
        if version_cap is self._marker:
            version_cap = self.version_cap

        return _CallContext(self.transport, target,
                            self.serializer,
                            timeout, check_for_lock,
                            version_cap)

    def cast(self, ctxt, method, **kwargs):
        """Invoke a method and return immediately.

        Method arguments must either be primitive types or types supported by
        the client's serializer (if any).

        :param ctxt: a request context dict
        :type ctxt: dict
        :param method: the method name
        :type method: str
        :param kwargs: a dict of method arguments
        :param kwargs: dict
        """
        self.prepare().cast(ctxt, method, **kwargs)

    def call(self, ctxt, method, **kwargs):
        """Invoke a method and wait for a reply.

        Method arguments must either be primitive types or types supported by
        the client's serializer (if any).

        :param ctxt: a request context dict
        :type ctxt: dict
        :param method: the method name
        :type method: str
        :param kwargs: a dict of method arguments
        :param kwargs: dict
        :raises: MessagingTimeout
        """
        return self.prepare().call(ctxt, method, **kwargs)
