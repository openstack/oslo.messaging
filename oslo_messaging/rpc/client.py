
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

__all__ = [
    'ClientSendError',
    'RPCClient',
    'RPCVersionCapError',
    'RemoteError',
]

import abc

from oslo_config import cfg
import six

from oslo_messaging._drivers import base as driver_base
from oslo_messaging import _utils as utils
from oslo_messaging import exceptions
from oslo_messaging import serializer as msg_serializer

_client_opts = [
    cfg.IntOpt('rpc_response_timeout',
               default=60,
               help='Seconds to wait for a response from a call.'),
]


class RemoteError(exceptions.MessagingException):

    """Signifies that a remote endpoint method has raised an exception.

    Contains a string representation of the type of the original exception,
    the value of the original exception, and the traceback.  These are
    sent to the parent as a joined string so printing the exception
    contains all of the relevant info.
    """

    def __init__(self, exc_type=None, value=None, traceback=None):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback
        msg = ("Remote error: %(exc_type)s %(value)s\n%(traceback)s." %
               dict(exc_type=self.exc_type, value=self.value,
                    traceback=self.traceback))
        super(RemoteError, self).__init__(msg)


class RPCVersionCapError(exceptions.MessagingException):

    def __init__(self, version, version_cap):
        self.version = version
        self.version_cap = version_cap
        msg = ("Requested message version, %(version)s is incompatible.  It "
               "needs to be equal in major version and less than or equal "
               "in minor version as the specified version cap "
               "%(version_cap)s." %
               dict(version=self.version, version_cap=self.version_cap))
        super(RPCVersionCapError, self).__init__(msg)


class ClientSendError(exceptions.MessagingException):
    """Raised if we failed to send a message to a target."""

    def __init__(self, target, ex):
        msg = 'Failed to send to target "%s": %s' % (target, ex)
        super(ClientSendError, self).__init__(msg)
        self.target = target
        self.ex = ex


@six.add_metaclass(abc.ABCMeta)
class _BaseCallContext(object):

    _marker = object()

    def __init__(self, transport, target, serializer,
                 timeout=None, version_cap=None, retry=None):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.serializer = serializer
        self.timeout = timeout
        self.retry = retry
        self.version_cap = version_cap

        super(_BaseCallContext, self).__init__()

    def _make_message(self, ctxt, method, args):
        msg = dict(method=method)

        msg['args'] = dict()
        for argname, arg in args.items():
            msg['args'][argname] = self.serializer.serialize_entity(ctxt, arg)

        if self.target.namespace is not None:
            msg['namespace'] = self.target.namespace
        if self.target.version is not None:
            msg['version'] = self.target.version

        return msg

    def _check_version_cap(self, version):
        if not utils.version_is_compatible(self.version_cap, version):
            raise RPCVersionCapError(version=version,
                                     version_cap=self.version_cap)

    def can_send_version(self, version=_marker):
        """Check to see if a version is compatible with the version cap."""
        version = self.target.version if version is self._marker else version
        return utils.version_is_compatible(self.version_cap, version)

    @classmethod
    def _check_version(cls, version):
        if version is not cls._marker:
            # quick sanity check to make sure parsable version numbers are used
            try:
                utils.version_is_compatible(version, version)
            except (IndexError, ValueError):
                raise exceptions.MessagingException(
                    "Version must contain a major and minor integer. Got %s"
                    % version)

    def cast(self, ctxt, method, **kwargs):
        """Invoke a method and return immediately. See RPCClient.cast()."""
        msg = self._make_message(ctxt, method, kwargs)
        msg_ctxt = self.serializer.serialize_context(ctxt)

        self._check_version_cap(msg.get('version'))

        try:
            self.transport._send(self.target, msg_ctxt, msg, retry=self.retry)
        except driver_base.TransportDriverError as ex:
            raise ClientSendError(self.target, ex)

    def call(self, ctxt, method, **kwargs):
        """Invoke a method and wait for a reply. See RPCClient.call()."""
        if self.target.fanout:
            raise exceptions.InvalidTarget('A call cannot be used with fanout',
                                           self.target)

        msg = self._make_message(ctxt, method, kwargs)
        msg_ctxt = self.serializer.serialize_context(ctxt)

        timeout = self.timeout
        if self.timeout is None:
            timeout = self.conf.rpc_response_timeout

        self._check_version_cap(msg.get('version'))

        try:
            result = self.transport._send(self.target, msg_ctxt, msg,
                                          wait_for_reply=True, timeout=timeout,
                                          retry=self.retry)
        except driver_base.TransportDriverError as ex:
            raise ClientSendError(self.target, ex)

        return self.serializer.deserialize_entity(ctxt, result)

    @abc.abstractmethod
    def prepare(self, exchange=_marker, topic=_marker, namespace=_marker,
                version=_marker, server=_marker, fanout=_marker,
                timeout=_marker, version_cap=_marker, retry=_marker):
        """Prepare a method invocation context. See RPCClient.prepare()."""


class _CallContext(_BaseCallContext):

    _marker = _BaseCallContext._marker

    @classmethod
    def _prepare(cls, call_context,
                 exchange=_marker, topic=_marker, namespace=_marker,
                 version=_marker, server=_marker, fanout=_marker,
                 timeout=_marker, version_cap=_marker, retry=_marker):
        cls._check_version(version)
        kwargs = dict(
            exchange=exchange,
            topic=topic,
            namespace=namespace,
            version=version,
            server=server,
            fanout=fanout)
        kwargs = dict([(k, v) for k, v in kwargs.items()
                       if v is not cls._marker])
        target = call_context.target(**kwargs)

        if timeout is cls._marker:
            timeout = call_context.timeout
        if version_cap is cls._marker:
            version_cap = call_context.version_cap
        if retry is cls._marker:
            retry = call_context.retry

        return _CallContext(call_context.transport, target,
                            call_context.serializer,
                            timeout, version_cap, retry)

    def prepare(self, exchange=_marker, topic=_marker, namespace=_marker,
                version=_marker, server=_marker, fanout=_marker,
                timeout=_marker, version_cap=_marker, retry=_marker):
        return _CallContext._prepare(self,
                                     exchange, topic, namespace,
                                     version, server, fanout,
                                     timeout, version_cap, retry)


class RPCClient(_BaseCallContext):

    """A class for invoking methods on remote RPC servers.

    The RPCClient class is responsible for sending method invocations to and
    receiving return values from remote RPC servers via a messaging transport.

    Two RPC patterns are supported: RPC calls and RPC casts.

    An RPC cast is used when an RPC method does *not* return a value to
    the caller. An RPC call is used when a return value is expected from the
    method.  For further information see the cast() and call() methods.

    The default target used for all subsequent calls and casts is supplied to
    the RPCClient constructor.  The client uses the target to control how the
    RPC request is delivered to a server.  If only the target's topic (and
    optionally exchange) are set, then the RPC can be serviced by any server
    that is listening to that topic (and exchange).  If multiple servers are
    listening on that topic/exchange, then one server is picked using a
    best-effort round-robin algorithm.  Alternatively, the client can set the
    Target's ``server`` attribute to the name of a specific server to send the
    RPC request to one particular server.  In the case of RPC cast, the RPC
    request can be broadcast to all servers listening to the Target's
    topic/exchange by setting the Target's ``fanout`` property to ``True``.

    While the default target is set on construction, target attributes can be
    overridden for individual method invocations using the prepare() method.

    A method invocation consists of a request context dictionary, a method name
    and a dictionary of arguments.

    This class is intended to be used by wrapping it in another class which
    provides methods on the subclass to perform the remote invocation using
    call() or cast()::

        class TestClient(object):

            def __init__(self, transport):
                target = messaging.Target(topic='test', version='2.0')
                self._client = messaging.RPCClient(transport, target)

            def test(self, ctxt, arg):
                return self._client.call(ctxt, 'test', arg=arg)

    An example of using the prepare() method to override some attributes of the
    default target::

        def test(self, ctxt, arg):
            cctxt = self._client.prepare(version='2.5')
            return cctxt.call(ctxt, 'test', arg=arg)

    RPCClient have a number of other properties - for example, timeout and
    version_cap - which may make sense to override for some method invocations,
    so they too can be passed to prepare()::

        def test(self, ctxt, arg):
            cctxt = self._client.prepare(timeout=10)
            return cctxt.call(ctxt, 'test', arg=arg)

    However, this class can be used directly without wrapping it another class.
    For example::

        transport = messaging.get_transport(cfg.CONF)
        target = messaging.Target(topic='test', version='2.0')
        client = messaging.RPCClient(transport, target)
        client.call(ctxt, 'test', arg=arg)

    but this is probably only useful in limited circumstances as a wrapper
    class will usually help to make the code much more obvious.

    If the connection to the messaging service is not active when an RPC
    request is made the client will block waiting for the connection to
    complete.  If the connection fails to complete, the client will try to
    re-establish that connection. By default this will continue indefinitely
    until the connection completes. However, the retry parameter can be used to
    have the RPC request fail with a MessageDeliveryFailure after the given
    number of retries. For example::

        client = messaging.RPCClient(transport, target, retry=None)
        client.call(ctxt, 'sync')
        try:
            client.prepare(retry=0).cast(ctxt, 'ping')
        except messaging.MessageDeliveryFailure:
            LOG.error("Failed to send ping message")
    """

    _marker = _BaseCallContext._marker

    def __init__(self, transport, target,
                 timeout=None, version_cap=None, serializer=None, retry=None):
        """Construct an RPC client.

        :param transport: a messaging transport handle
        :type transport: Transport
        :param target: the default target for invocations
        :type target: Target
        :param timeout: an optional default timeout (in seconds) for call()s
        :type timeout: int or float
        :param version_cap: raise a RPCVersionCapError version exceeds this cap
        :type version_cap: str
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        :param retry: an optional default connection retries configuration:
                      None or -1 means to retry forever.
                      0 means no retry is attempted.
                      N means attempt at most N retries.
        :type retry: int
        """
        if serializer is None:
            serializer = msg_serializer.NoOpSerializer()

        super(RPCClient, self).__init__(
            transport, target, serializer, timeout, version_cap, retry
        )

        self.conf.register_opts(_client_opts)

    def prepare(self, exchange=_marker, topic=_marker, namespace=_marker,
                version=_marker, server=_marker, fanout=_marker,
                timeout=_marker, version_cap=_marker, retry=_marker):
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
        :param version_cap: raise a RPCVersionCapError version exceeds this cap
        :type version_cap: str
        :param retry: an optional connection retries configuration:
                      None or -1 means to retry forever.
                      0 means no retry is attempted.
                      N means attempt at most N retries.
        :type retry: int
        """
        return _CallContext._prepare(self,
                                     exchange, topic, namespace,
                                     version, server, fanout,
                                     timeout, version_cap, retry)

    def cast(self, ctxt, method, **kwargs):
        """Invoke a method without blocking for a return value.

        The cast() method is used to invoke an RPC method that does not return
        a value.  cast() RPC requests may be broadcast to all Servers listening
        on a given topic by setting the fanout Target property to ``True``.

        The cast() operation is best-effort: cast() will block the
        calling thread until the RPC request method is accepted by the
        messaging transport, but cast() does *not* verify that the RPC method
        has been invoked by the server. cast() does guarantee that the
        method will be not executed twice on a destination (e.g. 'at-most-once'
        execution).

        There are no ordering guarantees across successive casts, even
        among casts to the same destination. Therefore methods may be executed
        in an order different from the order in which they are cast.

        Method arguments must either be primitive types or types supported by
        the client's serializer (if any).

        Similarly, the request context must be a dict unless the client's
        serializer supports serializing another type.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param method: the method name
        :type method: str
        :param kwargs: a dict of method arguments
        :type kwargs: dict
        :raises: MessageDeliveryFailure if the messaging transport fails to
                 accept the request.

        """
        self.prepare().cast(ctxt, method, **kwargs)

    def call(self, ctxt, method, **kwargs):
        """Invoke a method and wait for a reply.

        The call() method is used to invoke RPC methods that return a
        value. Since only a single return value is permitted it is not possible
        to call() to a fanout target.

        call() will block the calling thread until the messaging transport
        provides the return value, a timeout occurs, or a non-recoverable error
        occurs.

        call() guarantees that the RPC request is done 'at-most-once' which
        ensures that the call will never be duplicated.  However if the call
        should fail or time out before the return value arrives then there are
        no guarantees whether or not the method was invoked.

        Since call() blocks until completion of the RPC method, call()s from
        the same thread are guaranteed to be processed in-order.

        Method arguments must either be primitive types or types supported by
        the client's serializer (if any). Similarly, the request context must
        be a dict unless the client's serializer supports serializing another
        type.

        The semantics of how any errors raised by the remote RPC endpoint
        method are handled are quite subtle.

        Firstly, if the remote exception is contained in one of the modules
        listed in the allow_remote_exmods messaging.get_transport() parameter,
        then it this exception will be re-raised by call(). However, such
        locally re-raised remote exceptions are distinguishable from the same
        exception type raised locally because re-raised remote exceptions are
        modified such that their class name ends with the '_Remote' suffix so
        you may do::

            if ex.__class__.__name__.endswith('_Remote'):
                # Some special case for locally re-raised remote exceptions

        Secondly, if a remote exception is not from a module listed in the
        allowed_remote_exmods list, then a messaging.RemoteError exception is
        raised with all details of the remote exception.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param method: the method name
        :type method: str
        :param kwargs: a dict of method arguments
        :type kwargs: dict
        :raises: MessagingTimeout, RemoteError, MessageDeliveryFailure
        """
        return self.prepare().call(ctxt, method, **kwargs)

    def can_send_version(self, version=_marker):
        """Check to see if a version is compatible with the version cap."""
        return self.prepare(version=version).can_send_version()
