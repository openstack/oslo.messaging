
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

"""
An RPC server exposes a number of endpoints, each of which contain a set of
methods which may be invoked remotely by clients over a given transport.

To create an RPC server, you supply a transport, target and a list of
endpoints.

A transport can be obtained simply by calling the get_transport() method::

    transport = messaging.get_transport(conf)

which will load the appropriate transport driver according to the user's
messaging configuration configuration. See get_transport() for more details.

The target supplied when creating an RPC server expresses the topic, server
name and - optionally - the exchange to listen on. See Target for more details
on these attributes.

Each endpoint object may have a target attribute which may have namespace and
version fields set. By default, we use the 'null namespace' and version 1.0.
Incoming method calls will be dispatched to the first endpoint with the
requested method, a matching namespace and a compatible version number.

RPC servers have start(), stop() and wait() messages to begin handling
requests, stop handling requests and wait for all in-process requests to
complete.

A simple example of an RPC server with multiple endpoints might be::

    from oslo_config import cfg
    import oslo_messaging

    class ServerControlEndpoint(object):

        target = oslo_messaging.Target(namespace='control',
                                       version='2.0')

        def __init__(self, server):
            self.server = server

        def stop(self, ctx):
            if server:
                self.server.stop()

    class TestEndpoint(object):

        def test(self, ctx, arg):
            return arg

    transport = oslo_messaging.get_transport(cfg.CONF)
    target = oslo_messaging.Target(topic='test', server='server1')
    endpoints = [
        ServerControlEndpoint(None),
        TestEndpoint(),
    ]
    server = oslo_messaging.get_rpc_server(transport, target, endpoints,
                                           executor='blocking')
    server.start()
    server.wait()

Clients can invoke methods on the server by sending the request to a topic and
it gets sent to one of the servers listening on the topic, or by sending the
request to a specific server listening on the topic, or by sending the request
to all servers listening on the topic (known as fanout). These modes are chosen
via the server and fanout attributes on Target but the mode used is transparent
to the server.

The first parameter to method invocations is always the request context
supplied by the client.

Parameters to the method invocation are primitive types and so must be the
return values from the methods. By supplying a serializer object, a server can
deserialize a request context and arguments from - and serialize return values
to - primitive types.
"""

__all__ = [
    'get_rpc_server',
    'expected_exceptions',
]

from oslo_messaging.rpc import dispatcher as rpc_dispatcher
from oslo_messaging import server as msg_server


def get_rpc_server(transport, target, endpoints,
                   executor='blocking', serializer=None):
    """Construct an RPC server.

    The executor parameter controls how incoming messages will be received and
    dispatched. By default, the most simple executor is used - the blocking
    executor.

    If the eventlet executor is used, the threading and time library need to be
    monkeypatched.

    :param transport: the messaging transport
    :type transport: Transport
    :param target: the exchange, topic and server to listen on
    :type target: Target
    :param endpoints: a list of endpoint objects
    :type endpoints: list
    :param executor: name of a message executor - for example
                     'eventlet', 'blocking'
    :type executor: str
    :param serializer: an optional entity serializer
    :type serializer: Serializer
    """
    dispatcher = rpc_dispatcher.RPCDispatcher(target, endpoints, serializer)
    return msg_server.MessageHandlingServer(transport, dispatcher, executor)


def expected_exceptions(*exceptions):
    """Decorator for RPC endpoint methods that raise expected exceptions.

    Marking an endpoint method with this decorator allows the declaration
    of expected exceptions that the RPC server should not consider fatal,
    and not log as if they were generated in a real error scenario.

    Note that this will cause listed exceptions to be wrapped in an
    ExpectedException, which is used internally by the RPC sever. The RPC
    client will see the original exception type.
    """
    def outer(func):
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            # Take advantage of the fact that we can catch
            # multiple exception types using a tuple of
            # exception classes, with subclass detection
            # for free. Any exception that is not in or
            # derived from the args passed to us will be
            # ignored and thrown as normal.
            except exceptions:
                raise rpc_dispatcher.ExpectedException()
        return inner
    return outer
