
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

from openstack.common.messaging._executors import impl_blocking
from openstack.common.messaging import _server
from openstack.common.messaging.rpc import _dispatcher

"""
An RPC server exposes a number of endpoints, each of which contain a set of
methods which may be invoked remotely by clients over a given transport.

To create an RPC server, you supply a transport, target and a list of
endpoints.

A transport can be obtained simply by calling the get_transport() method:

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

An RPC server class is provided for each supported I/O handling framework.
Currently BlockingRPCServer and eventlet.RPCServer are available.

A simple example of an RPC server with multiple endpoints might be:

    from oslo.config import cfg
    from openstack.common import messaging

    class ServerControlEndpoint(object):

        target = messaging.Target(namespace='control',
                                  version='2.0')

        def __init__(self, server):
            self.server = server

        def stop(self, ctx):
            self.server.stop()

    class TestEndpoint(object):

        def test(self, ctx, arg):
            return arg

    transport = messaging.get_transport(cfg.CONF)
    target = messaging.Target(topic='test', server='server1')
    endpoints = [
        ServerControlEndpoint(self),
        TestEndpoint(),
    ]
    server = messaging.BlockingRPCServer(transport, target, endpoints)
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
deserialize arguments from - serialize return values to - primitive types.
"""


class _RPCServer(_server.MessageHandlingServer):

    def __init__(self, transport, target, endpoints, serializer, executor_cls):
        dispatcher = _dispatcher.RPCDispatcher(endpoints, serializer)
        super(_RPCServer, self).__init__(transport,
                                         target,
                                         dispatcher,
                                         executor_cls)


class BlockingRPCServer(_RPCServer):

    """An RPC server which blocks and dispatches in the current thread.

    The blocking RPC server is a very simple RPC server whose start() method
    functions as a request processing loop - i.e. it blocks, processes messages
    and only returns when stop() is called from a dispatched method.

    Method calls are dispatched in the current thread, so only a single method
    call can be executing at once.

    This class is likely to only be useful for simple demo programs.
    """

    def __init__(self, transport, target, endpoints, serializer=None):
        """Construct a new blocking RPC server.

        :param transport: the messaging transport
        :type transport: Transport
        :param target: the exchange, topic and server to listen on
        :type target: Target
        :param endpoints: a list of endpoint objects
        :type endpoints: list
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        """
        executor_cls = impl_blocking.BlockingExecutor
        super(BlockingRPCServer, self).__init__(transport,
                                                target,
                                                endpoints,
                                                serializer,
                                                executor_cls)
