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

A transport can be obtained simply by calling the get_rpc_transport() method::

    transport = messaging.get_rpc_transport(conf)

which will load the appropriate transport driver according to the user's
messaging configuration. See get_rpc_transport() for more details.

The target supplied when creating an RPC server expresses the topic, server
name and - optionally - the exchange to listen on. See Target for more details
on these attributes.

Multiple RPC Servers may listen to the same topic (and exchange)
simultaneously. See RPCClient for details regarding how RPC requests are
distributed to the Servers in this case.

Each endpoint object may have a target attribute which may have namespace and
version fields set. By default, we use the 'null namespace' and version 1.0.
Incoming method calls will be dispatched to the first endpoint with the
requested method, a matching namespace and a compatible version number.

The first parameter to method invocations is always the request context
supplied by the client.  The remaining parameters are the arguments supplied to
the method by the client.  Endpoint methods may return a value.  If so the RPC
Server will send the returned value back to the requesting client via the
transport.

The executor parameter controls how incoming messages will be received and
dispatched. Refer to the Executor documentation for descriptions of the types
of executors.

*Note:* If the "eventlet" executor is used, the threading and time library need
to be monkeypatched. The Eventlet executor is deprecated and the threading
executor will be the only available executor.

The RPC reply operation is best-effort: the server will consider the message
containing the reply successfully sent once it is accepted by the messaging
transport.  The server does not guarantee that the reply is processed by the
RPC client.  If the send fails an error will be logged and the server will
continue to processing incoming RPC requests.

Parameters to the method invocation and values returned from the method are
python primitive types. However the actual encoding of the data in the message
may not be in primitive form (e.g. the message payload may be a dictionary
encoded as an ASCII string using JSON). A serializer object is used to convert
incoming encoded message data to primitive types.  The serializer is also used
to convert the return value from primitive types to an encoding suitable for
the message payload.

RPC servers have start(), stop() and wait() methods to begin handling
requests, stop handling requests, and wait for all in-process requests to
complete after the Server has been stopped.

A simple example of an RPC server with multiple endpoints might be::

    # NOTE(changzhi): We are using eventlet executor and
    # time.sleep(1), therefore, the server code needs to be
    # monkey-patched.

    import eventlet
    eventlet.monkey_patch()

    from oslo_config import cfg
    import oslo_messaging
    import time

    class ServerControlEndpoint(object):

        target = oslo_messaging.Target(namespace='control',
                                       version='2.0')

        def __init__(self, server):
            self.server = server

        def stop(self, ctx):
            if self.server:
                self.server.stop()

    class TestEndpoint(object):

        def test(self, ctx, arg):
            return arg

    transport = oslo_messaging.get_rpc_transport(cfg.CONF)
    target = oslo_messaging.Target(topic='test', server='server1')
    endpoints = [
        ServerControlEndpoint(None),
        TestEndpoint(),
    ]
    server = oslo_messaging.get_rpc_server(transport, target, endpoints,
                                           executor='eventlet')
    try:
        server.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping server")

    server.stop()
    server.wait()

"""

import logging
import sys
import time

import debtcollector
from oslo_messaging import exceptions
from oslo_messaging.rpc import dispatcher as rpc_dispatcher
from oslo_messaging import server as msg_server
from oslo_messaging import transport as msg_transport

__all__ = [
    'get_rpc_server',
    'expected_exceptions',
    'expose'
]

LOG = logging.getLogger(__name__)


class RPCServer(msg_server.MessageHandlingServer):
    def __init__(self, transport, target, dispatcher, executor=None):
        super().__init__(transport, dispatcher, executor)
        if not isinstance(transport, msg_transport.RPCTransport):
            LOG.warning("Using notification transport for RPC. Please use "
                        "get_rpc_transport to obtain an RPC transport "
                        "instance.")
        self._target = target

    def _create_listener(self):
        return self.transport._listen(self._target, 1, None)

    def _process_incoming(self, incoming):
        message = incoming[0]
        rpc_method = message.message.get('method')
        start = time.time()
        LOG.debug("Receive incoming message with id %(msg_id)s and "
                  "method: %(method)s.",
                  {"msg_id": message.msg_id,
                   "method": rpc_method})

        # TODO(sileht): We should remove that at some point and do
        # this directly in the driver
        try:
            message.acknowledge()
        except Exception:
            LOG.exception("Can not acknowledge message. Skip processing")
            return

        failure = None
        try:
            res = self.dispatcher.dispatch(message)
        except rpc_dispatcher.ExpectedException as e:
            # current sys.exc_info() content can be overridden
            # by another exception raised by a log handler during
            # LOG.debug(). So keep a copy and delete it later.
            failure = e.exc_info
            LOG.debug('Expected exception during message handling (%s)', e)
        except rpc_dispatcher.NoSuchMethod as e:
            failure = sys.exc_info()
            if e.method.endswith('_ignore_errors'):
                LOG.debug('Method %s not found', e.method)
            else:
                LOG.exception('Exception during message handling')
        except Exception:
            failure = sys.exc_info()
            LOG.exception('Exception during message handling')

        try:
            if failure is None:
                message.reply(res)
                LOG.debug("Replied success message with id %(msg_id)s and "
                          "method: %(method)s. Time elapsed: %(elapsed).3f",
                          {"msg_id": message.msg_id,
                           "method": rpc_method,
                           "elapsed": (time.time() - start)})
            else:
                message.reply(failure=failure)
                LOG.debug("Replied failure for incoming message with "
                          "id %(msg_id)s and method: %(method)s. "
                          "Time elapsed: %(elapsed).3f",
                          {"msg_id": message.msg_id,
                           "method": rpc_method,
                           "elapsed": (time.time() - start)})
        except exceptions.MessageUndeliverable as e:
            LOG.exception(
                "MessageUndeliverable error, "
                "source exception: %s, routing_key: %s, exchange: %s: ",
                e.exception, e.routing_key, e.exchange
            )
        except Exception:
            LOG.exception("Can not send reply for message")
        finally:
            # NOTE(dhellmann): Remove circular object reference
            # between the current stack frame and the traceback in
            # exc_info.
            del failure


@debtcollector.removals.removed_kwarg(
    'executor',
    message="the eventlet executor is now deprecated. Threading "
            "will be the only execution model available.")
def get_rpc_server(transport, target, endpoints,
                   executor=None, serializer=None, access_policy=None,
                   server_cls=RPCServer):
    """Construct an RPC server.

    :param transport: the messaging transport
    :type transport: Transport
    :param target: the exchange, topic and server to listen on
    :type target: Target
    :param endpoints: a list of endpoint objects
    :type endpoints: list
    :param executor: (DEPRECATED) name of message executor -
        available values are 'eventlet' and 'threading'.
        The Eventlet executor is also deprecated.
    :type executor: str
    :param serializer: an optional entity serializer
    :type serializer: Serializer
    :param access_policy: an optional access policy.
           Defaults to DefaultRPCAccessPolicy
    :type access_policy: RPCAccessPolicyBase
    :param server_cls: The server class to instantiate
    :type server_cls: class
    """
    dispatcher = rpc_dispatcher.RPCDispatcher(endpoints, serializer,
                                              access_policy)
    return server_cls(transport, target, dispatcher, executor)


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


def expose(func):
    """Decorator for RPC endpoint methods that are exposed to the RPC client.

    If the dispatcher's access_policy is set to ExplicitRPCAccessPolicy then
    endpoint methods need to be explicitly exposed.::

        # foo() cannot be invoked by an RPC client
        def foo(self):
            pass

        # bar() can be invoked by an RPC client
        @rpc.expose
        def bar(self):
            pass

    """

    func.exposed = True

    return func
