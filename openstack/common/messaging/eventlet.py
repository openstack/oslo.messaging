
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

from openstack.common.messaging._executors import impl_eventlet
from openstack.common.messaging.rpc import server


class EventletRPCServer(server._RPCServer):

    """An RPC server which integrates with eventlet.

    This is an RPC server which polls for incoming messages from a greenthread
    and dispatches each message in its own greenthread.

    The stop() method kills the message polling greenthread and the wait()
    method waits for all message dispatch greenthreads to complete.
    """

    def __init__(self, transport, target, endpoints, serializer=None):
        """Construct a new eventlet RPC server.

        :param transport: the messaging transport
        :type transport: Transport
        :param target: the exchange, topic and server to listen on
        :type target: Target
        :param endpoints: a list of endpoint objects
        :type endpoints: list
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        """
        executor_cls = impl_eventlet.EventletExecutor
        super(EventletRPCServer, self).__init__(transport,
                                                target,
                                                endpoints,
                                                serializer,
                                                executor_cls)
