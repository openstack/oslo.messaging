
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

from openstack.common.messaging import _server
from openstack.common.messaging._executors import impl_blocking
from openstack.common.messaging.rpc import _dispatcher


class _RPCServer(_server.MessageHandlingServer):

    def __init__(self, transport, target, endpoints, executor_cls):
        super(_RPCServer, self).__init__(transport,
                                         target,
                                         _dispatcher.RPCDispatcher(endpoints),
                                         executor_cls)


class BlockingRPCServer(_RPCServer):

    def __init__(self, transport, target, endpoints):
        executor_cls = impl_blocking.BlockingExecutor
        super(BlockingRPCServer, self).__init__(transport,
                                                target,
                                                endpoints,
                                                executor_cls)
