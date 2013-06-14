
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

from openstack.common.messaging import exceptions
from openstack.common.messaging.rpc import client
from openstack.common.messaging.rpc import dispatcher as rpc_dispatcher
from openstack.common.messaging.rpc import server as rpc_server
from openstack.common.messaging import serializer
from openstack.common.messaging import server
from openstack.common.messaging import target
from openstack.common.messaging import transport


get_transport = transport.get_transport
Target = target.Target

RPCClient = client.RPCClient

MessageHandlingServer = server.MessageHandlingServer
get_rpc_server = rpc_server.get_rpc_server
RPCDispatcher = rpc_dispatcher.RPCDispatcher
Serializer = serializer.Serializer


#
# Exceptions
#
MessagingException = exceptions.MessagingException
MessagingTimeout = exceptions.MessagingTimeout

DriverLoadFailure = transport.DriverLoadFailure
InvalidTransportURL = transport.InvalidTransportURL

RPCVersionCapError = client.RPCVersionCapError

MessagingServerError = server.MessagingServerError
ExecutorLoadFailure = server.ExecutorLoadFailure
ServerListenError = server.ServerListenError

RPCDispatcherError = rpc_dispatcher.RPCDispatcherError
NoSuchMethod = rpc_dispatcher.NoSuchMethod
UnsupportedVersion = rpc_dispatcher.UnsupportedVersion
