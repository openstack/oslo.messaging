#    Copyright 2015 Mirantis, Inc.
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

from oslo_messaging import exceptions


class ExchangeNotFoundException(exceptions.MessageDeliveryFailure):
    pass


class MessageRejectedException(exceptions.MessageDeliveryFailure):
    pass


class RoutingException(exceptions.MessageDeliveryFailure):
    pass


class ConnectionException(exceptions.MessagingException):
    pass


class HostConnectionNotAllowedException(ConnectionException):
    pass


class EstablishConnectionException(ConnectionException):
    pass


class TimeoutConnectionException(ConnectionException):
    pass
