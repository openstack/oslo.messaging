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
    """Is raised if specified exchange is not found in RabbitMQ."""
    pass


class MessageRejectedException(exceptions.MessageDeliveryFailure):
    """Is raised if message which you are trying to send was nacked by RabbitMQ
    it may happen if RabbitMQ is not able to process message
    """
    pass


class RoutingException(exceptions.MessageDeliveryFailure):
    """Is raised if message can not be delivered to any queue. Usually it means
    that any queue is not binded to given exchange with given routing key.
    Raised if 'mandatory' flag specified only
    """
    pass


class ConnectionException(exceptions.MessagingException):
    """Is raised if some operation can not be performed due to connectivity
    problem
    """
    pass


class TimeoutConnectionException(ConnectionException):
    """Is raised if socket timeout was expired during network interaction"""
    pass


class EstablishConnectionException(ConnectionException):
    """Is raised if we have some problem during establishing connection
    procedure
    """
    pass


class HostConnectionNotAllowedException(EstablishConnectionException):
    """Is raised in case of try to establish connection to temporary
    not allowed host (because of reconnection policy for example)
    """
    pass


class UnsupportedDriverVersion(exceptions.MessagingException):
    """Is raised when message is received but was sent by different,
    not supported driver version
    """
    pass
