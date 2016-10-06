#    Copyright 2016 Mirantis, Inc.
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

import abc
import contextlib
import logging

import retrying
import six

from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


def _drop_message_warn(request):
    LOG.warning(_LW("Matchmaker contains no records for specified "
                    "target %(target)s. Dropping message %(msg_id)s.")
                % {"target": request.target,
                   "msg_id": request.message_id})


def target_not_found_warn(func):
    def _target_not_found_warn(self, request, *args, **kwargs):
        try:
            return func(self, request, *args, **kwargs)
        except (zmq_matchmaker_base.MatchmakerUnavailable,
                retrying.RetryError):
            _drop_message_warn(request)
    return _target_not_found_warn


def target_not_found_timeout(func):
    def _target_not_found_timeout(self, request, *args, **kwargs):
        try:
            return func(self, request, *args, **kwargs)
        except (zmq_matchmaker_base.MatchmakerUnavailable,
                retrying.RetryError):
            _drop_message_warn(request)
            self.publisher._raise_timeout(request)
    return _target_not_found_timeout


@six.add_metaclass(abc.ABCMeta)
class PublisherManagerBase(object):

    """Abstract publisher manager class

    Publisher knows how to establish connection, how to send message,
    and how to receive reply. PublisherManager coordinates all these steps
    regarding retrying logic in AckManager implementations
    """

    def __init__(self, publisher):
        self.publisher = publisher
        self.conf = publisher.conf
        self.sender = publisher.sender
        self.receiver = publisher.receiver

    @abc.abstractmethod
    def send_call(self, request):
        """Send call request

        :param request: request object
        :type request: zmq_request.CallRequest
        """

    @abc.abstractmethod
    def send_cast(self, request):
        """Send cast request

        :param request: request object
        :type request: zmq_request.CastRequest
        """

    @abc.abstractmethod
    def send_fanout(self, request):
        """Send fanout request

        :param request: request object
        :type request: zmq_request.FanoutRequest
        """

    @abc.abstractmethod
    def send_notify(self, request):
        """Send notification request

        :param request: request object
        :type request: zmq_request.NotificationRequest
        """

    def cleanup(self):
        self.publisher.cleanup()


class PublisherManagerDynamic(PublisherManagerBase):

    @target_not_found_timeout
    def send_call(self, request):
        with contextlib.closing(self.publisher.acquire_connection(request)) \
                as socket:
            self.publisher.send_request(socket, request)
            reply = self.publisher.receive_reply(socket, request)
            return reply

    @target_not_found_warn
    def _send(self, request):
        with contextlib.closing(self.publisher.acquire_connection(request)) \
                as socket:
            self.publisher.send_request(socket, request)

    send_cast = _send
    send_fanout = _send
    send_notify = _send


class PublisherManagerStatic(PublisherManagerBase):

    @target_not_found_timeout
    def send_call(self, request):
        socket = self.publisher.acquire_connection(request)
        self.publisher.send_request(socket, request)
        reply = self.publisher.receive_reply(socket, request)
        return reply

    @target_not_found_warn
    def _send(self, request):
        socket = self.publisher.acquire_connection(request)
        self.publisher.send_request(socket, request)

    send_cast = _send
    send_fanout = _send
    send_notify = _send
