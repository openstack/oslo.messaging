
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

import abc

import six

from oslo.messaging import exceptions


class TransportDriverError(exceptions.MessagingException):
    """Base class for transport driver specific exceptions."""


@six.add_metaclass(abc.ABCMeta)
class IncomingMessage(object):

    def __init__(self, listener, ctxt, message):
        self.conf = listener.conf
        self.listener = listener
        self.ctxt = ctxt
        self.message = message

    @abc.abstractmethod
    def reply(self, reply=None, failure=None, log_failure=True):
        "Send a reply or failure back to the client."

    def acknowledge(self):
        "Acknowledge the message."

    @abc.abstractmethod
    def requeue(self):
        "Requeue the message."


@six.add_metaclass(abc.ABCMeta)
class Listener(object):

    def __init__(self, driver):
        self.conf = driver.conf
        self.driver = driver

    @abc.abstractmethod
    def poll(self, timeout=None):
        """Blocking until a message is pending and return IncomingMessage.
        Return None after timeout seconds if timeout is set and no message is
        ending.
        """


@six.add_metaclass(abc.ABCMeta)
class BaseDriver(object):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        self.conf = conf
        self._url = url
        self._default_exchange = default_exchange
        self._allowed_remote_exmods = allowed_remote_exmods

    def require_features(self, requeue=False):
        if requeue:
            raise NotImplementedError('Message requeueing not supported by '
                                      'this transport driver')

    @abc.abstractmethod
    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""

    @abc.abstractmethod
    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""

    @abc.abstractmethod
    def listen(self, target):
        """Construct a Listener for the given target."""

    @abc.abstractmethod
    def listen_for_notifications(self, targets_and_priorities):
        """Construct a notification Listener for the given list of
        tuple of (target, priority).
        """

    @abc.abstractmethod
    def cleanup(self):
        """Release all resources."""
