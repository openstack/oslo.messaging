
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

__all__ = ['MessagingException', 'MessagingTimeout', 'MessageDeliveryFailure',
           'InvalidTarget']

import six


class MessagingException(Exception):
    """Base class for exceptions."""


class MessagingTimeout(MessagingException):
    """Raised if message sending times out."""


class MessageDeliveryFailure(MessagingException):
    """Raised if message sending failed after the asked retry."""


class InvalidTarget(MessagingException, ValueError):
    """Raised if a target does not meet certain pre-conditions."""

    def __init__(self, msg, target):
        msg = msg + ":" + six.text_type(target)
        super(InvalidTarget, self).__init__(msg)
        self.target = target
