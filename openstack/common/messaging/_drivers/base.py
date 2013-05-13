
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


class Listener(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, driver, target):
        self.driver = driver
        self.target = target

    @abc.abstractmethod
    def poll(self):
        # returns (ctxt, message)
        pass

    @abc.abstractmethod
    def done(self, ctxt, message):
        # so the transport can ack the message
        pass

    @abc.abstractmethod
    def reply(self, ctxt, reply=None, failure=None):
        pass


class BaseDriver(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, conf, url=None, default_exchange=None):
        self.conf = conf
        self._url = url
        self._default_exchange = default_exchange

    @abc.abstractmethod
    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None):
        """Send a message to the given target."""
        return None

    @abc.abstractmethod
    def listen(self, target):
        """Construct a Listener for the given target."""
        return None
