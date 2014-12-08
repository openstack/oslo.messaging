# Copyright 2013 New Dream Network, LLC (DreamHost)
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


@six.add_metaclass(abc.ABCMeta)
class ExecutorBase(object):

    def __init__(self, conf, listener, dispatcher):
        self.conf = conf
        self.listener = listener
        self.dispatcher = dispatcher

    @abc.abstractmethod
    def start(self):
        "Start polling for incoming messages."

    @abc.abstractmethod
    def stop(self):
        "Stop polling for messages."

    @abc.abstractmethod
    def wait(self):
        "Wait until the executor has stopped polling."
