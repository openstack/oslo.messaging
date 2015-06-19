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

import abc

import six

from oslo_messaging._drivers.zmq_driver import zmq_async


@six.add_metaclass(abc.ABCMeta)
class BaseProxy(object):

    def __init__(self, conf, context):
        super(BaseProxy, self).__init__()
        self.conf = conf
        self.context = context
        self.executor = zmq_async.get_executor(self.run)

    @abc.abstractmethod
    def run(self):
        "Main execution point of the proxy"

    def start(self):
        self.executor.execute()

    def stop(self):
        self.executor.stop()

    def wait(self):
        self.executor.wait()


@six.add_metaclass(abc.ABCMeta)
class BaseTcpFrontend(object):

    def __init__(self, conf, poller, context):
        self.conf = conf
        self.poller = poller
        self.context = context

    def receive_incoming(self):
        message, socket = self.poller.poll(1)
        return message


@six.add_metaclass(abc.ABCMeta)
class BaseBackendMatcher(object):

    def __init__(self, conf, poller, context):
        self.conf = conf
        self.context = context
        self.backends = {}
        self.poller = poller

    def redirect_to_backend(self, message):
        backend, topic = self._match_backend(message)
        self._send_message(backend, message, topic)

    def _match_backend(self, message):
        topic = self._get_topic(message)
        ipc_address = self._get_ipc_address(topic)
        if ipc_address not in self.backends:
            self._create_backend(ipc_address)
        return self.backend, topic

    @abc.abstractmethod
    def _get_topic(self, message):
        "Extract topic from message"

    @abc.abstractmethod
    def _get_ipc_address(self, topic):
        "Get ipc backend address from topic"

    @abc.abstractmethod
    def _send_message(self, backend, message, topic):
        "Backend specific sending logic"

    @abc.abstractmethod
    def _create_backend(self, ipc_address):
        "Backend specific socket opening logic"
