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

    """Base TCP-proxy.

    TCP-proxy redirects messages received by TCP from clients to servers
    over IPC. Consists of TCP-frontend and IPC-backend objects. Runs
    in async executor.
    """

    def __init__(self, conf, context):
        super(BaseProxy, self).__init__()
        self.conf = conf
        self.context = context
        self.executor = zmq_async.get_executor(self.run,
                                               zmq_concurrency='native')

    @abc.abstractmethod
    def run(self):
        """Main execution point of the proxy"""

    def start(self):
        self.executor.execute()

    def stop(self):
        self.executor.stop()

    def wait(self):
        self.executor.wait()
