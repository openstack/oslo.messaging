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

from oslo_config import cfg
import six

_pool_opts = [
    cfg.IntOpt('rpc_thread_pool_size',
               default=64,
               help='Size of RPC thread pool.'),
]


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


class PooledExecutorBase(ExecutorBase):
    """An executor that uses a rpc thread pool of a given size."""

    def __init__(self, conf, listener, callback):
        super(PooledExecutorBase, self).__init__(conf, listener, callback)
        self.conf.register_opts(_pool_opts)
