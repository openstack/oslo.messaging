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
import logging
import time

import six

from oslo_messaging._drivers.zmq_driver import zmq_async

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class UpdaterBase(object):

    def __init__(self, conf, matchmaker, update_method):
        self.conf = conf
        self.matchmaker = matchmaker
        self.update_method = update_method
        self.executor = zmq_async.get_executor(method=self._update_loop)
        self.executor.execute()

    def _update_loop(self):
        self.update_method()
        time.sleep(self.conf.zmq_target_update)

    def cleanup(self):
        self.executor.stop()


@six.add_metaclass(abc.ABCMeta)
class ConnectionUpdater(UpdaterBase):

    def __init__(self, conf, matchmaker, socket):
        self.socket = socket
        super(ConnectionUpdater, self).__init__(
            conf, matchmaker, self._update_connection)

    @abc.abstractmethod
    def _update_connection(self):
        """Update connection info"""
