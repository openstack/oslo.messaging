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
import logging

import six

from oslo_messaging._drivers.zmq_driver import zmq_async


LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class CastPublisherBase(object):

    def __init__(self, conf):
        self.conf = conf
        self.zmq_context = zmq.Context()
        self.outbound_sockets = {}
        super(CastPublisherBase, self).__init__()

    @abc.abstractmethod
    def cast(self, target, context,
             message, timeout=None, retry=None):
        "Send CAST to target"
