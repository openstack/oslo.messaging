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
import logging
import sys

import six

from oslo import messaging

_LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class ExecutorBase(object):

    def __init__(self, conf, listener, callback):
        self.conf = conf
        self.listener = listener
        self.callback = callback

    def _dispatch(self, incoming):
        try:
            incoming.reply(self.callback(incoming.ctxt, incoming.message))
        except messaging.ExpectedException as e:
            _LOG.debug('Expected exception during message handling (%s)' %
                       e.exc_info[1])
            incoming.reply(failure=e.exc_info, log_failure=False)
        except Exception:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            _LOG.error('Exception during message handling',
                       exc_info=exc_info)
            incoming.reply(failure=exc_info)

    @abc.abstractmethod
    def start(self):
        "Start polling for incoming messages."

    @abc.abstractmethod
    def stop(self):
        "Stop polling for messages."

    @abc.abstractmethod
    def wait(self):
        "Wait until the executor has stopped polling."
