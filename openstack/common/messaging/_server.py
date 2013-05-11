# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
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

from openstack.common.gettextutils import _
from openstack.common import log as logging

_LOG = logging.getLogger(__name__)


class MessagingServerError(Exception):
    pass


class MessageHandlingServer(object):
    """Server for handling messages.

    Connect a transport to a dispatcher that knows how process the
    message using an executor that knows how the app wants to create
    new tasks.
    """

    def __init__(self, transport, target, dispatcher, executor_cls):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.dispatcher = dispatcher

        self._executor_cls = executor_cls
        self._executor = None

        super(MessageHandlingServer, self).__init__()

    def start(self):
        if self._executor is not None:
            return
        listener = self.transport._listen(self.target)
        self._executor = self._executor_cls(self.conf, listener,
                                            self.dispatcher)
        self._executor.start()

    def stop(self):
        if self._executor is not None:
            self._executor.stop()

    def wait(self):
        if self._executor is not None:
            self._executor.wait()
        self._executor = None
