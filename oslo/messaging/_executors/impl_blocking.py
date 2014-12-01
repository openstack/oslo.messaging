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

import logging

from oslo.messaging._executors import base
from oslo.messaging._i18n import _

LOG = logging.getLogger(__name__)


class BlockingExecutor(base.ExecutorBase):

    """A message executor which blocks the current thread.

    The blocking executor's start() method functions as a request processing
    loop - i.e. it blocks, processes messages and only returns when stop() is
    called from a dispatched method.

    Method calls are dispatched in the current thread, so only a single method
    call can be executing at once. This executor is likely to only be useful
    for simple demo programs.
    """

    def __init__(self, conf, listener, dispatcher):
        super(BlockingExecutor, self).__init__(conf, listener, dispatcher)
        self._running = False

    def start(self):
        self._running = True
        while self._running:
            try:
                incoming = self.listener.poll(timeout=base.POLL_TIMEOUT)
                if incoming is not None:
                    with self.dispatcher(incoming) as callback:
                        callback()
            except Exception:
                LOG.exception(_("Unexpected exception occurred."))

    def stop(self):
        self._running = False

    def wait(self):
        pass
