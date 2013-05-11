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

import eventlet
import greenlet

from openstack.common.messaging._executors import base


class EventletExecutor(base.ExecutorBase):

    def __init__(self, conf, listener, callback):
        super(EventletExecutor, self).__init__(conf, listener, callback)
        self._thread = None

    def start(self):
        if self._thread is not None:
            return

        def _executor_thread():
            try:
                while True:
            except greenlet.GreenletExit:
                return

        self._thread = eventlet.spawn(_executor_thread)

    def stop(self):
        if self._thread is None:
            return
        self._thread.kill()

    def wait(self):
        if self._thread is None:
            return
        try:
            self._thread.wait()
        except greenlet.GreenletExit:
            pass
        self._thread = None
