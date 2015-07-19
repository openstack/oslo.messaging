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

from eventlet.green import threading as greenthreading
import futurist

from oslo_messaging._executors import impl_pooledexecutor
from oslo_utils import eventletutils

LOG = logging.getLogger(__name__)


class EventletExecutor(impl_pooledexecutor.PooledExecutor):
    """A message executor which integrates with eventlet.

    This is an executor which polls for incoming messages from a greenthread
    and dispatches each message in its own greenthread powered async
    executor.

    The stop() method kills the message polling greenthread and the wait()
    method waits for all executor maintained greenthreads to complete.
    """

    def __init__(self, conf, listener, dispatcher):
        super(EventletExecutor, self).__init__(conf, listener, dispatcher)
        eventletutils.warn_eventlet_not_patched(
            expected_patched_modules=['thread'],
            what="the 'oslo.messaging eventlet executor'")

    _executor_cls = futurist.GreenThreadPoolExecutor
    _lock_cls = greenthreading.Lock
    _event_cls = greenthreading.Event
    _thread_cls = greenthreading.Thread
