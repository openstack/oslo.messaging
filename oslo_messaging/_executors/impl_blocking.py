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

import futurist
import threading

from oslo_messaging._executors import impl_pooledexecutor
from oslo_utils import timeutils


class FakeBlockingThread(object):
    '''A minimal implementation of threading.Thread which does not create a
    thread or start executing the target when start() is called. Instead, the
    caller must explicitly execute the non-blocking thread.execute() method
    after start() has been called.
    '''

    def __init__(self, target):
        self._target = target
        self._running = False
        self._running_cond = threading.Condition()

    def start(self):
        if self._running:
            # Not a user error. No need to translate.
            raise RuntimeError('FakeBlockingThread already started')

        with self._running_cond:
            self._running = True
            self._running_cond.notify_all()

    def join(self, timeout=None):
        with timeutils.StopWatch(duration=timeout) as w, self._running_cond:
            while self._running:
                self._running_cond.wait(w.leftover(return_none=True))

                # Thread.join() does not raise an exception on timeout. It is
                # the caller's responsibility to check is_alive().
                if w.expired():
                    return

    def is_alive(self):
        return self._running

    def execute(self):
        if not self._running:
            # Not a user error. No need to translate.
            raise RuntimeError('FakeBlockingThread not started')

        try:
            self._target()
        finally:
            with self._running_cond:
                self._running = False
                self._running_cond.notify_all()


class BlockingExecutor(impl_pooledexecutor.PooledExecutor):
    """A message executor which blocks the current thread.

    The blocking executor's start() method functions as a request processing
    loop - i.e. it blocks, processes messages and only returns when stop() is
    called from a dispatched method.

    Method calls are dispatched in the current thread, so only a single method
    call can be executing at once. This executor is likely to only be useful
    for simple demo programs.
    """

    _executor_cls = lambda __, ___: futurist.SynchronousExecutor()
    _thread_cls = FakeBlockingThread

    def __init__(self, *args, **kwargs):
        super(BlockingExecutor, self).__init__(*args, **kwargs)

    def execute(self):
        '''Explicitly run the executor in the current context.'''
        # NOTE(mdbooth): Splitting start into start and execute for the
        # blocking executor closes a potential race. On a non-blocking
        # executor, calling start performs some initialisation synchronously
        # before starting the executor and returning control to the caller. In
        # the non-blocking caller there was no externally visible boundary
        # between the completion of initialisation and the start of execution,
        # meaning the caller cannot indicate to another thread that
        # initialisation is complete. With the split, the start call for the
        # blocking executor becomes analogous to the non-blocking case,
        # indicating that initialisation is complete. The caller can then
        # synchronously call execute.
        if self._poller is not None:
            self._poller.execute()
