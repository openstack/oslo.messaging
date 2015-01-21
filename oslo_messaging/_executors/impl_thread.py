# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import collections
import functools
import sys
import threading

from concurrent import futures
from oslo_utils import excutils
import six

from oslo_messaging._executors import base


class ThreadExecutor(base.PooledExecutorBase):
    """A message executor which integrates with threads.

    A message process that polls for messages from a dispatching thread and
    on reception of an incoming message places the message to be processed in
    a thread pool to be executed at a later time.
    """

    # NOTE(harlowja): if eventlet is being used and the thread module is monkey
    # patched this should/is supposed to work the same as the eventlet based
    # executor.

    # NOTE(harlowja): Make it somewhat easy to change this via
    # inheritance (since there does exist other executor types that could be
    # used/tried here).
    _executor_cls = futures.ThreadPoolExecutor

    def __init__(self, conf, listener, dispatcher):
        super(ThreadExecutor, self).__init__(conf, listener, dispatcher)
        self._poller = None
        self._executor = None
        self._tombstone = threading.Event()
        self._incomplete = collections.deque()
        self._mutator = threading.Lock()

    def _completer(self, exit_method, fut):
        """Completes futures."""
        try:
            exc = fut.exception()
            if exc is not None:
                exc_type = type(exc)
                # Not available on < 3.x due to this being an added feature
                # of pep-3134 (exception chaining and embedded tracebacks).
                if six.PY3:
                    exc_tb = exc.__traceback__
                else:
                    exc_tb = None
                if not exit_method(exc_type, exc, exc_tb):
                    six.reraise(exc_type, exc, tb=exc_tb)
            else:
                exit_method(None, None, None)
        finally:
            with self._mutator:
                try:
                    self._incomplete.remove(fut)
                except ValueError:
                    pass

    @excutils.forever_retry_uncaught_exceptions
    def _runner(self):
        while not self._tombstone.is_set():
            incoming = self.listener.poll()
            if incoming is None:
                continue
            # This is hacky, needs to be fixed....
            context = self.dispatcher(incoming)
            enter_method = context.__enter__()
            exit_method = context.__exit__
            try:
                fut = self._executor.submit(enter_method)
            except RuntimeError:
                # This is triggered when the executor has been shutdown...
                #
                # TODO(harlowja): should we put whatever we pulled off back
                # since when this is thrown it means the executor has been
                # shutdown already??
                exit_method(*sys.exc_info())
                return
            else:
                with self._mutator:
                    self._incomplete.append(fut)
                # Run the other half (__exit__) when done...
                fut.add_done_callback(functools.partial(self._completer,
                                                        exit_method))

    def start(self):
        if self._executor is None:
            self._executor = self._executor_cls(self.conf.rpc_thread_pool_size)
        self._tombstone.clear()
        if self._poller is None or not self._poller.is_alive():
            self._poller = threading.Thread(target=self._runner)
            self._poller.daemon = True
            self._poller.start()

    def stop(self):
        if self._executor is not None:
            self._executor.shutdown(wait=False)
        self._tombstone.set()
        self.listener.stop()

    def wait(self):
        # TODO(harlowja): this method really needs a timeout.
        if self._poller is not None:
            self._tombstone.wait()
            self._poller.join()
            self._poller = None
        if self._executor is not None:
            with self._mutator:
                incomplete_fs = list(self._incomplete)
                self._incomplete.clear()
            if incomplete_fs:
                futures.wait(incomplete_fs, return_when=futures.ALL_COMPLETED)
            self._executor = None
