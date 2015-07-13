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
import threading

from concurrent import futures
from oslo_config import cfg
from oslo_utils import excutils

from oslo_messaging._executors import base

_pool_opts = [
    cfg.IntOpt('rpc_thread_pool_size',
               default=64,
               help='Size of RPC thread pool.'),
]


class PooledExecutor(base.ExecutorBase):
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
    _event_cls = threading.Event
    _lock_cls = threading.Lock
    _thread_cls = threading.Thread

    def __init__(self, conf, listener, dispatcher):
        super(PooledExecutor, self).__init__(conf, listener, dispatcher)
        self.conf.register_opts(_pool_opts)
        self._poller = None
        self._executor = None
        self._tombstone = self._event_cls()
        self._incomplete = collections.deque()
        self._mutator = self._lock_cls()

    @excutils.forever_retry_uncaught_exceptions
    def _runner(self):
        while not self._tombstone.is_set():
            incoming = self.listener.poll()
            if incoming is None:
                continue
            callback = self.dispatcher(incoming, self._executor_callback)
            try:
                fut = self._executor.submit(callback.run)
            except RuntimeError:
                # This is triggered when the executor has been shutdown...
                #
                # TODO(harlowja): should we put whatever we pulled off back
                # since when this is thrown it means the executor has been
                # shutdown already??
                callback.done()
                return
            else:
                with self._mutator:
                    self._incomplete.append(fut)
                # Run the other post processing of the callback when done...
                fut.add_done_callback(lambda f: callback.done())

    def start(self):
        if self._executor is None:
            self._executor = self._executor_cls(self.conf.rpc_thread_pool_size)
        self._tombstone.clear()
        if self._poller is None or not self._poller.is_alive():
            self._poller = self._thread_cls(target=self._runner)
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
