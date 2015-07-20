# Copyright 2014 eNovance.
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

import aioeventlet
import trollius

from oslo_messaging._executors import impl_eventlet


class AsyncioEventletExecutor(impl_eventlet.EventletExecutor):

    """A message executor which integrates with eventlet and trollius.

    The executor is based on eventlet executor and so is compatible with it.
    The executor supports trollius coroutines, explicit asynchronous
    programming, in addition to eventlet greenthreads, implicit asynchronous
    programming.

    To use the executor, an aioeventlet event loop must the running in the
    thread executing the executor (usually the main thread). Example of code to
    setup and run an aioeventlet event loop for the executor (in the main
    thread)::

        import aioeventlet
        import trollius

        policy = aioeventlet.EventLoopPolicy()
        trollius.set_event_loop_policy(policy)

        def run_loop(loop):
            loop.run_forever()
            loop.close()

        # Get the aioeventlet event loop (create it if needed)
        loop = trollius.get_event_loop()

        # run the event loop in a new greenthread,
        # close it when it is done
        eventlet.spawn(run_loop, loop)

    """

    def __init__(self, conf, listener, dispatcher):
        super(AsyncioEventletExecutor, self).__init__(conf, listener,
                                                      dispatcher)
        self._loop = None

    def start(self):
        # check that the event loop is an aioeventlet event loop
        loop = trollius.get_event_loop()
        if not isinstance(loop, aioeventlet.EventLoop):
            raise RuntimeError("need an aioeventlet event loop")
        self._loop = loop

        super(AsyncioEventletExecutor, self).start()

    def _coroutine_wrapper(self, func, *args, **kw):
        result = func(*args, **kw)
        if trollius.iscoroutine(result):
            result = aioeventlet.yield_future(result, loop=self._loop)
        return result

    _executor_callback = _coroutine_wrapper
