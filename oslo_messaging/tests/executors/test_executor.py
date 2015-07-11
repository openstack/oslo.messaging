# Copyright 2011 OpenStack Foundation.
# All Rights Reserved.
# Copyright 2013 eNovance
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

import threading

# eventlet 0.16 with monkey patching does not work yet on Python 3,
# so make aioeventlet, eventlet and trollius import optional
try:
    import aioeventlet
except ImportError:
    aioeventlet = None
try:
    import eventlet
except ImportError:
    eventlet = None
import testscenarios
try:
    import trollius
except ImportError:
    pass


try:
    from oslo_messaging._executors import impl_aioeventlet
except ImportError:
    impl_aioeventlet = None
from oslo_messaging._executors import impl_blocking
try:
    from oslo_messaging._executors import impl_eventlet
except ImportError:
    impl_eventlet = None
from oslo_messaging._executors import impl_thread
from oslo_messaging import _utils as utils
from oslo_messaging.tests import utils as test_utils
from six.moves import mock

load_tests = testscenarios.load_tests_apply_scenarios


class TestExecutor(test_utils.BaseTestCase):
    @classmethod
    def generate_scenarios(cls):
        impl = [
            ('blocking', dict(executor=impl_blocking.BlockingExecutor)),
            ('threaded', dict(executor=impl_thread.ThreadExecutor)),
        ]
        if impl_eventlet is not None:
            impl.append(
                ('eventlet', dict(executor=impl_eventlet.EventletExecutor)))
        if impl_aioeventlet is not None:
            impl.append(
                ('aioeventlet',
                 dict(executor=impl_aioeventlet.AsyncioEventletExecutor)))
        cls.scenarios = testscenarios.multiply_scenarios(impl)

    @staticmethod
    def _run_in_thread(target, executor):
        thread = threading.Thread(target=target, args=(executor,))
        thread.daemon = True
        thread.start()
        thread.join(timeout=30)

    def test_executor_dispatch(self):
        if impl_aioeventlet is not None:
            aioeventlet_class = impl_aioeventlet.AsyncioEventletExecutor
        else:
            aioeventlet_class = None
        is_aioeventlet = (self.executor == aioeventlet_class)

        if is_aioeventlet:
            policy = aioeventlet.EventLoopPolicy()
            trollius.set_event_loop_policy(policy)
            self.addCleanup(trollius.set_event_loop_policy, None)

            def run_loop(loop):
                loop.run_forever()
                loop.close()
                trollius.set_event_loop(None)

            def run_executor(executor):
                # create an event loop in the executor thread
                loop = trollius.new_event_loop()
                trollius.set_event_loop(loop)
                eventlet.spawn(run_loop, loop)

                # run the executor
                executor.start()
                executor.wait()

                # stop the event loop: run_loop() will close it
                loop.stop()

            @trollius.coroutine
            def simple_coroutine(value):
                raise trollius.Return(value)

            endpoint = mock.MagicMock(return_value=simple_coroutine('result'))
            event = eventlet.event.Event()
        else:
            def run_executor(executor):
                executor.start()
                executor.wait()

            endpoint = mock.MagicMock(return_value='result')

        class Dispatcher(object):
            def __init__(self, endpoint):
                self.endpoint = endpoint
                self.result = "not set"

            def callback(self, incoming, executor_callback):
                if executor_callback is None:
                    result = self.endpoint(incoming.ctxt,
                                           incoming.message)
                else:
                    result = executor_callback(self.endpoint,
                                               incoming.ctxt,
                                               incoming.message)
                if is_aioeventlet:
                    event.send()
                self.result = result
                return result

            def __call__(self, incoming, executor_callback=None):
                return utils.DispatcherExecutorContext(incoming,
                                                       self.callback,
                                                       executor_callback)

        listener = mock.Mock(spec=['poll', 'stop'])
        dispatcher = Dispatcher(endpoint)
        executor = self.executor(self.conf, listener, dispatcher)

        incoming_message = mock.MagicMock(ctxt={}, message={'payload': 'data'})

        def fake_poll(timeout=None):
            if is_aioeventlet:
                if listener.poll.call_count == 1:
                    return incoming_message
                event.wait()
                executor.stop()
            else:
                if listener.poll.call_count == 1:
                    return incoming_message
                executor.stop()

        listener.poll.side_effect = fake_poll

        self._run_in_thread(run_executor, executor)

        endpoint.assert_called_once_with({}, {'payload': 'data'})
        self.assertEqual(dispatcher.result, 'result')

TestExecutor.generate_scenarios()
