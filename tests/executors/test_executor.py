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

import contextlib
import eventlet
import threading

import mock
import testscenarios

from oslo.messaging._executors import impl_blocking
from oslo.messaging._executors import impl_eventlet
from tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class TestExecutor(test_utils.BaseTestCase):

    _impl = [('blocking', dict(executor=impl_blocking.BlockingExecutor,
                               stop_before_return=True)),
             ('eventlet', dict(executor=impl_eventlet.EventletExecutor,
                               stop_before_return=False))]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._impl)

    @staticmethod
    def _run_in_thread(executor):
        def thread():
            executor.start()
            executor.wait()
        thread = threading.Thread(target=thread)
        thread.daemon = True
        thread.start()
        thread.join(timeout=30)

    def test_executor_dispatch(self):
        callback = mock.MagicMock(return_value='result')

        class Dispatcher(object):
            @contextlib.contextmanager
            def __call__(self, incoming):
                yield lambda: callback(incoming.ctxt, incoming.message)

        listener = mock.Mock(spec=['poll'])
        executor = self.executor(self.conf, listener, Dispatcher())

        incoming_message = mock.MagicMock(ctxt={},
                                          message={'payload': 'data'})

        def fake_poll():
            if self.stop_before_return:
                executor.stop()
                return incoming_message
            else:
                if listener.poll.call_count == 1:
                    return incoming_message
                executor.stop()

        listener.poll.side_effect = fake_poll

        self._run_in_thread(executor)

        callback.assert_called_once_with({}, {'payload': 'data'})

TestExecutor.generate_scenarios()


class ExceptedException(Exception):
    pass


class EventletContextManagerSpawnTest(test_utils.BaseTestCase):
    def setUp(self):
        super(EventletContextManagerSpawnTest, self).setUp()
        self.before = mock.Mock()
        self.callback = mock.Mock()
        self.after = mock.Mock()
        self.exception_call = mock.Mock()

        @contextlib.contextmanager
        def context_mgr():
            self.before()
            try:
                yield lambda: self.callback()
            except ExceptedException:
                self.exception_call()
            self.after()

        self.mgr = context_mgr()

    def test_normal_run(self):
        thread = impl_eventlet.spawn_with(self.mgr, pool=eventlet)
        thread.wait()
        self.assertEqual(1, self.before.call_count)
        self.assertEqual(1, self.callback.call_count)
        self.assertEqual(1, self.after.call_count)
        self.assertEqual(0, self.exception_call.call_count)

    def test_excepted_exception(self):
        self.callback.side_effect = ExceptedException
        thread = impl_eventlet.spawn_with(self.mgr, pool=eventlet)
        try:
            thread.wait()
        except ExceptedException:
            pass
        self.assertEqual(1, self.before.call_count)
        self.assertEqual(1, self.callback.call_count)
        self.assertEqual(1, self.after.call_count)
        self.assertEqual(1, self.exception_call.call_count)

    def test_unexcepted_exception(self):
        self.callback.side_effect = Exception
        thread = impl_eventlet.spawn_with(self.mgr, pool=eventlet)
        try:
            thread.wait()
        except Exception:
            pass
        self.assertEqual(1, self.before.call_count)
        self.assertEqual(1, self.callback.call_count)
        self.assertEqual(0, self.after.call_count)
        self.assertEqual(0, self.exception_call.call_count)
