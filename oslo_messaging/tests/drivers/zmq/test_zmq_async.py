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

import mock
import testtools

from oslo_messaging._drivers.zmq_driver.poller import green_poller
from oslo_messaging._drivers.zmq_driver.poller import threading_poller
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests import utils as test_utils

zmq = zmq_async.import_zmq()


class TestImportZmq(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestImportZmq, self).setUp()

    def test_config_short_names_are_converted_to_correct_module_names(self):
        mock_try_import = mock.Mock()
        zmq_async.importutils.try_import = mock_try_import

        zmq_async.importutils.try_import.return_value = 'mock zmq module'
        self.assertEqual('mock zmq module', zmq_async.import_zmq('native'))
        mock_try_import.assert_called_with('zmq', default=None)

        zmq_async.importutils.try_import.return_value = 'mock eventlet module'
        self.assertEqual('mock eventlet module',
                         zmq_async.import_zmq('eventlet'))
        mock_try_import.assert_called_with('eventlet.green.zmq', default=None)

    def test_when_no_args_then_default_zmq_module_is_loaded(self):
        mock_try_import = mock.Mock()
        zmq_async.importutils.try_import = mock_try_import

        zmq_async.import_zmq()

        mock_try_import.assert_called_with('eventlet.green.zmq', default=None)

    def test_invalid_config_value_raise_ValueError(self):
        invalid_opt = 'x'

        errmsg = 'Invalid zmq_concurrency value: x'
        with self.assertRaisesRegexp(ValueError, errmsg):
            zmq_async.import_zmq(invalid_opt)


class TestGetPoller(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestGetPoller, self).setUp()

    def test_when_no_arg_to_get_poller_then_return_default_poller(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        actual = zmq_async.get_poller()

        self.assertTrue(isinstance(actual, green_poller.GreenPoller))

    def test_when_native_poller_requested_then_return_ThreadingPoller(self):
        actual = zmq_async.get_poller('native')

        self.assertTrue(isinstance(actual, threading_poller.ThreadingPoller))

    def test_when_eventlet_is_unavailable_then_return_ThreadingPoller(self):
        zmq_async._is_eventlet_zmq_available = lambda: False

        actual = zmq_async.get_poller('eventlet')

        self.assertTrue(isinstance(actual, threading_poller.ThreadingPoller))

    def test_when_eventlet_is_available_then_return_GreenPoller(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        actual = zmq_async.get_poller('eventlet')

        self.assertTrue(isinstance(actual, green_poller.GreenPoller))

    def test_invalid_config_value_raise_ValueError(self):
        invalid_opt = 'x'

        errmsg = 'Invalid zmq_concurrency value: x'
        with self.assertRaisesRegexp(ValueError, errmsg):
            zmq_async.get_poller(invalid_opt)


class TestGetReplyPoller(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestGetReplyPoller, self).setUp()

    def test_default_reply_poller_is_HoldReplyPoller(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        actual = zmq_async.get_poller()

        self.assertTrue(isinstance(actual, green_poller.GreenPoller))

    def test_when_eventlet_is_available_then_return_HoldReplyPoller(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        actual = zmq_async.get_poller('eventlet')

        self.assertTrue(isinstance(actual, green_poller.GreenPoller))

    def test_when_eventlet_is_unavailable_then_return_ThreadingPoller(self):
        zmq_async._is_eventlet_zmq_available = lambda: False

        actual = zmq_async.get_poller('eventlet')

        self.assertTrue(isinstance(actual, threading_poller.ThreadingPoller))

    def test_invalid_config_value_raise_ValueError(self):
        invalid_opt = 'x'

        errmsg = 'Invalid zmq_concurrency value: x'
        with self.assertRaisesRegexp(ValueError, errmsg):
            zmq_async.get_poller(invalid_opt)


class TestGetExecutor(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestGetExecutor, self).setUp()

    def test_default_executor_is_GreenExecutor(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        executor = zmq_async.get_executor('any method')

        self.assertTrue(isinstance(executor, green_poller.GreenExecutor))
        self.assertEqual('any method', executor._method)

    def test_when_eventlet_module_is_available_then_return_GreenExecutor(self):
        zmq_async._is_eventlet_zmq_available = lambda: True

        executor = zmq_async.get_executor('any method', 'eventlet')

        self.assertTrue(isinstance(executor, green_poller.GreenExecutor))
        self.assertEqual('any method', executor._method)

    def test_when_eventlet_is_unavailable_then_return_ThreadingExecutor(self):
        zmq_async._is_eventlet_zmq_available = lambda: False

        executor = zmq_async.get_executor('any method', 'eventlet')

        self.assertTrue(isinstance(executor,
                                   threading_poller.ThreadingExecutor))
        self.assertEqual('any method', executor._method)

    def test_invalid_config_value_raise_ValueError(self):
        invalid_opt = 'x'

        errmsg = 'Invalid zmq_concurrency value: x'
        with self.assertRaisesRegexp(ValueError, errmsg):
            zmq_async.get_executor('any method', invalid_opt)
