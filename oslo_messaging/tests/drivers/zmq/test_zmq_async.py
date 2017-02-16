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

from six.moves import mock
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

    def test_when_eventlet_is_available_then_load_eventlet_green_zmq(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: True

        mock_try_import = mock.Mock()
        zmq_async.importutils.try_import = mock_try_import

        zmq_async.import_zmq()

        mock_try_import.assert_called_with('eventlet.green.zmq', default=None)

    def test_when_evetlet_is_unavailable_then_load_zmq(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: False

        mock_try_import = mock.Mock()
        zmq_async.importutils.try_import = mock_try_import

        zmq_async.import_zmq()

        mock_try_import.assert_called_with('zmq', default=None)


class TestGetPoller(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestGetPoller, self).setUp()

    def test_when_eventlet_is_available_then_return_GreenPoller(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: True

        poller = zmq_async.get_poller()

        self.assertIsInstance(poller, green_poller.GreenPoller)

    def test_when_eventlet_is_unavailable_then_return_ThreadingPoller(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: False

        poller = zmq_async.get_poller()

        self.assertIsInstance(poller, threading_poller.ThreadingPoller)


class TestGetExecutor(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestGetExecutor, self).setUp()

    def test_when_eventlet_module_is_available_then_return_GreenExecutor(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: True

        executor = zmq_async.get_executor('any method')

        self.assertIsInstance(executor, green_poller.GreenExecutor)
        self.assertEqual('any method', executor._method)

    def test_when_eventlet_is_unavailable_then_return_ThreadingExecutor(self):
        zmq_async.eventletutils.is_monkey_patched = lambda _: False

        executor = zmq_async.get_executor('any method')

        self.assertIsInstance(executor,
                              threading_poller.ThreadingExecutor)
        self.assertEqual('any method', executor._method)
