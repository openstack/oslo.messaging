#    Copyright 2015 Mirantis, Inc.
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

import testtools

import oslo_messaging
from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging.tests.drivers.zmq import zmq_common
from oslo_messaging.tests import utils as test_utils


zmq = zmq_async.import_zmq()


class ZmqTestPortsRange(zmq_common.ZmqBaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(ZmqTestPortsRange, self).setUp()

        # Set config values
        kwargs = {'rpc_zmq_min_port': 5555,
                  'rpc_zmq_max_port': 5560}
        self.config(group='oslo_messaging_zmq', **kwargs)

    def test_ports_range(self):
        listeners = []

        for i in range(10):
            try:
                target = oslo_messaging.Target(topic='testtopic_' + str(i))
                new_listener = self.driver.listen(target, None, None)
                listeners.append(new_listener)
            except zmq_socket.ZmqPortBusy:
                pass

        self.assertLessEqual(len(listeners), 5)

        for l in listeners:
            l.cleanup()


class TestConfZmqDriverLoad(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestConfZmqDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, impl_zmq.ZmqDriver)


class TestZmqBasics(zmq_common.ZmqBaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqBasics, self).setUp()
        self.target = oslo_messaging.Target(topic='topic')
        self.ctxt = {'key': 'value'}
        self.message = {'method': 'qwerty', 'args': {'int': 1, 'bool': True}}

    def test_send_call_without_method_failure(self):
        self.message.pop('method')
        self.listener.listen(self.target)
        self.assertRaises(KeyError, self.driver.send,
                          self.target, self.ctxt, self.message,
                          wait_for_reply=True, timeout=10)

    def _check_listener_received(self):
        self.assertTrue(self.listener._received.isSet())
        self.assertEqual(self.ctxt, self.listener.message.ctxt)
        self.assertEqual(self.message, self.listener.message.message)

    def test_send_call_success(self):
        self.listener.listen(self.target)
        result = self.driver.send(self.target, self.ctxt, self.message,
                                  wait_for_reply=True, timeout=10)
        self.assertTrue(result)
        self._check_listener_received()

    def test_send_call_direct_success(self):
        self.target.server = 'server'
        self.listener.listen(self.target)
        result = self.driver.send(self.target, self.ctxt, self.message,
                                  wait_for_reply=True, timeout=10)
        self.assertTrue(result)
        self._check_listener_received()

    def test_send_cast_direct_success(self):
        self.target.server = 'server'
        self.listener.listen(self.target)
        result = self.driver.send(self.target, self.ctxt, self.message,
                                  wait_for_reply=False)
        self.listener._received.wait(5)
        self.assertIsNone(result)
        self._check_listener_received()

    def test_send_fanout_success(self):
        self.target.fanout = True
        self.listener.listen(self.target)
        result = self.driver.send(self.target, self.ctxt, self.message,
                                  wait_for_reply=False)
        self.listener._received.wait(5)
        self.assertIsNone(result)
        self._check_listener_received()

    def test_send_notify_success(self):
        self.listener.listen_notifications([(self.target, 'info')])
        self.target.topic += '.info'
        result = self.driver.send_notification(self.target, self.ctxt,
                                               self.message, '3.0')
        self.listener._received.wait(5)
        self.assertIsNone(result)
        self._check_listener_received()
