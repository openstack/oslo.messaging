#    Copyright 2016 Mirantis, Inc.
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

import mock
import testtools

import oslo_messaging
from oslo_messaging._drivers.zmq_driver.client import zmq_receivers
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.proxy import zmq_proxy
from oslo_messaging._drivers.zmq_driver.proxy import zmq_queue_proxy
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_options
from oslo_messaging.tests.drivers.zmq import zmq_common
from oslo_messaging.tests import utils as test_utils

zmq = zmq_async.import_zmq()


class TestZmqAckManager(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqAckManager, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'
        zmq_options.register_opts(self.conf)

        # set config opts
        kwargs = {'rpc_zmq_matchmaker': 'dummy',
                  'use_pub_sub': False,
                  'use_router_proxy': True,
                  'rpc_thread_pool_size': 1,
                  'rpc_use_acks': True,
                  'rpc_ack_timeout_base': 3,
                  'rpc_ack_timeout_multiplier': 1,
                  'rpc_retry_attempts': 2}
        self.config(group='oslo_messaging_zmq', **kwargs)
        self.conf.register_opts(zmq_proxy.zmq_proxy_opts,
                                group='zmq_proxy_opts')

        # mock set_result method of futures
        self.set_result_patcher = mock.patch.object(
            zmq_receivers.futurist.Future, 'set_result',
            side_effect=zmq_receivers.futurist.Future.set_result, autospec=True
        )
        self.set_result = self.set_result_patcher.start()

        # mock send method of senders
        self.send_patcher = mock.patch.object(
            zmq_senders.RequestSenderProxy, 'send',
            side_effect=zmq_senders.RequestSenderProxy.send, autospec=True
        )
        self.send = self.send_patcher.start()

        # get driver
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

        # get ack manager
        self.ack_manager = self.driver.client.get().publishers['default']

        # prepare and launch proxy
        self.proxy = zmq_proxy.ZmqProxy(self.conf,
                                        zmq_queue_proxy.UniversalQueueProxy)
        vars(self.driver.matchmaker).update(vars(self.proxy.matchmaker))
        self.executor = zmq_async.get_executor(self.proxy.run)
        self.executor.execute()

        # create listener
        self.listener = zmq_common.TestServerListener(self.driver)

        # create target and message
        self.target = oslo_messaging.Target(topic='topic', server='server')
        self.message = {'method': 'xyz', 'args': {'x': 1, 'y': 2, 'z': 3}}

        self.addCleanup(
            zmq_common.StopRpc(
                self, [('listener', 'stop'), ('executor', 'stop'),
                       ('proxy', 'close'), ('driver', 'cleanup'),
                       ('send_patcher', 'stop'),
                       ('set_result_patcher', 'stop')]
            )
        )

    @mock.patch.object(
        zmq_incoming_message.ZmqIncomingMessage, 'acknowledge',
        side_effect=zmq_incoming_message.ZmqIncomingMessage.acknowledge,
        autospec=True
    )
    def test_cast_success_without_retries(self, received_ack_mock):
        self.listener.listen(self.target)
        result = self.driver.send(
            self.target, {}, self.message, wait_for_reply=False
        )
        self.ack_manager._pool.shutdown(wait=True)
        self.assertIsNone(result)
        self.assertTrue(self.listener._received.isSet())
        self.assertEqual(self.message, self.listener.message.message)
        self.assertEqual(1, self.send.call_count)
        self.assertEqual(1, received_ack_mock.call_count)
        self.assertEqual(2, self.set_result.call_count)

    def test_cast_success_with_one_retry(self):
        self.listener.listen(self.target)
        with mock.patch.object(zmq_incoming_message.ZmqIncomingMessage,
                               'acknowledge') as lost_ack_mock:
            result = self.driver.send(
                self.target, {}, self.message, wait_for_reply=False
            )
            self.listener._received.wait(3)
            self.assertIsNone(result)
            self.assertTrue(self.listener._received.isSet())
            self.assertEqual(self.message, self.listener.message.message)
            self.assertEqual(1, self.send.call_count)
            self.assertEqual(1, lost_ack_mock.call_count)
            self.assertEqual(0, self.set_result.call_count)
        with mock.patch.object(
            zmq_incoming_message.ZmqIncomingMessage, 'acknowledge',
            side_effect=zmq_incoming_message.ZmqIncomingMessage.acknowledge,
            autospec=True
        ) as received_ack_mock:
            self.listener._received.clear()
            self.ack_manager._pool.shutdown(wait=True)
            self.listener._received.wait(3)
            self.assertFalse(self.listener._received.isSet())
            self.assertEqual(2, self.send.call_count)
            self.assertEqual(1, received_ack_mock.call_count)
            self.assertEqual(2, self.set_result.call_count)

    def test_cast_success_with_two_retries(self):
        self.listener.listen(self.target)
        with mock.patch.object(zmq_incoming_message.ZmqIncomingMessage,
                               'acknowledge') as lost_ack_mock:
            result = self.driver.send(
                self.target, {}, self.message, wait_for_reply=False
            )
            self.listener._received.wait(3)
            self.assertIsNone(result)
            self.assertTrue(self.listener._received.isSet())
            self.assertEqual(self.message, self.listener.message.message)
            self.assertEqual(1, self.send.call_count)
            self.assertEqual(1, lost_ack_mock.call_count)
            self.assertEqual(0, self.set_result.call_count)
            self.listener._received.clear()
            self.listener._received.wait(4.5)
            self.assertFalse(self.listener._received.isSet())
            self.assertEqual(2, self.send.call_count)
            self.assertEqual(2, lost_ack_mock.call_count)
            self.assertEqual(0, self.set_result.call_count)
        with mock.patch.object(
            zmq_incoming_message.ZmqIncomingMessage, 'acknowledge',
            side_effect=zmq_incoming_message.ZmqIncomingMessage.acknowledge,
            autospec=True
        ) as received_ack_mock:
            self.ack_manager._pool.shutdown(wait=True)
            self.assertFalse(self.listener._received.isSet())
            self.assertEqual(3, self.send.call_count)
            self.assertEqual(1, received_ack_mock.call_count)
            self.assertEqual(2, self.set_result.call_count)

    @mock.patch.object(zmq_incoming_message.ZmqIncomingMessage, 'acknowledge')
    def test_cast_failure_exhausted_retries(self, lost_ack_mock):
        self.listener.listen(self.target)
        result = self.driver.send(
            self.target, {}, self.message, wait_for_reply=False
        )
        self.ack_manager._pool.shutdown(wait=True)
        self.assertIsNone(result)
        self.assertTrue(self.listener._received.isSet())
        self.assertEqual(self.message, self.listener.message.message)
        self.assertEqual(3, self.send.call_count)
        self.assertEqual(3, lost_ack_mock.call_count)
        self.assertEqual(1, self.set_result.call_count)
