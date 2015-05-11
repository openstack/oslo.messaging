# Copyright 2014 Canonical, Ltd.
# All Rights Reserved.
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

import logging
import socket

import fixtures
import testtools

from six.moves import mock

try:
    import zmq
except ImportError:
    zmq = None

from oslo import messaging
from oslo.utils import importutils
from oslo_messaging.tests import utils as test_utils

# eventlet is not yet py3 compatible, so skip if not installed
eventlet = importutils.try_import('eventlet')

impl_zmq = importutils.try_import('oslo_messaging._drivers.impl_zmq')

LOG = logging.getLogger(__name__)


def get_unused_port():
    """Returns an unused port on localhost."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ZmqBaseTestCase(test_utils.BaseTestCase):
    """Base test case for all ZMQ tests that make use of the ZMQ Proxy"""

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(ZmqBaseTestCase, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'
        # Get driver
        transport = messaging.get_transport(self.conf)
        self.driver = transport._driver

        # Set config values
        self.internal_ipc_dir = self.useFixture(fixtures.TempDir()).path
        kwargs = {'rpc_zmq_bind_address': '127.0.0.1',
                  'rpc_zmq_host': '127.0.0.1',
                  'rpc_response_timeout': 5,
                  'rpc_zmq_port': get_unused_port(),
                  'rpc_zmq_ipc_dir': self.internal_ipc_dir}
        self.config(**kwargs)

        # Start RPC
        LOG.info("Running internal zmq receiver.")
        self.reactor = impl_zmq.ZmqProxy(self.conf)
        self.reactor.consume_in_thread()

        self.matchmaker = impl_zmq._get_matchmaker(host='127.0.0.1')
        self.addCleanup(stopRpc(self.__dict__))


class TestConfZmqDriverLoad(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestConfZmqDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'

    def test_driver_load(self):
        transport = messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, impl_zmq.ZmqDriver)


class stopRpc(object):
    def __init__(self, attrs):
        self.attrs = attrs

    def __call__(self):
        if self.attrs['reactor']:
            self.attrs['reactor'].close()
        if self.attrs['driver']:
            self.attrs['driver'].cleanup()


class TestZmqBasics(ZmqBaseTestCase):

    def test_start_stop_listener(self):
        target = messaging.Target(topic='testtopic')
        listener = self.driver.listen(target)
        result = listener.poll(0.01)
        self.assertEqual(result, None)

    def test_send_receive_raises(self):
        """Call() without method."""
        target = messaging.Target(topic='testtopic')
        self.driver.listen(target)
        self.assertRaises(
            KeyError,
            self.driver.send,
            target, {}, {'tx_id': 1}, wait_for_reply=True)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqIncomingMessage')
    def test_send_receive_topic(self, mock_msg):
        """Call() with method."""
        mock_msg.return_value = msg = mock.MagicMock()
        msg.received = received = mock.MagicMock()
        received.failure = False
        received.reply = True
        msg.condition = condition = mock.MagicMock()
        condition.wait.return_value = True

        target = messaging.Target(topic='testtopic')
        self.driver.listen(target)
        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=True)
        self.assertEqual(result, True)

    @mock.patch('oslo_messaging._drivers.impl_zmq._call', autospec=True)
    def test_send_receive_fanout(self, mock_call):
        target = messaging.Target(topic='testtopic', fanout=True)
        self.driver.listen(target)

        mock_call.__name__ = '_call'
        mock_call.return_value = [True]

        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=True)

        self.assertEqual(result, True)
        mock_call.assert_called_once_with(
            self.driver,
            'tcp://127.0.0.1:%s' % self.conf['rpc_zmq_port'],
            {}, 'fanout~testtopic.127.0.0.1',
            {'tx_id': 1, 'method': 'hello-world'},
            None, False, [], True)

    @mock.patch('oslo_messaging._drivers.impl_zmq._call', autospec=True)
    def test_send_receive_direct(self, mock_call):
        # Also verifies fix for bug http://pad.lv/1301723
        target = messaging.Target(topic='testtopic', server='localhost')
        self.driver.listen(target)

        mock_call.__name__ = '_call'
        mock_call.return_value = [True]

        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=True)

        self.assertEqual(result, True)
        mock_call.assert_called_once_with(
            self.driver,
            'tcp://localhost:%s' % self.conf['rpc_zmq_port'],
            {}, 'testtopic.localhost',
            {'tx_id': 1, 'method': 'hello-world'},
            None, False, [], True)


class TestZmqSocket(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqSocket, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'
        # Get driver
        transport = messaging.get_transport(self.conf)
        self.driver = transport._driver

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqSocket.subscribe')
    @mock.patch('oslo_messaging._drivers.impl_zmq.zmq.Context')
    def test_zmqsocket_init_type_pull(self, mock_context, mock_subscribe):
        mock_ctxt = mock.Mock()
        mock_context.return_value = mock_ctxt
        mock_sock = mock.Mock()
        mock_ctxt.socket = mock.Mock(return_value=mock_sock)
        mock_sock.connect = mock.Mock()
        mock_sock.bind = mock.Mock()
        addr = '127.0.0.1'

        sock = impl_zmq.ZmqSocket(addr, impl_zmq.zmq.PULL, bind=False,
                                  subscribe=None)
        self.assertTrue(sock.can_recv)
        self.assertFalse(sock.can_send)
        self.assertFalse(sock.can_sub)
        self.assertTrue(mock_sock.connect.called)
        self.assertFalse(mock_sock.bind.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqSocket.subscribe')
    @mock.patch('oslo_messaging._drivers.impl_zmq.zmq.Context')
    def test_zmqsocket_init_type_sub(self, mock_context, mock_subscribe):
        mock_ctxt = mock.Mock()
        mock_context.return_value = mock_ctxt
        mock_sock = mock.Mock()
        mock_ctxt.socket = mock.Mock(return_value=mock_sock)
        mock_sock.connect = mock.Mock()
        mock_sock.bind = mock.Mock()
        addr = '127.0.0.1'

        sock = impl_zmq.ZmqSocket(addr, impl_zmq.zmq.SUB, bind=False,
                                  subscribe=None)
        self.assertTrue(sock.can_recv)
        self.assertFalse(sock.can_send)
        self.assertTrue(sock.can_sub)
        self.assertTrue(mock_sock.connect.called)
        self.assertFalse(mock_sock.bind.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqSocket.subscribe')
    @mock.patch('oslo_messaging._drivers.impl_zmq.zmq.Context')
    def test_zmqsocket_init_type_push(self, mock_context, mock_subscribe):
        mock_ctxt = mock.Mock()
        mock_context.return_value = mock_ctxt
        mock_sock = mock.Mock()
        mock_ctxt.socket = mock.Mock(return_value=mock_sock)
        mock_sock.connect = mock.Mock()
        mock_sock.bind = mock.Mock()
        addr = '127.0.0.1'

        sock = impl_zmq.ZmqSocket(addr, impl_zmq.zmq.PUSH, bind=False,
                                  subscribe=None)
        self.assertFalse(sock.can_recv)
        self.assertTrue(sock.can_send)
        self.assertFalse(sock.can_sub)
        self.assertTrue(mock_sock.connect.called)
        self.assertFalse(mock_sock.bind.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqSocket.subscribe')
    @mock.patch('oslo_messaging._drivers.impl_zmq.zmq.Context')
    def test_zmqsocket_init_type_pub(self, mock_context, mock_subscribe):
        mock_ctxt = mock.Mock()
        mock_context.return_value = mock_ctxt
        mock_sock = mock.Mock()
        mock_ctxt.socket = mock.Mock(return_value=mock_sock)
        mock_sock.connect = mock.Mock()
        mock_sock.bind = mock.Mock()
        addr = '127.0.0.1'

        sock = impl_zmq.ZmqSocket(addr, impl_zmq.zmq.PUB, bind=False,
                                  subscribe=None)
        self.assertFalse(sock.can_recv)
        self.assertTrue(sock.can_send)
        self.assertFalse(sock.can_sub)
        self.assertTrue(mock_sock.connect.called)
        self.assertFalse(mock_sock.bind.called)


class TestZmqIncomingMessage(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestZmqIncomingMessage, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'
        # Get driver
        transport = messaging.get_transport(self.conf)
        self.driver = transport._driver

    def test_zmqincomingmessage(self):
        msg = impl_zmq.ZmqIncomingMessage(mock.Mock(), None, 'msg.foo')
        msg.reply("abc")
        self.assertIsInstance(
            msg.received, impl_zmq.ZmqIncomingMessage.ReceivedReply)
        self.assertIsInstance(
            msg.received, impl_zmq.ZmqIncomingMessage.ReceivedReply)
        self.assertEqual(msg.received.reply, "abc")
        msg.requeue()


class TestZmqConnection(ZmqBaseTestCase):

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqReactor', autospec=True)
    def test_zmqconnection_create_consumer(self, mock_reactor):

        mock_reactor.register = mock.Mock()
        conn = impl_zmq.Connection(self.driver.conf, self.driver)
        topic = 'topic.foo'
        context = mock.Mock()
        inaddr = ('ipc://%s/zmq_topic_topic.127.0.0.1' %
                  (self.internal_ipc_dir))
        # No Fanout
        conn.create_consumer(topic, context)
        conn.reactor.register.assert_called_with(context, inaddr,
                                                 impl_zmq.zmq.PULL,
                                                 subscribe=None, in_bind=False)

        # Reset for next bunch of checks
        conn.reactor.register.reset_mock()

        # Fanout
        inaddr = ('ipc://%s/zmq_topic_fanout~topic' %
                  (self.internal_ipc_dir))
        conn.create_consumer(topic, context, fanout='subscriber.foo')
        conn.reactor.register.assert_called_with(context, inaddr,
                                                 impl_zmq.zmq.SUB,
                                                 subscribe='subscriber.foo',
                                                 in_bind=False)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqReactor', autospec=True)
    def test_zmqconnection_create_consumer_topic_exists(self, mock_reactor):
        mock_reactor.register = mock.Mock()
        conn = impl_zmq.Connection(self.driver.conf, self.driver)
        topic = 'topic.foo'
        context = mock.Mock()
        inaddr = ('ipc://%s/zmq_topic_topic.127.0.0.1' %
                  (self.internal_ipc_dir))

        conn.create_consumer(topic, context)
        conn.reactor.register.assert_called_with(
            context, inaddr, impl_zmq.zmq.PULL, subscribe=None, in_bind=False)
        conn.reactor.register.reset_mock()
        # Call again with same topic
        conn.create_consumer(topic, context)
        self.assertFalse(conn.reactor.register.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq._get_matchmaker',
                autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqReactor', autospec=True)
    def test_zmqconnection_close(self, mock_reactor, mock_getmatchmaker):
        conn = impl_zmq.Connection(self.driver.conf, self.driver)
        conn.reactor.close = mock.Mock()
        mock_getmatchmaker.return_value.stop_heartbeat = mock.Mock()
        conn.close()
        self.assertTrue(mock_getmatchmaker.return_value.stop_heartbeat.called)
        self.assertTrue(conn.reactor.close.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqReactor', autospec=True)
    def test_zmqconnection_wait(self, mock_reactor):
        conn = impl_zmq.Connection(self.driver.conf, self.driver)
        conn.reactor.wait = mock.Mock()
        conn.wait()
        self.assertTrue(conn.reactor.wait.called)

    @mock.patch('oslo_messaging._drivers.impl_zmq._get_matchmaker',
                autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqReactor', autospec=True)
    def test_zmqconnection_consume_in_thread(self, mock_reactor,
                                             mock_getmatchmaker):
        mock_getmatchmaker.return_value.start_heartbeat = mock.Mock()
        conn = impl_zmq.Connection(self.driver.conf, self.driver)
        conn.reactor.consume_in_thread = mock.Mock()
        conn.consume_in_thread()
        self.assertTrue(mock_getmatchmaker.return_value.start_heartbeat.called)
        self.assertTrue(conn.reactor.consume_in_thread.called)


class TestZmqListener(ZmqBaseTestCase):

    def test_zmqlistener_no_msg(self):
        listener = impl_zmq.ZmqListener(self.driver)
        # Timeout = 0 should return straight away since the queue is empty
        listener.poll(timeout=0)

    def test_zmqlistener_w_msg(self):
        listener = impl_zmq.ZmqListener(self.driver)
        kwargs = {'a': 1, 'b': 2}
        m = mock.Mock()
        ctxt = mock.Mock(autospec=impl_zmq.RpcContext)
        message = {'namespace': 'name.space', 'method': m.fake_method,
                   'args': kwargs}
        eventlet.spawn_n(listener.dispatch, ctxt, message)
        resp = listener.poll(timeout=10)
        msg = {'method': m.fake_method, 'namespace': 'name.space',
               'args': kwargs}
        self.assertEqual(resp.message, msg)


class TestZmqDriver(ZmqBaseTestCase):

    @mock.patch('oslo_messaging._drivers.impl_zmq._cast', autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq._multi_send', autospec=True)
    def test_zmqdriver_send(self, mock_multi_send, mock_cast):
        context = mock.Mock(autospec=impl_zmq.RpcContext)
        topic = 'testtopic'
        msg = 'jeronimo'
        self.driver.send(messaging.Target(topic=topic), context, msg,
                         False, 0, False)
        mock_multi_send.assert_called_with(self.driver, mock_cast, context,
                                           topic, msg,
                                           allowed_remote_exmods=[],
                                           envelope=False, pooled=True)

    @mock.patch('oslo_messaging._drivers.impl_zmq._cast', autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq._multi_send', autospec=True)
    def test_zmqdriver_send_notification(self, mock_multi_send, mock_cast):
        context = mock.Mock(autospec=impl_zmq.RpcContext)
        topic = 'testtopic.foo'
        topic_reformat = 'testtopic-foo'
        msg = 'jeronimo'
        self.driver.send_notification(messaging.Target(topic=topic), context,
                                      msg, False, False)
        mock_multi_send.assert_called_with(self.driver, mock_cast, context,
                                           topic_reformat, msg,
                                           allowed_remote_exmods=[],
                                           envelope=False, pooled=True)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqListener', autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq.Connection', autospec=True)
    def test_zmqdriver_listen(self, mock_connection, mock_listener):
        mock_listener.return_value = listener = mock.Mock()
        mock_connection.return_value = conn = mock.Mock()
        conn.create_consumer = mock.Mock()
        conn.consume_in_thread = mock.Mock()
        topic = 'testtopic.foo'
        self.driver.listen(messaging.Target(topic=topic))
        conn.create_consumer.assert_called_with(topic, listener, fanout=True)

    @mock.patch('oslo_messaging._drivers.impl_zmq.ZmqListener', autospec=True)
    @mock.patch('oslo_messaging._drivers.impl_zmq.Connection', autospec=True)
    def test_zmqdriver_listen_for_notification(self, mock_connection,
                                               mock_listener):
        mock_listener.return_value = listener = mock.Mock()
        mock_connection.return_value = conn = mock.Mock()
        conn.create_consumer = mock.Mock()
        conn.consume_in_thread = mock.Mock()
        topic = 'testtopic.foo'
        data = [(messaging.Target(topic=topic), 0)]
        # NOTE(jamespage): Pooling not supported, just pass None for now.
        self.driver.listen_for_notifications(data, None)
        conn.create_consumer.assert_called_with("%s-%s" % (topic, 0), listener)
