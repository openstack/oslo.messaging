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

import logging
import os
import re
import socket
import threading

import fixtures
import testtools

import oslo_messaging
from oslo_messaging._drivers.common import RPCException
from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver.broker.zmq_broker import ZmqBroker
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._i18n import _
from oslo_messaging.tests import utils as test_utils

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class TestRPCServerListener(object):

    def __init__(self, driver):
        self.driver = driver
        self.target = None
        self.listener = None
        self.executor = zmq_async.get_executor(self._run)
        self._stop = threading.Event()
        self._received = threading.Event()
        self.message = None

    def listen(self, target):
        self.target = target
        self.listener = self.driver.listen(self.target)
        self.executor.execute()

    def _run(self):
        try:
            message = self.listener.poll()
            if message is not None:
                self._received.set()
                self.message = message
                message.reply(reply=True)
        except Exception:
            LOG.exception(_("Unexpected exception occurred."))

    def stop(self):
        self.executor.stop()


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
        transport = oslo_messaging.get_transport(self.conf)
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
        self.broker = ZmqBroker(self.conf)
        self.broker.start()

        self.listener = TestRPCServerListener(self.driver)

        self.addCleanup(stopRpc(self.__dict__))


class TestConfZmqDriverLoad(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestConfZmqDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, impl_zmq.ZmqDriver)


class stopRpc(object):
    def __init__(self, attrs):
        self.attrs = attrs

    def __call__(self):
        if self.attrs['broker']:
            self.attrs['broker'].close()
        if self.attrs['driver']:
            self.attrs['driver'].cleanup()
        if self.attrs['listener']:
            self.attrs['listener'].stop()


class TestZmqBasics(ZmqBaseTestCase):

    def test_send_receive_raises(self):
        """Call() without method."""
        target = oslo_messaging.Target(topic='testtopic')
        self.listener.listen(target)
        self.assertRaises(
            KeyError,
            self.driver.send,
            target, {}, {'tx_id': 1}, wait_for_reply=True)

    def test_send_receive_topic(self):
        """Call() with topic."""

        target = oslo_messaging.Target(topic='testtopic')
        self.listener.listen(target)
        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=True)
        self.assertIsNotNone(result)

    def test_send_noreply(self):
        """Cast() with topic."""

        target = oslo_messaging.Target(topic='testtopic', server="127.0.0.1")
        self.listener.listen(target)
        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=False)

        self.listener._received.wait()

        self.assertIsNone(result)
        self.assertEqual(True, self.listener._received.isSet())
        method = self.listener.message.message[u'method']
        self.assertEqual(u'hello-world', method)

    def test_send_fanout(self):
        target = oslo_messaging.Target(topic='testtopic', fanout=True)
        self.listener.listen(target)

        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=False)

        self.listener._received.wait()

        self.assertIsNone(result)
        self.assertEqual(True, self.listener._received.isSet())
        method = self.listener.message.message[u'method']
        self.assertEqual(u'hello-world', method)

    def test_send_receive_direct(self):
        """Call() without topic."""

        target = oslo_messaging.Target(server='127.0.0.1')
        self.listener.listen(target)
        message = {'method': 'hello-world', 'tx_id': 1}
        context = {}
        result = self.driver.send(target, context, message,
                                  wait_for_reply=True)
        self.assertTrue(result)


class TestPoller(test_utils.BaseTestCase):

    def setUp(self):
        super(TestPoller, self).setUp()
        self.poller = zmq_async.get_poller()
        self.ctx = zmq.Context()
        self.internal_ipc_dir = self.useFixture(fixtures.TempDir()).path
        self.ADDR_REQ = "ipc://%s/request1" % self.internal_ipc_dir

    def test_poll_blocking(self):

        rep = self.ctx.socket(zmq.REP)
        rep.bind(self.ADDR_REQ)

        reply_poller = zmq_async.get_reply_poller()
        reply_poller.register(rep)

        def listener():
            incoming, socket = reply_poller.poll()
            self.assertEqual(b'Hello', incoming[0])
            socket.send_string('Reply')
            reply_poller.resume_polling(socket)

        executor = zmq_async.get_executor(listener)
        executor.execute()

        req1 = self.ctx.socket(zmq.REQ)
        req1.connect(self.ADDR_REQ)

        req2 = self.ctx.socket(zmq.REQ)
        req2.connect(self.ADDR_REQ)

        req1.send_string('Hello')
        req2.send_string('Hello')

        reply = req1.recv_string()
        self.assertEqual('Reply', reply)

        reply = req2.recv_string()
        self.assertEqual('Reply', reply)

    def test_poll_timeout(self):
        rep = self.ctx.socket(zmq.REP)
        rep.bind(self.ADDR_REQ)

        reply_poller = zmq_async.get_reply_poller()
        reply_poller.register(rep)

        incoming, socket = reply_poller.poll(1)
        self.assertIsNone(incoming)
        self.assertIsNone(socket)


class TestZmqSerializer(test_utils.BaseTestCase):

    def test_message_without_topic_raises_RPCException(self):
        # The topic is the 4th element of the message.
        msg_without_topic = ['only', 'three', 'parts']

        expected = "Message did not contain a topic: %s" % msg_without_topic
        with self.assertRaisesRegexp(RPCException, re.escape(expected)):
            zmq_serializer.get_topic_from_call_message(msg_without_topic)

    def test_invalid_topic_format_raises_RPCException(self):
        invalid_topic = "no dots to split on, so not index-able".encode('utf8')
        bad_message = ['', '', '', invalid_topic]

        expected_msg = "Topic was not formatted correctly: %s"
        expected_msg = expected_msg % invalid_topic.decode('utf8')
        with self.assertRaisesRegexp(RPCException, expected_msg):
            zmq_serializer.get_topic_from_call_message(bad_message)

    def test_py3_decodes_bytes_correctly(self):
        message = ['', '', '', b'topic.ipaddress']

        actual, _ = zmq_serializer.get_topic_from_call_message(message)

        self.assertEqual('topic', actual)

    def test_bad_characters_in_topic_raise_RPCException(self):
        # handle unexpected os path separators:
        unexpected_evil = '<'
        os.path.sep = unexpected_evil

        unexpected_alt_evil = '>'
        os.path.altsep = unexpected_alt_evil

        evil_chars = [unexpected_evil, unexpected_alt_evil, '\\', '/']

        for evil_char in evil_chars:
            evil_topic = '%s%s%s' % ('trust.me', evil_char, 'please')
            evil_topic = evil_topic.encode('utf8')
            evil_message = ['', '', '', evil_topic]

            expected_msg = "Topic contained dangerous characters: %s"
            expected_msg = expected_msg % evil_topic.decode('utf8')
            expected_msg = re.escape(expected_msg)

            with self.assertRaisesRegexp(RPCException, expected_msg):
                zmq_serializer.get_topic_from_call_message(evil_message)
