# Copyright (C) 2014 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import copy
import logging
import os
import select
import shlex
import shutil
from six.moves import mock
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from oslo_utils import importutils
from six import moves
from string import Template
import testtools

import oslo_messaging
from oslo_messaging.tests import utils as test_utils

# TODO(kgiusti) Conditionally run these tests only if the necessary
# dependencies are installed.  This should be removed once the proton libraries
# are available in the base repos for all supported platforms.
pyngus = importutils.try_import("pyngus")
if pyngus:
    from oslo_messaging._drivers.amqp1_driver.addressing \
        import AddresserFactory
    from oslo_messaging._drivers.amqp1_driver.addressing \
        import LegacyAddresser
    from oslo_messaging._drivers.amqp1_driver.addressing \
        import RoutableAddresser
    import oslo_messaging._drivers.impl_amqp1 as amqp_driver

# The Cyrus-based SASL tests can only be run if the installed version of proton
# has been built with Cyrus SASL support.
_proton = importutils.try_import("proton")
CYRUS_ENABLED = (pyngus and pyngus.VERSION >= (2, 0, 0) and _proton
                 and getattr(_proton.SASL, "extended", lambda: False)())
# same with SSL
# SSL_ENABLED = (_proton and getattr(_proton.SSL, "present", lambda: False)())
SSL_ENABLED = False

LOG = logging.getLogger(__name__)


def _wait_until(predicate, timeout):
    deadline = timeout + time.time()
    while not predicate() and deadline > time.time():
        time.sleep(0.1)


class _ListenerThread(threading.Thread):
    """Run a blocking listener in a thread."""
    def __init__(self, listener, msg_count, msg_ack=True):
        super(_ListenerThread, self).__init__()
        self.listener = listener
        self.msg_count = msg_count
        self._msg_ack = msg_ack
        self.messages = moves.queue.Queue()
        self.daemon = True
        self.started = threading.Event()
        self._done = False
        self.start()
        self.started.wait()

    def run(self):
        LOG.debug("Listener started")
        self.started.set()
        while not self._done:
            for in_msg in self.listener.poll(timeout=0.5):
                self.messages.put(in_msg)
                self.msg_count -= 1
                self._done = self.msg_count == 0
                if self._msg_ack:
                    in_msg.acknowledge()
                    if in_msg.message.get('method') == 'echo':
                        in_msg.reply(reply={'correlation-id':
                                            in_msg.message.get('id')})
                else:
                    in_msg.requeue()

        LOG.debug("Listener stopped")

    def get_messages(self):
        """Returns a list of all received messages."""
        msgs = []
        try:
            while True:
                m = self.messages.get(False)
                msgs.append(m)
        except moves.queue.Empty:
            pass
        return msgs

    def kill(self, timeout=30):
        self._done = True
        self.join(timeout)


@testtools.skipUnless(pyngus, "proton modules not present")
class TestProtonDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestProtonDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'amqp'

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver,
                              amqp_driver.ProtonDriver)


class _AmqpBrokerTestCase(test_utils.BaseTestCase):
    """Creates a single FakeBroker for use by the tests"""
    @testtools.skipUnless(pyngus, "proton modules not present")
    def setUp(self):
        super(_AmqpBrokerTestCase, self).setUp()
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp)
        self._broker_addr = "amqp://%s:%d" % (self._broker.host,
                                              self._broker.port)
        self._broker_url = oslo_messaging.TransportURL.parse(
            self.conf, self._broker_addr)

    def tearDown(self):
        super(_AmqpBrokerTestCase, self).tearDown()
        if self._broker:
            self._broker.stop()


class _AmqpBrokerTestCaseAuto(_AmqpBrokerTestCase):
    """Like _AmqpBrokerTestCase, but starts the broker"""
    @testtools.skipUnless(pyngus, "proton modules not present")
    def setUp(self):
        super(_AmqpBrokerTestCaseAuto, self).setUp()
        self._broker.start()


class TestAmqpSend(_AmqpBrokerTestCaseAuto):
    """Test sending and receiving messages."""

    def test_driver_unconnected_cleanup(self):
        """Verify the driver can cleanly shutdown even if never connected."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        driver.cleanup()

    def test_listener_cleanup(self):
        """Verify unused listener can cleanly shutdown."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = driver.listen(target, None, None)._poll_style_listener
        self.assertIsInstance(listener, amqp_driver.ProtonListener)
        driver.cleanup()

    def test_send_no_reply(self):
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        rc = driver.send(target, {"context": True},
                         {"msg": "value"}, wait_for_reply=False)
        self.assertIsNone(rc)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        self.assertEqual({"msg": "value"}, listener.messages.get().message)

        predicate = lambda: (self._broker.sender_link_ack_count == 1)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.cleanup()

    def test_send_exchange_with_reply(self):
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target1 = oslo_messaging.Target(topic="test-topic", exchange="e1")
        listener1 = _ListenerThread(
            driver.listen(target1, None, None)._poll_style_listener, 1)
        target2 = oslo_messaging.Target(topic="test-topic", exchange="e2")
        listener2 = _ListenerThread(
            driver.listen(target2, None, None)._poll_style_listener, 1)

        rc = driver.send(target1, {"context": "whatever"},
                         {"method": "echo", "id": "e1"},
                         wait_for_reply=True,
                         timeout=30)
        self.assertIsNotNone(rc)
        self.assertEqual('e1', rc.get('correlation-id'))

        rc = driver.send(target2, {"context": "whatever"},
                         {"method": "echo", "id": "e2"},
                         wait_for_reply=True,
                         timeout=30)
        self.assertIsNotNone(rc)
        self.assertEqual('e2', rc.get('correlation-id'))

        listener1.join(timeout=30)
        self.assertFalse(listener1.isAlive())
        listener2.join(timeout=30)
        self.assertFalse(listener2.isAlive())
        driver.cleanup()

    def test_messaging_patterns(self):
        """Verify the direct, shared, and fanout message patterns work."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target1 = oslo_messaging.Target(topic="test-topic", server="server1")
        listener1 = _ListenerThread(
            driver.listen(target1, None, None)._poll_style_listener, 4)
        target2 = oslo_messaging.Target(topic="test-topic", server="server2")
        listener2 = _ListenerThread(
            driver.listen(target2, None, None)._poll_style_listener, 3)

        shared_target = oslo_messaging.Target(topic="test-topic")
        fanout_target = oslo_messaging.Target(topic="test-topic",
                                              fanout=True)
        # this should go to only one server:
        driver.send(shared_target, {"context": "whatever"},
                    {"method": "echo", "id": "either-1"},
                    wait_for_reply=True)
        self.assertEqual(1, self._broker.topic_count)
        self.assertEqual(1, self._broker.direct_count)  # reply

        # this should go to the other server:
        driver.send(shared_target, {"context": "whatever"},
                    {"method": "echo", "id": "either-2"},
                    wait_for_reply=True)
        self.assertEqual(2, self._broker.topic_count)
        self.assertEqual(2, self._broker.direct_count)  # reply

        # these should only go to listener1:
        driver.send(target1, {"context": "whatever"},
                    {"method": "echo", "id": "server1-1"},
                    wait_for_reply=True)

        driver.send(target1, {"context": "whatever"},
                    {"method": "echo", "id": "server1-2"},
                    wait_for_reply=True)
        self.assertEqual(6, self._broker.direct_count)  # 2X(send+reply)

        # this should only go to listener2:
        driver.send(target2, {"context": "whatever"},
                    {"method": "echo", "id": "server2"},
                    wait_for_reply=True)
        self.assertEqual(8, self._broker.direct_count)

        # both listeners should get a copy:
        driver.send(fanout_target, {"context": "whatever"},
                    {"method": "echo", "id": "fanout"})

        listener1.join(timeout=30)
        self.assertFalse(listener1.isAlive())
        listener2.join(timeout=30)
        self.assertFalse(listener2.isAlive())
        self.assertEqual(1, self._broker.fanout_count)

        listener1_ids = [x.message.get('id') for x in listener1.get_messages()]
        listener2_ids = [x.message.get('id') for x in listener2.get_messages()]

        self.assertTrue('fanout' in listener1_ids and
                        'fanout' in listener2_ids)
        self.assertTrue('server1-1' in listener1_ids and
                        'server1-1' not in listener2_ids)
        self.assertTrue('server1-2' in listener1_ids and
                        'server1-2' not in listener2_ids)
        self.assertTrue('server2' in listener2_ids and
                        'server2' not in listener1_ids)
        if 'either-1' in listener1_ids:
            self.assertTrue('either-2' in listener2_ids and
                            'either-2' not in listener1_ids and
                            'either-1' not in listener2_ids)
        else:
            self.assertTrue('either-2' in listener1_ids and
                            'either-2' not in listener2_ids and
                            'either-1' in listener2_ids)

        predicate = lambda: (self._broker.sender_link_ack_count == 12)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.cleanup()

    def test_send_timeout(self):
        """Verify send timeout - no reply sent."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)

        # the listener will drop this message:
        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "drop"},
                          wait_for_reply=True,
                          timeout=1.0)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_released_send(self):
        """Verify exception thrown if send Nacked."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="no listener")

        # the broker will send a nack (released) since there is no active
        # listener for the target:
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "drop"},
                          wait_for_reply=True,
                          retry=0,
                          timeout=1.0)
        driver.cleanup()

    def test_send_not_acked(self):
        """Verify exception thrown ack dropped."""
        self.config(pre_settled=[],
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        # set this directly so we can use a value < minimum allowed
        driver._default_send_timeout = 2
        target = oslo_messaging.Target(topic="!no-ack!")

        # the broker will silently discard:
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "drop"},
                          retry=0,
                          wait_for_reply=True)
        driver.cleanup()

    def test_no_ack_cast(self):
        """Verify no exception is thrown if acks are turned off"""
        # set casts to ignore ack
        self.config(pre_settled=['rpc-cast'],
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        # set this directly so we can use a value < minimum allowed
        driver._default_send_timeout = 2
        target = oslo_messaging.Target(topic="!no-ack!")

        # the broker will silently discard this cast, but since ack'ing is
        # disabled the send does not fail
        driver.send(target, {"context": "whatever"},
                    {"method": "drop"},
                    wait_for_reply=False)
        driver.cleanup()

    def test_call_late_reply(self):
        """What happens if reply arrives after timeout?"""

        class _SlowResponder(_ListenerThread):
            def __init__(self, listener, delay):
                self._delay = delay
                super(_SlowResponder, self).__init__(listener, 1)

            def run(self):
                self.started.set()
                while not self._done:
                    for in_msg in self.listener.poll(timeout=0.5):
                        time.sleep(self._delay)
                        in_msg.acknowledge()
                        in_msg.reply(reply={'correlation-id':
                                            in_msg.message.get('id')})
                        self.messages.put(in_msg)
                        self._done = True

        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _SlowResponder(
            driver.listen(target, None, None)._poll_style_listener, 3)

        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "echo", "id": "???"},
                          wait_for_reply=True,
                          timeout=1.0)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())

        predicate = lambda: (self._broker.sender_link_ack_count == 1)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.cleanup()

    def test_call_failed_reply(self):
        """Send back an exception generated at the listener"""
        class _FailedResponder(_ListenerThread):
            def __init__(self, listener):
                super(_FailedResponder, self).__init__(listener, 1)

            def run(self):
                self.started.set()
                while not self._done:
                    for in_msg in self.listener.poll(timeout=0.5):
                        try:
                            raise RuntimeError("Oopsie!")
                        except RuntimeError:
                            in_msg.reply(reply=None,
                                         failure=sys.exc_info())
                        self._done = True

        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _FailedResponder(
            driver.listen(target, None, None)._poll_style_listener)

        self.assertRaises(RuntimeError,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "echo"},
                          wait_for_reply=True,
                          timeout=5.0)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_call_reply_timeout(self):
        """What happens if the replier times out?"""
        class _TimeoutListener(_ListenerThread):
            def __init__(self, listener):
                super(_TimeoutListener, self).__init__(listener, 1)

            def run(self):
                self.started.set()
                while not self._done:
                    for in_msg in self.listener.poll(timeout=0.5):
                        # reply will never be acked (simulate drop):
                        in_msg._reply_to = "!no-ack!"
                        in_msg.reply(reply={'correlation-id':
                                            in_msg.message.get("id")})
                        self._done = True

        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        driver._default_reply_timeout = 1
        target = oslo_messaging.Target(topic="test-topic")
        listener = _TimeoutListener(
            driver.listen(target, None, None)._poll_style_listener)

        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "echo"},
                          wait_for_reply=True,
                          timeout=3)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_listener_requeue(self):
        "Emulate Server requeue on listener incoming messages"
        self.config(pre_settled=[], group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        driver.require_features(requeue=True)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1,
            msg_ack=False)

        rc = driver.send(target, {"context": True},
                         {"msg": "value"}, wait_for_reply=False)
        self.assertIsNone(rc)

        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())

        predicate = lambda: (self._broker.sender_link_requeue_count == 1)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.cleanup()

    def test_sender_minimal_credit(self):
        # ensure capacity is replenished when only 1 credit is configured
        self.config(reply_link_credit=1,
                    rpc_server_credit=1,
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic", server="server")
        listener = _ListenerThread(driver.listen(target,
                                                 None,
                                                 None)._poll_style_listener,
                                   4)
        for i in range(4):
            threading.Thread(target=driver.send,
                             args=(target,
                                   {"context": "whatever"},
                                   {"method": "echo"}),
                             kwargs={'wait_for_reply': True}).start()
        predicate = lambda: (self._broker.direct_count == 8)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())
        listener.join(timeout=30)
        driver.cleanup()

    def test_sender_link_maintenance(self):
        # ensure links are purged from cache
        self.config(default_sender_link_timeout=1,
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic-maint")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 3)

        # the send should create a receiver link on the broker
        rc = driver.send(target, {"context": True},
                         {"msg": "value"}, wait_for_reply=False)
        self.assertIsNone(rc)

        predicate = lambda: (self._broker.receiver_link_count == 1)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        self.assertTrue(listener.isAlive())
        self.assertEqual({"msg": "value"}, listener.messages.get().message)

        predicate = lambda: (self._broker.receiver_link_count == 0)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        # the next send should create a separate receiver link on the broker
        rc = driver.send(target, {"context": True},
                         {"msg": "value"}, wait_for_reply=False)
        self.assertIsNone(rc)

        predicate = lambda: (self._broker.receiver_link_count == 1)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        self.assertTrue(listener.isAlive())
        self.assertEqual({"msg": "value"}, listener.messages.get().message)

        predicate = lambda: (self._broker.receiver_link_count == 0)
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.cleanup()


class TestAmqpNotification(_AmqpBrokerTestCaseAuto):
    """Test sending and receiving notifications."""

    def test_notification(self):
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        notifications = [(oslo_messaging.Target(topic="topic-1"), 'info'),
                         (oslo_messaging.Target(topic="topic-1"), 'error'),
                         (oslo_messaging.Target(topic="topic-2"), 'debug')]
        nl = driver.listen_for_notifications(
            notifications, None, None, None)._poll_style_listener

        # send one for each support version:
        msg_count = len(notifications) * 2
        listener = _ListenerThread(nl, msg_count)
        targets = ['topic-1.info',
                   'topic-1.bad',  # will raise MessageDeliveryFailure
                   'bad-topic.debug',  # will raise MessageDeliveryFailure
                   'topic-1.error',
                   'topic-2.debug']

        excepted_targets = []
        for version in (1.0, 2.0):
            for t in targets:
                try:
                    driver.send_notification(oslo_messaging.Target(topic=t),
                                             "context", {'target': t},
                                             version, retry=0)
                except oslo_messaging.MessageDeliveryFailure:
                    excepted_targets.append(t)

        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        topics = [x.message.get('target') for x in listener.get_messages()]
        self.assertEqual(msg_count, len(topics))
        self.assertEqual(2, topics.count('topic-1.info'))
        self.assertEqual(2, topics.count('topic-1.error'))
        self.assertEqual(2, topics.count('topic-2.debug'))
        self.assertEqual(4, self._broker.dropped_count)
        self.assertEqual(2, excepted_targets.count('topic-1.bad'))
        self.assertEqual(2, excepted_targets.count('bad-topic.debug'))
        driver.cleanup()

    def test_released_notification(self):
        """Broker sends a Nack (released)"""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send_notification,
                          oslo_messaging.Target(topic="bad address"),
                          "context", {'target': "bad address"},
                          2.0,
                          retry=0)
        driver.cleanup()

    def test_notification_not_acked(self):
        """Simulate drop of ack from broker"""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        # set this directly so we can use a value < minimum allowed
        driver._default_notify_timeout = 2
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send_notification,
                          oslo_messaging.Target(topic="!no-ack!"),
                          "context", {'target': "!no-ack!"},
                          2.0, retry=0)
        driver.cleanup()

    def test_no_ack_notification(self):
        """Verify no exception is thrown if acks are turned off"""
        # add a couple of illegal values for coverage of the warning
        self.config(pre_settled=['notify', 'fleabag', 'poochie'],
                    group="oslo_messaging_amqp")

        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        # set this directly so we can use a value < minimum allowed
        driver._default_notify_timeout = 2
        driver.send_notification(oslo_messaging.Target(topic="!no-ack!"),
                                 "context", {'target': "!no-ack!"}, 2.0)
        driver.cleanup()


@testtools.skipUnless(pyngus and pyngus.VERSION < (2, 0, 0),
                      "pyngus module not present")
class TestAuthentication(test_utils.BaseTestCase):
    """Test user authentication using the old pyngus API"""
    def setUp(self):
        super(TestAuthentication, self).setUp()
        # for simplicity, encode the credentials as they would appear 'on the
        # wire' in a SASL frame - username and password prefixed by zero.
        user_credentials = ["\0joe\0secret"]
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sasl_mechanisms="PLAIN",
                                  user_credentials=user_credentials)
        self._broker.start()

    def tearDown(self):
        super(TestAuthentication, self).tearDown()
        self._broker.stop()

    def test_authentication_ok(self):
        """Verify that username and password given in TransportHost are
        accepted by the broker.
        """

        addr = "amqp://joe:secret@%s:%d" % (self._broker.host,
                                            self._broker.port)
        url = oslo_messaging.TransportURL.parse(self.conf, addr)
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        rc = driver.send(target, {"context": True},
                         {"method": "echo"}, wait_for_reply=True)
        self.assertIsNotNone(rc)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_authentication_failure(self):
        """Verify that a bad password given in TransportHost is
        rejected by the broker.
        """

        addr = "amqp://joe:badpass@%s:%d" % (self._broker.host,
                                             self._broker.port)
        url = oslo_messaging.TransportURL.parse(self.conf, addr)
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send,
                          target, {"context": True},
                          {"method": "echo"},
                          wait_for_reply=True,
                          retry=2)
        driver.cleanup()


@testtools.skipUnless(CYRUS_ENABLED, "Cyrus SASL not supported")
class TestCyrusAuthentication(test_utils.BaseTestCase):
    """Test the driver's Cyrus SASL integration"""

    _conf_dir = None

    # Note: don't add ANONYMOUS or EXTERNAL mechs without updating the
    # test_authentication_bad_mechs test below
    _mechs = "DIGEST-MD5 SCRAM-SHA-1 CRAM-MD5 PLAIN"

    @classmethod
    def setUpClass(cls):
        # The Cyrus library can only be initialized once per _process_
        # Create a SASL configuration and user database,
        # add a user 'joe' with password 'secret':
        cls._conf_dir = "/tmp/amqp1_tests_%s" % os.getpid()
        # no, we cannot use tempfile.mkdtemp() as it will 'helpfully' remove
        # the temp dir after the first test is run
        os.makedirs(cls._conf_dir)
        db = os.path.join(cls._conf_dir, 'openstack.sasldb')
        _t = "echo secret | saslpasswd2 -c -p -f ${db} -u myrealm joe"
        cmd = Template(_t).substitute(db=db)
        try:
            subprocess.check_call(args=cmd, shell=True)
        except Exception:
            shutil.rmtree(cls._conf_dir, ignore_errors=True)
            cls._conf_dir = None
            return

        # configure the SASL server:
        conf = os.path.join(cls._conf_dir, 'openstack.conf')
        t = Template("""sasldb_path: ${db}
pwcheck_method: auxprop
auxprop_plugin: sasldb
mech_list: ${mechs}
""")
        with open(conf, 'w') as f:
            f.write(t.substitute(db=db, mechs=cls._mechs))

    @classmethod
    def tearDownClass(cls):
        if cls._conf_dir:
            shutil.rmtree(cls._conf_dir, ignore_errors=True)

    def setUp(self):
        # fire up a test broker with the SASL config:
        super(TestCyrusAuthentication, self).setUp()
        if TestCyrusAuthentication._conf_dir is None:
            self.skipTest("Cyrus SASL tools not installed")
        _mechs = TestCyrusAuthentication._mechs
        _dir = TestCyrusAuthentication._conf_dir
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sasl_mechanisms=_mechs,
                                  user_credentials=["\0joe@myrealm\0secret"],
                                  sasl_config_dir=_dir,
                                  sasl_config_name="openstack")
        self._broker.start()
        self.messaging_conf.transport_driver = 'amqp'
        self.conf = self.messaging_conf.conf

    def tearDown(self):
        if self._broker:
            self._broker.stop()
            self._broker = None
        super(TestCyrusAuthentication, self).tearDown()

    def _authentication_test(self, addr, retry=None):
        url = oslo_messaging.TransportURL.parse(self.conf, addr)
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        try:
            rc = driver.send(target, {"context": True},
                             {"method": "echo"}, wait_for_reply=True,
                             retry=retry)
            self.assertIsNotNone(rc)
            listener.join(timeout=30)
            self.assertFalse(listener.isAlive())
        finally:
            driver.cleanup()

    def test_authentication_ok(self):
        """Verify that username and password given in TransportHost are
        accepted by the broker.
        """
        addr = "amqp://joe@myrealm:secret@%s:%d" % (self._broker.host,
                                                    self._broker.port)
        self._authentication_test(addr)

    def test_authentication_failure(self):
        """Verify that a bad password given in TransportHost is
        rejected by the broker.
        """
        addr = "amqp://joe@myrealm:badpass@%s:%d" % (self._broker.host,
                                                     self._broker.port)
        try:
            self._authentication_test(addr, retry=2)
        except oslo_messaging.MessageDeliveryFailure as e:
            # verify the exception indicates the failure was an authentication
            # error
            self.assertTrue('amqp:unauthorized-access' in str(e))
        else:
            self.assertIsNone("Expected authentication failure")

    def test_authentication_bad_mechs(self):
        """Verify that the connection fails if the client's SASL mechanisms do
        not match the broker's.
        """
        self.config(sasl_mechanisms="EXTERNAL ANONYMOUS",
                    group="oslo_messaging_amqp")
        addr = "amqp://joe@myrealm:secret@%s:%d" % (self._broker.host,
                                                    self._broker.port)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self._authentication_test,
                          addr,
                          retry=0)

    def test_authentication_default_username(self):
        """Verify that a configured username/password is used if none appears
        in the URL.
        Deprecated: username password deprecated in favor of transport_url
        """
        addr = "amqp://%s:%d" % (self._broker.host, self._broker.port)
        self.config(username="joe@myrealm",
                    password="secret",
                    group="oslo_messaging_amqp")
        self._authentication_test(addr)

    def test_authentication_default_realm(self):
        """Verify that default realm is used if none present in username"""
        addr = "amqp://joe:secret@%s:%d" % (self._broker.host,
                                            self._broker.port)
        self.config(sasl_default_realm="myrealm",
                    group="oslo_messaging_amqp")
        self._authentication_test(addr)

    def test_authentication_ignore_default_realm(self):
        """Verify that default realm is not used if realm present in
        username
        """
        addr = "amqp://joe@myrealm:secret@%s:%d" % (self._broker.host,
                                                    self._broker.port)
        self.config(sasl_default_realm="bad-realm",
                    group="oslo_messaging_amqp")
        self._authentication_test(addr)


@testtools.skipUnless(pyngus, "proton modules not present")
class TestFailover(test_utils.BaseTestCase):

    def setUp(self):
        super(TestFailover, self).setUp()
        # configure different addressing modes on the brokers to test failing
        # over from one type of backend to another
        self.config(addressing_mode='dynamic', group="oslo_messaging_amqp")
        self._brokers = [FakeBroker(self.conf.oslo_messaging_amqp,
                                    product="qpid-cpp"),
                         FakeBroker(self.conf.oslo_messaging_amqp,
                                    product="routable")]
        self._primary = 0
        self._backup = 1
        hosts = []
        for broker in self._brokers:
            hosts.append(oslo_messaging.TransportHost(hostname=broker.host,
                                                      port=broker.port))
        self._broker_url = oslo_messaging.TransportURL(self.conf,
                                                       transport="amqp",
                                                       hosts=hosts)

    def tearDown(self):
        super(TestFailover, self).tearDown()
        for broker in self._brokers:
            if broker.isAlive():
                broker.stop()

    def _failover(self, fail_broker):
        self._brokers[0].start()
        self._brokers[1].start()

        # self.config(trace=True, group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)

        target = oslo_messaging.Target(topic="my-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 2)

        # wait for listener links to come up on either broker
        # 4 == 3 links per listener + 1 for the global reply queue
        predicate = lambda: ((self._brokers[0].sender_link_count == 4) or
                             (self._brokers[1].sender_link_count == 4))
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        if self._brokers[1].sender_link_count == 4:
            self._primary = 1
            self._backup = 0

        rc = driver.send(target, {"context": "whatever"},
                         {"method": "echo", "id": "echo-1"},
                         wait_for_reply=True,
                         timeout=30)
        self.assertIsNotNone(rc)
        self.assertEqual('echo-1', rc.get('correlation-id'))

        # 1 request msg, 1 response:
        self.assertEqual(1, self._brokers[self._primary].topic_count)
        self.assertEqual(1, self._brokers[self._primary].direct_count)

        # invoke failover method
        fail_broker(self._brokers[self._primary])

        # wait for listener links to re-establish on broker 1
        # 4 = 3 links per listener + 1 for the global reply queue
        predicate = lambda: self._brokers[self._backup].sender_link_count == 4
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        rc = driver.send(target,
                         {"context": "whatever"},
                         {"method": "echo", "id": "echo-2"},
                         wait_for_reply=True,
                         timeout=2)
        self.assertIsNotNone(rc)
        self.assertEqual('echo-2', rc.get('correlation-id'))

        # 1 request msg, 1 response:
        self.assertEqual(1, self._brokers[self._backup].topic_count)
        self.assertEqual(1, self._brokers[self._backup].direct_count)

        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())

        # note: stopping the broker first tests cleaning up driver without a
        # connection active
        self._brokers[self._backup].stop()
        driver.cleanup()

    def test_broker_crash(self):
        """Simulate a failure of one broker."""
        def _meth(broker):
            # fail broker:
            broker.stop()
            time.sleep(0.5)
        self._failover(_meth)

    def test_broker_shutdown(self):
        """Simulate a normal shutdown of a broker."""
        def _meth(broker):
            broker.stop(clean=True)
            time.sleep(0.5)
        self._failover(_meth)

    def test_heartbeat_failover(self):
        """Simulate broker heartbeat timeout."""
        def _meth(broker):
            # keep alive heartbeat from primary broker will stop, which should
            # force failover to backup broker in about two seconds
            broker.pause()
        self.config(idle_timeout=2, group="oslo_messaging_amqp")
        self._failover(_meth)
        self._brokers[self._primary].stop()

    def test_listener_failover(self):
        """Verify that Listeners sharing the same topic are re-established
        after failover.
        """
        self._brokers[0].start()
        # self.config(trace=True, group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)

        target = oslo_messaging.Target(topic="my-topic")
        bcast = oslo_messaging.Target(topic="my-topic", fanout=True)
        listener1 = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 2)
        listener2 = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 2)

        # wait for 7 sending links to become active on the broker.
        # 7 = 3 links per Listener + 1 global reply link
        predicate = lambda: self._brokers[0].sender_link_count == 7
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.send(bcast, {"context": "whatever"},
                    {"method": "ignore", "id": "echo-1"})

        # 1 message per listener
        predicate = lambda: self._brokers[0].fanout_sent_count == 2
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        #  start broker 1 then shutdown broker 0:
        self._brokers[1].start()
        self._brokers[0].stop(clean=True)

        # wait again for 7 sending links to re-establish on broker 1
        predicate = lambda: self._brokers[1].sender_link_count == 7
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        driver.send(bcast, {"context": "whatever"},
                    {"method": "ignore", "id": "echo-2"})

        # 1 message per listener
        predicate = lambda: self._brokers[1].fanout_sent_count == 2
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        listener1.join(timeout=30)
        listener2.join(timeout=30)
        self.assertFalse(listener1.isAlive() or listener2.isAlive())

        driver.cleanup()
        self._brokers[1].stop()


@testtools.skipUnless(pyngus, "proton modules not present")
class TestLinkRecovery(_AmqpBrokerTestCase):

    def _send_retry(self, reject, retries):
        self._reject = reject

        def on_active(link):
            if self._reject > 0:
                link.close()
                self._reject -= 1
            else:
                link.add_capacity(10)

        self._broker.on_receiver_active = on_active
        self._broker.start()
        self.config(link_retry_delay=1,
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(driver.listen(target,
                                                 None,
                                                 None)._poll_style_listener,
                                   1)
        try:
            rc = driver.send(target, {"context": "whatever"},
                             {"method": "echo", "id": "e1"},
                             wait_for_reply=True, retry=retries)
            self.assertIsNotNone(rc)
            self.assertEqual(rc.get('correlation-id'), 'e1')
        except Exception:
            listener.kill()
            driver.cleanup()
            raise
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        self.assertEqual(listener.messages.get().message.get('method'), "echo")
        driver.cleanup()

    def test_send_retry_ok(self):
        # verify sender with retry=3 survives 2 link failures:
        self._send_retry(reject=2, retries=3)

    def test_send_retry_fail(self):
        # verify sender fails if retries exhausted
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self._send_retry,
                          reject=3,
                          retries=2)

    def test_listener_recovery(self):
        # verify a listener recovers if all links fail:
        self._addrs = {'unicast.test-topic': 2,
                       'broadcast.test-topic.all': 2,
                       'exclusive.test-topic.server': 2}
        self._recovered = threading.Event()
        self._count = 0

        def _on_active(link):
            t = link.target_address
            if t in self._addrs:
                if self._addrs[t] > 0:
                    link.close()
                    self._addrs[t] -= 1
                else:
                    self._count += 1
                    if self._count == len(self._addrs):
                        self._recovered.set()

        self._broker.on_sender_active = _on_active
        self._broker.start()
        self.config(link_retry_delay=1, group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic",
                                       server="server")
        listener = _ListenerThread(driver.listen(target,
                                                 None,
                                                 None)._poll_style_listener,
                                   3)
        # wait for recovery
        self.assertTrue(self._recovered.wait(timeout=30))
        # verify server RPC:
        rc = driver.send(target, {"context": "whatever"},
                         {"method": "echo", "id": "e1"},
                         wait_for_reply=True)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'e1')
        # verify balanced RPC:
        target.server = None
        rc = driver.send(target, {"context": "whatever"},
                         {"method": "echo", "id": "e2"},
                         wait_for_reply=True)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'e2')
        # verify fanout:
        target.fanout = True
        driver.send(target, {"context": "whatever"},
                    {"msg": "value"},
                    wait_for_reply=False)
        listener.join(timeout=30)
        self.assertTrue(self._broker.fanout_count == 1)
        self.assertFalse(listener.isAlive())
        self.assertEqual(listener.messages.get().message.get('method'), "echo")
        driver.cleanup()

    def test_sender_credit_blocked(self):
        # ensure send requests resume once credit is provided
        self._blocked_links = set()

        def _on_active(link):
            # refuse granting credit for the broadcast link
            if self._broker._addresser._is_multicast(link.source_address):
                self._blocked_links.add(link)
            else:
                # unblock all link when RPC call is made
                link.add_capacity(10)
                for l in self._blocked_links:
                    l.add_capacity(10)

        self._broker.on_receiver_active = _on_active
        self._broker.on_credit_exhausted = lambda link: None
        self._broker.start()
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic", server="server")
        listener = _ListenerThread(driver.listen(target,
                                                 None,
                                                 None)._poll_style_listener,
                                   4)
        target.fanout = True
        target.server = None
        # these threads will share the same link
        for i in range(3):
            t = threading.Thread(target=driver.send,
                                 args=(target, {"context": "whatever"},
                                       {"msg": "n=%d" % i}),
                                 kwargs={'wait_for_reply': False})
            t.start()
            # casts return once message is put on active link
            t.join(timeout=30)

        time.sleep(1)  # ensure messages are going nowhere
        self.assertEqual(self._broker.fanout_sent_count, 0)
        # this will trigger the release of credit for the previous links
        target.fanout = False
        rc = driver.send(target, {"context": "whatever"},
                         {"method": "echo", "id": "e1"},
                         wait_for_reply=True)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'e1')
        listener.join(timeout=30)
        self.assertTrue(self._broker.fanout_count == 3)
        self.assertFalse(listener.isAlive())
        driver.cleanup()


@testtools.skipUnless(pyngus, "proton modules not present")
class TestAddressing(test_utils.BaseTestCase):
    # Verify the addressing modes supported by the driver
    def _address_test(self, rpc_target, targets_priorities):
        # verify proper messaging semantics for a given addressing mode
        broker = FakeBroker(self.conf.oslo_messaging_amqp)
        broker.start()
        url = oslo_messaging.TransportURL.parse(self.conf,
                                                "amqp://%s:%d" %
                                                (broker.host, broker.port))
        driver = amqp_driver.ProtonDriver(self.conf, url)

        rl = []
        for server in ["Server1", "Server2"]:
            _ = driver.listen(rpc_target(server=server), None,
                              None)._poll_style_listener
            # 3 == 1 msg to server + 1 fanout msg + 1 anycast msg
            rl.append(_ListenerThread(_, 3))

        nl = []
        for n in range(2):
            _ = driver.listen_for_notifications(targets_priorities, None, None,
                                                None)._poll_style_listener
            nl.append(_ListenerThread(_, len(targets_priorities)))

        driver.send(rpc_target(server="Server1"), {"context": "whatever"},
                    {"msg": "Server1"})
        driver.send(rpc_target(server="Server2"), {"context": "whatever"},
                    {"msg": "Server2"})
        driver.send(rpc_target(fanout=True), {"context": "whatever"},
                    {"msg": "Fanout"})
        # FakeBroker should evenly distribute these across the servers
        driver.send(rpc_target(server=None), {"context": "whatever"},
                    {"msg": "Anycast1"})
        driver.send(rpc_target(server=None), {"context": "whatever"},
                    {"msg": "Anycast2"})

        expected = []
        for n in targets_priorities:
            # this is how the notifier creates an address:
            topic = "%s.%s" % (n[0].topic, n[1])
            target = oslo_messaging.Target(topic=topic)
            driver.send_notification(target, {"context": "whatever"},
                                     {"msg": topic}, 2.0)
            expected.append(topic)

        for l in rl:
            l.join(timeout=30)

        # anycast will not evenly distribute an odd number of msgs
        predicate = lambda: len(expected) == (nl[0].messages.qsize() +
                                              nl[1].messages.qsize())
        _wait_until(predicate, 30)
        for l in nl:
            l.kill(timeout=30)

        s1_payload = [m.message.get('msg') for m in rl[0].get_messages()]
        s2_payload = [m.message.get('msg') for m in rl[1].get_messages()]

        self.assertTrue("Server1" in s1_payload
                        and "Server2" not in s1_payload)
        self.assertTrue("Server2" in s2_payload
                        and "Server1" not in s2_payload)
        self.assertEqual(s1_payload.count("Fanout"), 1)
        self.assertEqual(s2_payload.count("Fanout"), 1)
        self.assertEqual((s1_payload + s2_payload).count("Anycast1"), 1)
        self.assertEqual((s1_payload + s2_payload).count("Anycast2"), 1)

        n1_payload = [m.message.get('msg') for m in nl[0].get_messages()]
        n2_payload = [m.message.get('msg') for m in nl[1].get_messages()]

        self.assertEqual((n1_payload + n2_payload).sort(), expected.sort())

        driver.cleanup()
        broker.stop()
        return broker.message_log

    def test_routable_address(self):
        # verify routable address mode
        self.config(addressing_mode='routable', group="oslo_messaging_amqp")
        _opts = self.conf.oslo_messaging_amqp
        notifications = [(oslo_messaging.Target(topic="test-topic"), 'info'),
                         (oslo_messaging.Target(topic="test-topic"), 'error'),
                         (oslo_messaging.Target(topic="test-topic"), 'debug')]

        msgs = self._address_test(oslo_messaging.Target(exchange="ex",
                                                        topic="test-topic"),
                                  notifications)
        addrs = [m.address for m in msgs]

        notify_addrs = [a for a in addrs
                        if a.startswith(_opts.notify_address_prefix)]
        self.assertEqual(len(notify_addrs), len(notifications))
        # expect all notifications to be 'anycast'
        self.assertEqual(len(notifications),
                         len([a for a in notify_addrs
                              if _opts.anycast_address in a]))

        rpc_addrs = [a for a in addrs
                     if a.startswith(_opts.rpc_address_prefix)]
        # 2 anycast messages
        self.assertEqual(2,
                         len([a for a in rpc_addrs
                              if _opts.anycast_address in a]))
        # 1 fanout sent
        self.assertEqual(1,
                         len([a for a in rpc_addrs
                              if _opts.multicast_address in a]))
        # 2 unicast messages (1 for each server)
        self.assertEqual(2,
                         len([a for a in rpc_addrs
                              if _opts.unicast_address in a]))

    def test_legacy_address(self):
        # verify legacy address mode
        self.config(addressing_mode='legacy', group="oslo_messaging_amqp")
        _opts = self.conf.oslo_messaging_amqp
        notifications = [(oslo_messaging.Target(topic="test-topic"), 'info'),
                         (oslo_messaging.Target(topic="test-topic"), 'error'),
                         (oslo_messaging.Target(topic="test-topic"), 'debug')]

        msgs = self._address_test(oslo_messaging.Target(exchange="ex",
                                                        topic="test-topic"),
                                  notifications)
        addrs = [m.address for m in msgs]

        server_addrs = [a for a in addrs
                        if a.startswith(_opts.server_request_prefix)]
        broadcast_addrs = [a for a in addrs
                           if a.startswith(_opts.broadcast_prefix)]
        group_addrs = [a for a in addrs
                       if a.startswith(_opts.group_request_prefix)]
        # 2 server address messages sent
        self.assertEqual(len(server_addrs), 2)
        # 1 fanout address message sent
        self.assertEqual(len(broadcast_addrs), 1)
        # group messages: 2 rpc + all notifications
        self.assertEqual(len(group_addrs),
                         2 + len(notifications))

    def test_address_options(self):
        # verify addressing configuration options
        self.config(addressing_mode='routable', group="oslo_messaging_amqp")
        self.config(rpc_address_prefix="RPC-PREFIX",
                    group="oslo_messaging_amqp")
        self.config(notify_address_prefix="NOTIFY-PREFIX",
                    group="oslo_messaging_amqp")

        self.config(multicast_address="MULTI-CAST",
                    group="oslo_messaging_amqp")
        self.config(unicast_address="UNI-CAST",
                    group="oslo_messaging_amqp")
        self.config(anycast_address="ANY-CAST",
                    group="oslo_messaging_amqp")

        self.config(default_notification_exchange="NOTIFY-EXCHANGE",
                    group="oslo_messaging_amqp")
        self.config(default_rpc_exchange="RPC-EXCHANGE",
                    group="oslo_messaging_amqp")

        notifications = [(oslo_messaging.Target(topic="test-topic"), 'info'),
                         (oslo_messaging.Target(topic="test-topic"), 'error'),
                         (oslo_messaging.Target(topic="test-topic"), 'debug')]

        msgs = self._address_test(oslo_messaging.Target(exchange=None,
                                                        topic="test-topic"),
                                  notifications)
        addrs = [m.address for m in msgs]

        notify_addrs = [a for a in addrs
                        if a.startswith("NOTIFY-PREFIX")]
        self.assertEqual(len(notify_addrs), len(notifications))
        # expect all notifications to be 'anycast'
        self.assertEqual(len(notifications),
                         len([a for a in notify_addrs
                              if "ANY-CAST" in a]))
        # and all should contain the default exchange:
        self.assertEqual(len(notifications),
                         len([a for a in notify_addrs
                              if "NOTIFY-EXCHANGE" in a]))

        rpc_addrs = [a for a in addrs
                     if a.startswith("RPC-PREFIX")]
        # 2 RPC anycast messages
        self.assertEqual(2,
                         len([a for a in rpc_addrs
                              if "ANY-CAST" in a]))
        # 1 RPC fanout sent
        self.assertEqual(1,
                         len([a for a in rpc_addrs
                              if "MULTI-CAST" in a]))
        # 2 RPC unicast messages (1 for each server)
        self.assertEqual(2,
                         len([a for a in rpc_addrs
                              if "UNI-CAST" in a]))

        self.assertEqual(len(rpc_addrs),
                         len([a for a in rpc_addrs
                              if "RPC-EXCHANGE" in a]))

    def _dynamic_test(self, product):
        # return the addresser used when connected to 'product'
        broker = FakeBroker(self.conf.oslo_messaging_amqp,
                            product=product)
        broker.start()
        url = oslo_messaging.TransportURL.parse(self.conf,
                                                "amqp://%s:%d" %
                                                (broker.host, broker.port))
        driver = amqp_driver.ProtonDriver(self.conf, url)

        # need to send a message to initate the connection to the broker
        target = oslo_messaging.Target(topic="test-topic",
                                       server="Server")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        driver.send(target, {"context": True}, {"msg": "value"},
                    wait_for_reply=False)
        listener.join(timeout=30)

        addresser = driver._ctrl.addresser
        driver.cleanup()
        broker.stop()  # clears the driver's addresser
        return addresser

    def test_dynamic_addressing(self):
        # simply check that the correct addresser is provided based on the
        # identity of the messaging back-end
        self.config(addressing_mode='dynamic', group="oslo_messaging_amqp")
        self.assertIsInstance(self._dynamic_test("router"),
                              RoutableAddresser)
        self.assertIsInstance(self._dynamic_test("qpid-cpp"),
                              LegacyAddresser)


@testtools.skipUnless(pyngus, "proton modules not present")
class TestMessageRetransmit(_AmqpBrokerTestCase):
    # test message is retransmitted if safe to do so
    def _test_retransmit(self, nack_method):
        self._nack_count = 2

        def _on_message(message, handle, link):
            if self._nack_count:
                self._nack_count -= 1
                nack_method(link, handle)
            else:
                self._broker.forward_message(message, handle, link)

        self._broker.on_message = _on_message
        self._broker.start()
        self.config(link_retry_delay=1, pre_settled=[],
                    group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(driver.listen(target,
                                                 None,
                                                 None)._poll_style_listener,
                                   1)
        try:
            rc = driver.send(target, {"context": "whatever"},
                             {"method": "echo", "id": "blah"},
                             wait_for_reply=True,
                             retry=2)  # initial send + up to 2 resends
        except Exception:
            # Some test runs are expected to raise an exception,
            # clean up the listener since no message was received
            listener.kill(timeout=30)
            raise
        else:
            self.assertIsNotNone(rc)
            self.assertEqual(0, self._nack_count)
            self.assertEqual(rc.get('correlation-id'), 'blah')
            listener.join(timeout=30)
        finally:
            self.assertFalse(listener.isAlive())
            driver.cleanup()

    def test_released(self):
        # should retry and succeed
        self._test_retransmit(lambda l, h: l.message_released(h))

    def test_modified(self):
        # should retry and succeed
        self._test_retransmit(lambda l, h: l.message_modified(h,
                                                              False,
                                                              False,
                                                              {}))

    def test_modified_failed(self):
        # since delivery_failed is set to True, should fail
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self._test_retransmit,
                          lambda l, h: l.message_modified(h, True, False, {}))

    def test_rejected(self):
        # rejected - should fail
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self._test_retransmit,
                          lambda l, h: l.message_rejected(h, {}))


@testtools.skipUnless(SSL_ENABLED, "OpenSSL not supported")
class TestSSL(test_utils.BaseTestCase):
    """Test the driver's OpenSSL integration"""

    def setUp(self):
        super(TestSSL, self).setUp()
        # Create the CA, server, and client SSL certificates:
        self._tmpdir = tempfile.mkdtemp(prefix='amqp1')
        files = ['ca_key', 'ca_cert', 's_key', 's_req', 's_cert', 'c_key',
                 'c_req', 'c_cert', 'bad_cert', 'bad_req', 'bad_key']
        conf = dict(zip(files, [os.path.join(self._tmpdir, "%s.pem" % f)
                                for f in files]))
        conf['pw'] = 'password'
        conf['s_name'] = '127.0.0.1'
        conf['c_name'] = 'client.com'
        self._ssl_config = conf
        ssl_setup = [
            # create self-signed CA certificate:
            Template('openssl req -x509 -nodes -newkey rsa:2048'
                     ' -subj "/CN=Trusted.CA.com" -keyout ${ca_key}'
                     ' -out ${ca_cert}').substitute(conf),
            # create Server key and certificate:
            Template('openssl genrsa -out ${s_key} 2048').substitute(conf),
            Template('openssl req -new -key ${s_key} -subj /CN=${s_name}'
                     ' -passin pass:${pw} -out ${s_req}').substitute(conf),
            Template('openssl x509 -req -in ${s_req} -CA ${ca_cert}'
                     ' -CAkey ${ca_key} -CAcreateserial -out'
                     ' ${s_cert}').substitute(conf),
            # create a "bad" Server cert for testing CN validation:
            Template('openssl genrsa -out ${bad_key} 2048').substitute(conf),
            Template('openssl req -new -key ${bad_key} -subj /CN=Invalid'
                     ' -passin pass:${pw} -out ${bad_req}').substitute(conf),
            Template('openssl x509 -req -in ${bad_req} -CA ${ca_cert}'
                     ' -CAkey ${ca_key} -CAcreateserial -out'
                     ' ${bad_cert}').substitute(conf),
            # create Client key and certificate for client authentication:
            Template('openssl genrsa -out ${c_key} 2048').substitute(conf),
            Template('openssl req -new -key ${c_key} -subj /CN=${c_name}'
                     ' -passin pass:${pw} -out'
                     ' ${c_req}').substitute(conf),
            Template('openssl x509 -req -in ${c_req} -CA ${ca_cert}'
                     ' -CAkey ${ca_key} -CAcreateserial -out'
                     ' ${c_cert}').substitute(conf)
        ]
        for cmd in ssl_setup:
            try:
                subprocess.check_call(args=shlex.split(cmd))
            except Exception:
                shutil.rmtree(self._tmpdir, ignore_errors=True)
                self._tmpdir = None
                self.skipTest("OpenSSL tools not installed - skipping")

    def _ssl_server_ok(self, url):
        self._broker.start()
        self.config(ssl_ca_file=self._ssl_config['ca_cert'],
                    group='oslo_messaging_amqp')
        tport_url = oslo_messaging.TransportURL.parse(self.conf, url)
        driver = amqp_driver.ProtonDriver(self.conf, tport_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)

        driver.send(target,
                    {"context": "whatever"},
                    {"method": "echo", "a": "b"},
                    wait_for_reply=True,
                    timeout=30)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_server_ok(self):
        # test client authenticates server
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = "amqp://%s:%d" % (self._broker.host, self._broker.port)
        self._ssl_server_ok(url)

    def test_server_ignore_vhost_ok(self):
        # test client authenticates server and ignores vhost
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = "amqp://%s:%d/my-vhost" % (self._broker.host, self._broker.port)
        self._ssl_server_ok(url)

    def test_server_check_vhost_ok(self):
        # test client authenticates server using vhost as CN
        # Use 'Invalid' from bad_cert CN
        self.config(ssl_verify_vhost=True, group='oslo_messaging_amqp')
        self._ssl_config['s_cert'] = self._ssl_config['bad_cert']
        self._ssl_config['s_key'] = self._ssl_config['bad_key']
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = "amqp://%s:%d/Invalid" % (self._broker.host, self._broker.port)
        self._ssl_server_ok(url)

    @mock.patch('ssl.get_default_verify_paths')
    def test_server_ok_with_ssl_set_in_transport_url(self, mock_verify_paths):
        # test client authenticates server
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = oslo_messaging.TransportURL.parse(
            self.conf, "amqp://%s:%d?ssl=1" % (self._broker.host,
                                               self._broker.port))
        self._broker.start()
        mock_verify_paths.return_value = mock.Mock(
            cafile=self._ssl_config['ca_cert'])
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)

        driver.send(target,
                    {"context": "whatever"},
                    {"method": "echo", "a": "b"},
                    wait_for_reply=True,
                    timeout=30)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def test_bad_server_fail(self):
        # test client does not connect to invalid server
        self._ssl_config['s_cert'] = self._ssl_config['bad_cert']
        self._ssl_config['s_key'] = self._ssl_config['bad_key']
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = oslo_messaging.TransportURL.parse(self.conf, "amqp://%s:%d" %
                                                (self._broker.host,
                                                 self._broker.port))
        self._broker.start()

        self.config(ssl_ca_file=self._ssl_config['ca_cert'],
                    group='oslo_messaging_amqp')
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          driver.send, target,
                          {"context": "whatever"},
                          {"method": "echo", "a": "b"},
                          wait_for_reply=False,
                          retry=1)
        driver.cleanup()

    def test_client_auth_ok(self):
        # test server authenticates client
        self._ssl_config['authenticate_client'] = True
        self._broker = FakeBroker(self.conf.oslo_messaging_amqp,
                                  sock_addr=self._ssl_config['s_name'],
                                  ssl_config=self._ssl_config)
        url = oslo_messaging.TransportURL.parse(self.conf, "amqp://%s:%d" %
                                                (self._broker.host,
                                                 self._broker.port))
        self._broker.start()

        self.config(ssl_ca_file=self._ssl_config['ca_cert'],
                    ssl_cert_file=self._ssl_config['c_cert'],
                    ssl_key_file=self._ssl_config['c_key'],
                    ssl_key_password=self._ssl_config['pw'],
                    group='oslo_messaging_amqp')
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)

        driver.send(target,
                    {"context": "whatever"},
                    {"method": "echo", "a": "b"},
                    wait_for_reply=True,
                    timeout=30)
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()

    def tearDown(self):
        if self._broker:
            self._broker.stop()
            self._broker = None
        if self._tmpdir:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        super(TestSSL, self).tearDown()


@testtools.skipUnless(pyngus, "proton modules not present")
class TestVHost(_AmqpBrokerTestCaseAuto):
    """Verify the pseudo virtual host behavior"""

    def _vhost_test(self):
        """Verify that all messaging for a particular vhost stays on that vhost
        """
        self.config(pseudo_vhost=True,
                    group="oslo_messaging_amqp")

        vhosts = ["None", "HOSTA", "HOSTB", "HOSTC"]
        target = oslo_messaging.Target(topic="test-topic")
        fanout = oslo_messaging.Target(topic="test-topic", fanout=True)

        listeners = {}
        ldrivers = {}
        sdrivers = {}

        replies = {}
        msgs = {}

        for vhost in vhosts:
            url = copy.copy(self._broker_url)
            url.virtual_host = vhost if vhost != "None" else None
            ldriver = amqp_driver.ProtonDriver(self.conf, url)
            listeners[vhost] = _ListenerThread(
                ldriver.listen(target, None, None)._poll_style_listener,
                10)
            ldrivers[vhost] = ldriver
            sdrivers[vhost] = amqp_driver.ProtonDriver(self.conf, url)
            replies[vhost] = []
            msgs[vhost] = []

        # send a fanout and a single rpc call to each listener
        for vhost in vhosts:
            if vhost == "HOSTC":   # expect no messages to HOSTC
                continue
            sdrivers[vhost].send(fanout,
                                 {"context": vhost},
                                 {"vhost": vhost,
                                  "fanout": True,
                                  "id": vhost})
            replies[vhost].append(sdrivers[vhost].send(target,
                                                       {"context": vhost},
                                                       {"method": "echo",
                                                        "id": vhost},
                                                       wait_for_reply=True))
        time.sleep(1)

        for vhost in vhosts:
            msgs[vhost] += listeners[vhost].get_messages()
            if vhost == "HOSTC":
                # HOSTC should get nothing
                self.assertEqual(0, len(msgs[vhost]))
                self.assertEqual(0, len(replies[vhost]))
                continue

            self.assertEqual(2, len(msgs[vhost]))
            for m in msgs[vhost]:
                # the id must match the vhost
                self.assertEqual(vhost, m.message.get("id"))
            self.assertEqual(1, len(replies[vhost]))
            for m in replies[vhost]:
                # same for correlation id
                self.assertEqual(vhost, m.get("correlation-id"))

        for vhost in vhosts:
            listeners[vhost].kill()
            ldrivers[vhost].cleanup
            sdrivers[vhost].cleanup()

    def test_vhost_routing(self):
        """Test vhost using routable addresses
        """
        self.config(addressing_mode='routable', group="oslo_messaging_amqp")
        self._vhost_test()

    def test_vhost_legacy(self):
        """Test vhost using legacy addresses
        """
        self.config(addressing_mode='legacy', group="oslo_messaging_amqp")
        self._vhost_test()


class FakeBroker(threading.Thread):
    """A test AMQP message 'broker'."""

    if pyngus:
        class Connection(pyngus.ConnectionEventHandler):
            """A single AMQP connection."""

            def __init__(self, server, socket_, name, product,
                         sasl_mechanisms, user_credentials,
                         sasl_config_dir, sasl_config_name):
                """Create a Connection using socket_."""
                self.socket = socket_
                self.name = name
                self.server = server
                self.sasl_mechanisms = sasl_mechanisms
                self.user_credentials = user_credentials
                properties = {'x-server': True}
                # setup SASL:
                if self.sasl_mechanisms:
                    properties['x-sasl-mechs'] = self.sasl_mechanisms
                    if "ANONYMOUS" not in self.sasl_mechanisms:
                        properties['x-require-auth'] = True
                if sasl_config_dir:
                    properties['x-sasl-config-dir'] = sasl_config_dir
                if sasl_config_name:
                    properties['x-sasl-config-name'] = sasl_config_name
                # setup SSL
                if self.server._ssl_config:
                    ssl = self.server._ssl_config
                    properties['x-ssl-server'] = True
                    properties['x-ssl-identity'] = (ssl['s_cert'],
                                                    ssl['s_key'],
                                                    ssl['pw'])
                    # check for client authentication
                    if ssl.get('authenticate_client'):
                        properties['x-ssl-ca-file'] = ssl['ca_cert']
                        properties['x-ssl-verify-mode'] = 'verify-peer'
                        properties['x-ssl-peer-name'] = ssl['c_name']
                # misc connection properties
                if product:
                    properties['properties'] = {'product': product}

                self.connection = server.container.create_connection(
                    name, self, properties)
                self.connection.user_context = self
                if pyngus.VERSION < (2, 0, 0):
                    # older versions of pyngus don't recognize the sasl
                    # connection properties, so configure them manually:
                    if sasl_mechanisms:
                        self.connection.pn_sasl.mechanisms(sasl_mechanisms)
                        self.connection.pn_sasl.server()
                self.connection.open()
                self.sender_links = set()
                self.receiver_links = set()
                self.dead_links = set()

            def destroy(self):
                """Destroy the test connection."""
                for link in self.sender_links | self.receiver_links:
                    link.destroy()
                self.sender_links.clear()
                self.receiver_links.clear()
                self.dead_links.clear()
                self.connection.destroy()
                self.connection = None
                self.socket.close()
                self.socket = None

            def fileno(self):
                """Allows use of this in a select() call."""
                return self.socket.fileno()

            def process_input(self):
                """Called when socket is read-ready."""
                try:
                    pyngus.read_socket_input(self.connection, self.socket)
                    self.connection.process(time.time())
                except socket.error:
                    self._socket_error()

            def send_output(self):
                """Called when socket is write-ready."""
                try:
                    pyngus.write_socket_output(self.connection,
                                               self.socket)
                    self.connection.process(time.time())
                except socket.error:
                    self._socket_error()

            def _socket_error(self):
                self.connection.close_input()
                self.connection.close_output()
                # the broker will clean up in its main loop

            # Pyngus ConnectionEventHandler callbacks:

            def connection_active(self, connection):
                self.server.connection_count += 1

            def connection_remote_closed(self, connection, reason):
                """Peer has closed the connection."""
                self.connection.close()

            def connection_closed(self, connection):
                """Connection close completed."""
                self.server.connection_count -= 1

            def connection_failed(self, connection, error):
                """Connection failure detected."""
                self.connection_closed(connection)

            def sender_requested(self, connection, link_handle,
                                 name, requested_source, properties):
                """Create a new message source."""
                addr = requested_source or "source-" + uuid.uuid4().hex
                link = FakeBroker.SenderLink(self.server, self,
                                             link_handle, addr)
                self.sender_links.add(link)

            def receiver_requested(self, connection, link_handle,
                                   name, requested_target, properties):
                """Create a new message consumer."""
                addr = requested_target or "target-" + uuid.uuid4().hex
                FakeBroker.ReceiverLink(self.server, self,
                                        link_handle, addr)

            def sasl_step(self, connection, pn_sasl):
                # only called if not using Cyrus SASL
                if 'PLAIN' in self.sasl_mechanisms:
                    credentials = pn_sasl.recv()
                    if not credentials:
                        return  # wait until some arrives
                    if credentials not in self.user_credentials:
                        # failed
                        return pn_sasl.done(pn_sasl.AUTH)
                pn_sasl.done(pn_sasl.OK)

        class SenderLink(pyngus.SenderEventHandler):
            """An AMQP sending link."""
            def __init__(self, server, conn, handle, src_addr=None):
                self.server = server
                self.conn = conn
                cnn = conn.connection
                self.link = cnn.accept_sender(handle,
                                              source_override=src_addr,
                                              event_handler=self)
                conn.sender_links.add(self)
                self.link.open()
                self.routed = False

            def destroy(self):
                """Destroy the link."""
                conn = self.conn
                self.conn = None
                conn.sender_links.remove(self)
                conn.dead_links.discard(self)
                if self.link:
                    self.link.destroy()
                    self.link = None

            def send_message(self, message):
                """Send a message over this link."""
                def pyngus_callback(link, handle, state, info):
                    if state == pyngus.SenderLink.ACCEPTED:
                        self.server.sender_link_ack_count += 1
                    elif state == pyngus.SenderLink.RELEASED:
                        self.server.sender_link_requeue_count += 1

                self.link.send(message, delivery_callback=pyngus_callback)

            def _cleanup(self):
                if self.routed:
                    self.server.remove_route(self.link.source_address,
                                             self)
                    self.routed = False
                self.conn.dead_links.add(self)

            # Pyngus SenderEventHandler callbacks:

            def sender_active(self, sender_link):
                self.server.sender_link_count += 1
                self.server.add_route(self.link.source_address, self)
                self.routed = True
                self.server.on_sender_active(sender_link)

            def sender_remote_closed(self, sender_link, error):
                self.link.close()

            def sender_closed(self, sender_link):
                self.server.sender_link_count -= 1
                self._cleanup()

            def sender_failed(self, sender_link, error):
                self.sender_closed(sender_link)

        class ReceiverLink(pyngus.ReceiverEventHandler):
            """An AMQP Receiving link."""
            def __init__(self, server, conn, handle, addr=None):
                self.server = server
                self.conn = conn
                cnn = conn.connection
                self.link = cnn.accept_receiver(handle,
                                                target_override=addr,
                                                event_handler=self)
                conn.receiver_links.add(self)
                self.link.open()

            def destroy(self):
                """Destroy the link."""
                conn = self.conn
                self.conn = None
                conn.receiver_links.remove(self)
                conn.dead_links.discard(self)
                if self.link:
                    self.link.destroy()
                    self.link = None

            # ReceiverEventHandler callbacks:

            def receiver_active(self, receiver_link):
                self.server.receiver_link_count += 1
                self.server.on_receiver_active(receiver_link)

            def receiver_remote_closed(self, receiver_link, error):
                self.link.close()

            def receiver_closed(self, receiver_link):
                self.server.receiver_link_count -= 1
                self.conn.dead_links.add(self)

            def receiver_failed(self, receiver_link, error):
                self.receiver_closed(receiver_link)

            def message_received(self, receiver_link, message, handle):
                """Forward this message out the proper sending link."""
                self.server.on_message(message, handle, receiver_link)
                if self.link.capacity < 1:
                    self.server.on_credit_exhausted(self.link)

    def __init__(self, cfg,
                 sock_addr="", sock_port=0,
                 product=None,
                 default_exchange="Test-Exchange",
                 sasl_mechanisms="ANONYMOUS",
                 user_credentials=None,
                 sasl_config_dir=None,
                 sasl_config_name=None,
                 ssl_config=None):
        """Create a fake broker listening on sock_addr:sock_port."""
        if not pyngus:
            raise AssertionError("pyngus module not present")
        threading.Thread.__init__(self)
        self._product = product
        self._sasl_mechanisms = sasl_mechanisms
        self._sasl_config_dir = sasl_config_dir
        self._sasl_config_name = sasl_config_name
        self._user_credentials = user_credentials
        self._ssl_config = ssl_config
        self._wakeup_pipe = os.pipe()
        self._my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._my_socket.bind((sock_addr, sock_port))
        self.host, self.port = self._my_socket.getsockname()
        self.container = pyngus.Container("test_server_%s:%d"
                                          % (self.host, self.port))

        # create an addresser using the test client's config and expected
        # message bus so the broker can parse the message addresses
        af = AddresserFactory(default_exchange,
                              cfg.addressing_mode,
                              legacy_server_prefix=cfg.server_request_prefix,
                              legacy_broadcast_prefix=cfg.broadcast_prefix,
                              legacy_group_prefix=cfg.group_request_prefix,
                              rpc_prefix=cfg.rpc_address_prefix,
                              notify_prefix=cfg.notify_address_prefix,
                              multicast=cfg.multicast_address,
                              unicast=cfg.unicast_address,
                              anycast=cfg.anycast_address)
        props = {'product': product} if product else {}
        self._addresser = af(props)

        self._connections = {}
        self._sources = {}
        self._pause = threading.Event()
        # count of messages forwarded, by messaging pattern
        self.direct_count = 0
        self.topic_count = 0
        self.fanout_count = 0
        self.fanout_sent_count = 0
        self.dropped_count = 0
        # counts for active links and connections:
        self.connection_count = 0
        self.sender_link_count = 0
        self.receiver_link_count = 0
        self.sender_link_ack_count = 0
        self.sender_link_requeue_count = 0
        # log of all messages received by the broker
        self.message_log = []
        # callback hooks
        self.on_sender_active = lambda link: None
        self.on_receiver_active = lambda link: link.add_capacity(10)
        self.on_credit_exhausted = lambda link: link.add_capacity(10)
        self.on_message = lambda m, h, l: self.forward_message(m, h, l)

    def start(self):
        """Start the server."""
        LOG.debug("Starting Test Broker on %s:%d", self.host, self.port)
        self._shutdown = False
        self._closing = False
        self.daemon = True
        self._pause.set()
        self._my_socket.listen(10)
        super(FakeBroker, self).start()

    def pause(self):
        self._pause.clear()
        os.write(self._wakeup_pipe[1], b'!')

    def unpause(self):
        self._pause.set()

    def stop(self, clean=False):
        """Stop the server."""
        # If clean is True, attempt a clean shutdown by closing all open
        # links/connections first.  Otherwise force an immediate disconnect
        LOG.debug("Stopping test Broker %s:%d", self.host, self.port)
        if clean:
            self._closing = 1
        else:
            self._shutdown = True
        self._pause.set()
        os.write(self._wakeup_pipe[1], b'!')
        self.join()
        LOG.debug("Test Broker %s:%d stopped", self.host, self.port)

    def run(self):
        """Process I/O and timer events until the broker is stopped."""
        LOG.debug("Test Broker on %s:%d started", self.host, self.port)
        while not self._shutdown:
            self._pause.wait()
            readers, writers, timers = self.container.need_processing()

            # map pyngus Connections back to _TestConnections:
            readfd = [c.user_context for c in readers]
            readfd.extend([self._my_socket, self._wakeup_pipe[0]])
            writefd = [c.user_context for c in writers]

            timeout = None
            if timers:
                # [0] == next expiring timer
                deadline = timers[0].next_tick
                now = time.time()
                timeout = 0 if deadline <= now else deadline - now

            readable, writable, ignore = select.select(readfd,
                                                       writefd,
                                                       [],
                                                       timeout)
            worked = set()
            for r in readable:
                if r is self._my_socket:
                    # new inbound connection request received
                    sock, addr = self._my_socket.accept()
                    if not self._closing:
                        # create a new Connection for it:
                        name = str(addr)
                        conn = FakeBroker.Connection(self, sock, name,
                                                     self._product,
                                                     self._sasl_mechanisms,
                                                     self._user_credentials,
                                                     self._sasl_config_dir,
                                                     self._sasl_config_name)
                        self._connections[conn.name] = conn
                    else:
                        sock.close()  # drop it
                elif r is self._wakeup_pipe[0]:
                    os.read(self._wakeup_pipe[0], 512)
                else:
                    r.process_input()
                    worked.add(r)

            for t in timers:
                now = time.time()
                if t.next_tick > now:
                    break
                t.process(now)
                conn = t.user_context
                worked.add(conn)

            for w in writable:
                w.send_output()
                worked.add(w)

            # clean up any closed connections or links:
            while worked:
                conn = worked.pop()
                if conn.connection.closed:
                    del self._connections[conn.name]
                    conn.destroy()
                else:
                    while conn.dead_links:
                        conn.dead_links.pop().destroy()

            if self._closing and not self._connections:
                self._shutdown = True
            elif self._closing == 1:
                # start closing connections
                self._closing = 2
                for conn in self._connections.values():
                    conn.connection.close()

        # Shutting down.  Any open links are just disconnected - the peer will
        # see a socket close.
        self._my_socket.close()
        for conn in self._connections.values():
            conn.destroy()
        self._connections = None
        self.container.destroy()
        self.container = None
        return 0

    def add_route(self, address, link):
        # route from address -> link[, link ...]
        if address not in self._sources:
            self._sources[address] = [link]
        elif link not in self._sources[address]:
            self._sources[address].append(link)

    def remove_route(self, address, link):
        if address in self._sources:
            if link in self._sources[address]:
                self._sources[address].remove(link)
                if not self._sources[address]:
                    del self._sources[address]

    def forward_message(self, message, handle, rlink):
        # returns True if message was routed
        self.message_log.append(message)
        dest = message.address
        if dest not in self._sources:
            # can't forward
            self.dropped_count += 1
            # observe magic "don't ack" address
            if '!no-ack!' not in dest:
                rlink.message_released(handle)
            return

        LOG.debug("Forwarding [%s]", dest)
        # route "behavior" determined by address prefix:
        if self._addresser._is_multicast(dest):
            self.fanout_count += 1
            for link in self._sources[dest]:
                self.fanout_sent_count += 1
                LOG.debug("Broadcast to %s", dest)
                link.send_message(message)
        elif self._addresser._is_anycast(dest):
            # round-robin:
            self.topic_count += 1
            link = self._sources[dest].pop(0)
            link.send_message(message)
            LOG.debug("Send to %s", dest)
            self._sources[dest].append(link)
        else:
            # unicast:
            self.direct_count += 1
            LOG.debug("Unicast to %s", dest)
            self._sources[dest][0].send_message(message)
        rlink.message_accepted(handle)
