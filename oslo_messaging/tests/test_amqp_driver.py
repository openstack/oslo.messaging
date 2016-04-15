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

import logging
import os
import select
import shutil
import socket
import subprocess
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
    from oslo_messaging._drivers.protocols.amqp import driver as amqp_driver

# The Cyrus-based SASL tests can only be run if the installed version of proton
# has been built with Cyrus SASL support.
_proton = importutils.try_import("proton")
CYRUS_ENABLED = (pyngus and pyngus.VERSION >= (2, 0, 0) and _proton
                 and getattr(_proton.SASL, "extended", lambda: False)())

LOG = logging.getLogger(__name__)


def _wait_until(predicate, timeout):
    deadline = timeout + time.time()
    while not predicate() and deadline > time.time():
        time.sleep(0.1)


class _ListenerThread(threading.Thread):
    """Run a blocking listener in a thread."""
    def __init__(self, listener, msg_count):
        super(_ListenerThread, self).__init__()
        self.listener = listener
        self.msg_count = msg_count
        self.messages = moves.queue.Queue()
        self.daemon = True
        self.started = threading.Event()
        self.start()
        self.started.wait()

    def run(self):
        LOG.debug("Listener started")
        self.started.set()
        while self.msg_count > 0:
            in_msg = self.listener.poll()[0]
            self.messages.put(in_msg)
            self.msg_count -= 1
            if in_msg.message.get('method') == 'echo':
                in_msg.reply(reply={'correlation-id':
                                    in_msg.message.get('id')})
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

    @testtools.skipUnless(pyngus, "proton modules not present")
    def setUp(self):
        super(_AmqpBrokerTestCase, self).setUp()
        self._broker = FakeBroker()
        self._broker_addr = "amqp://%s:%d" % (self._broker.host,
                                              self._broker.port)
        self._broker_url = oslo_messaging.TransportURL.parse(
            self.conf, self._broker_addr)
        self._broker.start()

    def tearDown(self):
        super(_AmqpBrokerTestCase, self).tearDown()
        self._broker.stop()


class TestAmqpSend(_AmqpBrokerTestCase):
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
        self.assertEqual(listener.messages.get().message, {"msg": "value"})
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
        self.assertEqual(rc.get('correlation-id'), 'e1')

        rc = driver.send(target2, {"context": "whatever"},
                         {"method": "echo", "id": "e2"},
                         wait_for_reply=True,
                         timeout=30)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'e2')

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
        self.assertEqual(self._broker.topic_count, 1)
        self.assertEqual(self._broker.direct_count, 1)  # reply

        # this should go to the other server:
        driver.send(shared_target, {"context": "whatever"},
                    {"method": "echo", "id": "either-2"},
                    wait_for_reply=True)
        self.assertEqual(self._broker.topic_count, 2)
        self.assertEqual(self._broker.direct_count, 2)  # reply

        # these should only go to listener1:
        driver.send(target1, {"context": "whatever"},
                    {"method": "echo", "id": "server1-1"},
                    wait_for_reply=True)

        driver.send(target1, {"context": "whatever"},
                    {"method": "echo", "id": "server1-2"},
                    wait_for_reply=True)
        self.assertEqual(self._broker.direct_count, 6)  # 2X(send+reply)

        # this should only go to listener2:
        driver.send(target2, {"context": "whatever"},
                    {"method": "echo", "id": "server2"},
                    wait_for_reply=True)
        self.assertEqual(self._broker.direct_count, 8)

        # both listeners should get a copy:
        driver.send(fanout_target, {"context": "whatever"},
                    {"method": "echo", "id": "fanout"})

        listener1.join(timeout=30)
        self.assertFalse(listener1.isAlive())
        listener2.join(timeout=30)
        self.assertFalse(listener2.isAlive())
        self.assertEqual(self._broker.fanout_count, 1)

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
        driver.cleanup()

    def test_send_timeout(self):
        """Verify send timeout."""
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)
        target = oslo_messaging.Target(topic="test-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)

        # the listener will drop this message:
        try:
            driver.send(target,
                        {"context": "whatever"},
                        {"method": "drop"},
                        wait_for_reply=True,
                        timeout=1.0)
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.MessagingTimeout, ex)
        else:
            self.assertTrue(False, "No Exception raised!")
        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        driver.cleanup()


class TestAmqpNotification(_AmqpBrokerTestCase):
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
                                             version)
                except oslo_messaging.MessageDeliveryFailure:
                    excepted_targets.append(t)

        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())
        topics = [x.message.get('target') for x in listener.get_messages()]
        self.assertEqual(len(topics), msg_count)
        self.assertEqual(topics.count('topic-1.info'), 2)
        self.assertEqual(topics.count('topic-1.error'), 2)
        self.assertEqual(topics.count('topic-2.debug'), 2)
        self.assertEqual(self._broker.dropped_count, 4)
        self.assertEqual(excepted_targets.count('topic-1.bad'), 0)
        self.assertEqual(excepted_targets.count('bad-topic.debug'), 0)
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
        self._broker = FakeBroker(sasl_mechanisms="PLAIN",
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
        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send,
                          target, {"context": True},
                          {"method": "echo"},
                          wait_for_reply=True,
                          timeout=2.0)
        driver.cleanup()


@testtools.skipUnless(CYRUS_ENABLED, "Cyrus SASL not supported")
class TestCyrusAuthentication(test_utils.BaseTestCase):
    """Test the driver's Cyrus SASL integration"""

    def setUp(self):
        """Create a simple SASL configuration. This assumes saslpasswd2 is in
        the OS path, otherwise the test will be skipped.
        """
        super(TestCyrusAuthentication, self).setUp()
        # Create a SASL configuration and user database,
        # add a user 'joe' with password 'secret':
        self._conf_dir = tempfile.mkdtemp()
        db = os.path.join(self._conf_dir, 'openstack.sasldb')
        _t = "echo secret | saslpasswd2 -c -p -f ${db} joe"
        cmd = Template(_t).substitute(db=db)
        try:
            subprocess.check_call(args=cmd, shell=True)
        except Exception:
            shutil.rmtree(self._conf_dir, ignore_errors=True)
            self._conf_dir = None
            raise self.skip("Cyrus tool saslpasswd2 not installed")

        # configure the SASL broker:
        conf = os.path.join(self._conf_dir, 'openstack.conf')
        # Note: don't add ANONYMOUS or EXTERNAL without updating the
        # test_authentication_bad_mechs test below
        mechs = "DIGEST-MD5 SCRAM-SHA-1 CRAM-MD5 PLAIN"
        t = Template("""sasldb_path: ${db}
pwcheck_method: auxprop
auxprop_plugin: sasldb
mech_list: ${mechs}
""")
        with open(conf, 'w') as f:
            f.write(t.substitute(db=db, mechs=mechs))

        self._broker = FakeBroker(sasl_mechanisms=mechs,
                                  user_credentials=["\0joe\0secret"],
                                  sasl_config_dir=self._conf_dir,
                                  sasl_config_name="openstack")
        self._broker.start()
        self.messaging_conf.transport_driver = 'amqp'
        self.conf = self.messaging_conf.conf

    def tearDown(self):
        super(TestCyrusAuthentication, self).tearDown()
        if self._broker:
            self._broker.stop()
            self._broker = None
        if self._conf_dir:
            shutil.rmtree(self._conf_dir, ignore_errors=True)

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
        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send,
                          target, {"context": True},
                          {"method": "echo"},
                          wait_for_reply=True,
                          timeout=2.0)
        driver.cleanup()

    def test_authentication_bad_mechs(self):
        """Verify that the connection fails if the client's SASL mechanisms do
        not match the broker's.
        """
        self.config(sasl_mechanisms="EXTERNAL ANONYMOUS",
                    group="oslo_messaging_amqp")
        addr = "amqp://joe:secret@%s:%d" % (self._broker.host,
                                            self._broker.port)
        url = oslo_messaging.TransportURL.parse(self.conf, addr)
        driver = amqp_driver.ProtonDriver(self.conf, url)
        target = oslo_messaging.Target(topic="test-topic")
        _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 1)
        self.assertRaises(oslo_messaging.MessagingTimeout,
                          driver.send,
                          target, {"context": True},
                          {"method": "echo"},
                          wait_for_reply=True,
                          timeout=2.0)
        driver.cleanup()

    def test_authentication_default_username(self):
        """Verify that a configured username/password is used if none appears
        in the URL.
        """
        addr = "amqp://%s:%d" % (self._broker.host, self._broker.port)
        self.config(username="joe",
                    password="secret",
                    group="oslo_messaging_amqp")
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


@testtools.skipUnless(pyngus, "proton modules not present")
class TestFailover(test_utils.BaseTestCase):

    def setUp(self):
        super(TestFailover, self).setUp()
        self._brokers = [FakeBroker(), FakeBroker()]
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

    def _failover(self, fail_brokers):
        self._brokers[0].start()
        # self.config(trace=True, group="oslo_messaging_amqp")
        driver = amqp_driver.ProtonDriver(self.conf, self._broker_url)

        target = oslo_messaging.Target(topic="my-topic")
        listener = _ListenerThread(
            driver.listen(target, None, None)._poll_style_listener, 2)

        # wait for listener links to come up
        # 4 == 3 links per listener + 1 for the global reply queue
        predicate = lambda: self._brokers[0].sender_link_count == 4
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        rc = driver.send(target, {"context": "whatever"},
                         {"method": "echo", "id": "echo-1"},
                         wait_for_reply=True,
                         timeout=30)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'echo-1')

        # 1 request msg, 1 response:
        self.assertEqual(self._brokers[0].topic_count, 1)
        self.assertEqual(self._brokers[0].direct_count, 1)

        # invoke failover method
        fail_brokers(self._brokers[0], self._brokers[1])

        # wait for listener links to re-establish on broker 1
        # 4 = 3 links per listener + 1 for the global reply queue
        predicate = lambda: self._brokers[1].sender_link_count == 4
        _wait_until(predicate, 30)
        self.assertTrue(predicate())

        rc = driver.send(target,
                         {"context": "whatever"},
                         {"method": "echo", "id": "echo-2"},
                         wait_for_reply=True,
                         timeout=2)
        self.assertIsNotNone(rc)
        self.assertEqual(rc.get('correlation-id'), 'echo-2')

        # 1 request msg, 1 response:
        self.assertEqual(self._brokers[1].topic_count, 1)
        self.assertEqual(self._brokers[1].direct_count, 1)

        listener.join(timeout=30)
        self.assertFalse(listener.isAlive())

        # note: stopping the broker first tests cleaning up driver without a
        # connection active
        self._brokers[1].stop()
        driver.cleanup()

    def test_broker_crash(self):
        """Simulate a failure of one broker."""
        def _meth(broker0, broker1):
            # fail broker 0 and start broker 1:
            broker0.stop()
            time.sleep(0.5)
            broker1.start()
        self._failover(_meth)

    def test_broker_shutdown(self):
        """Simulate a normal shutdown of a broker."""
        def _meth(broker0, broker1):
            broker0.stop(clean=True)
            time.sleep(0.5)
            broker1.start()
        self._failover(_meth)

    def test_heartbeat_failover(self):
        """Simulate broker heartbeat timeout."""
        def _meth(broker0, broker1):
            # keep alive heartbeat from broker 0 will stop, which should force
            # failover to broker 1 in about two seconds
            broker0.pause()
            broker1.start()
        self.config(idle_timeout=2, group="oslo_messaging_amqp")
        self._failover(_meth)
        self._brokers[0].stop()

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


class FakeBroker(threading.Thread):
    """A test AMQP message 'broker'."""

    if pyngus:
        class Connection(pyngus.ConnectionEventHandler):
            """A single AMQP connection."""

            def __init__(self, server, socket_, name,
                         sasl_mechanisms, user_credentials,
                         sasl_config_dir, sasl_config_name):
                """Create a Connection using socket_."""
                self.socket = socket_
                self.name = name
                self.server = server
                self.sasl_mechanisms = sasl_mechanisms
                self.user_credentials = user_credentials
                properties = {'x-server': True}
                if self.sasl_mechanisms:
                    properties['x-sasl-mechs'] = self.sasl_mechanisms
                    if "ANONYMOUS" not in self.sasl_mechanisms:
                        properties['x-require-auth'] = True
                if sasl_config_dir:
                    properties['x-sasl-config-dir'] = sasl_config_dir
                if sasl_config_name:
                    properties['x-sasl-config-name'] = sasl_config_name

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
                self.link.send(message)

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
                self.link.add_capacity(10)

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

            def receiver_remote_closed(self, receiver_link, error):
                self.link.close()

            def receiver_closed(self, receiver_link):
                self.server.receiver_link_count -= 1
                self.conn.dead_links.add(self)

            def receiver_failed(self, receiver_link, error):
                self.receiver_closed(receiver_link)

            def message_received(self, receiver_link, message, handle):
                """Forward this message out the proper sending link."""
                if self.server.forward_message(message):
                    self.link.message_accepted(handle)
                else:
                    self.link.message_rejected(handle)

                if self.link.capacity < 1:
                    self.link.add_capacity(10)

    def __init__(self, server_prefix="exclusive",
                 broadcast_prefix="broadcast",
                 group_prefix="unicast",
                 address_separator=".",
                 sock_addr="", sock_port=0,
                 sasl_mechanisms="ANONYMOUS",
                 user_credentials=None,
                 sasl_config_dir=None,
                 sasl_config_name=None):
        """Create a fake broker listening on sock_addr:sock_port."""
        if not pyngus:
            raise AssertionError("pyngus module not present")
        threading.Thread.__init__(self)
        self._server_prefix = server_prefix + address_separator
        self._broadcast_prefix = broadcast_prefix + address_separator
        self._group_prefix = group_prefix + address_separator
        self._address_separator = address_separator
        self._sasl_mechanisms = sasl_mechanisms
        self._sasl_config_dir = sasl_config_dir
        self._sasl_config_name = sasl_config_name
        self._user_credentials = user_credentials
        self._wakeup_pipe = os.pipe()
        self._my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._my_socket.bind((sock_addr, sock_port))
        self.host, self.port = self._my_socket.getsockname()
        self.container = pyngus.Container("test_server_%s:%d"
                                          % (self.host, self.port))
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

    def forward_message(self, message):
        # returns True if message was routed
        dest = message.address
        if dest not in self._sources:
            self.dropped_count += 1
            return False
        LOG.debug("Forwarding [%s]", dest)
        # route "behavior" determined by prefix:
        if dest.startswith(self._broadcast_prefix):
            self.fanout_count += 1
            for link in self._sources[dest]:
                self.fanout_sent_count += 1
                LOG.debug("Broadcast to %s", dest)
                link.send_message(message)
        elif dest.startswith(self._group_prefix):
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
        return True
