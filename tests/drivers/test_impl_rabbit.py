# Copyright 2013 Red Hat, Inc.
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

import datetime
import sys
import threading
import time
import uuid

import fixtures
import kombu
import mock
from oslotest import mockpatch
import testscenarios

from oslo.config import cfg
from oslo import messaging
from oslo.serialization import jsonutils
from oslo_messaging._drivers import amqp
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers import impl_rabbit as rabbit_driver
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class TestDeprecatedRabbitDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestDeprecatedRabbitDriverLoad, self).setUp(
            conf=cfg.ConfigOpts())
        self.messaging_conf.transport_driver = 'rabbit'
        self.config(fake_rabbit=True, group="oslo_messaging_rabbit")

    def test_driver_load(self):
        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')
        transport = messaging.get_transport(self.conf)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        url = driver._get_connection()._url

        self.assertIsInstance(driver, rabbit_driver.RabbitDriver)
        self.assertEqual('memory:////', url)


class TestRabbitDriverLoad(test_utils.BaseTestCase):

    scenarios = [
        ('rabbit', dict(transport_driver='rabbit',
                        url='amqp://guest:guest@localhost:5672//')),
        ('kombu', dict(transport_driver='kombu',
                       url='amqp://guest:guest@localhost:5672//')),
        ('rabbit+memory', dict(transport_driver='kombu+memory',
                               url='memory:///'))
    ]

    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.ensure')
    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.reset')
    def test_driver_load(self, fake_ensure, fake_reset):
        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')
        self.messaging_conf.transport_driver = self.transport_driver
        transport = messaging.get_transport(self.conf)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        url = driver._get_connection()._url

        self.assertIsInstance(driver, rabbit_driver.RabbitDriver)
        self.assertEqual(self.url, url)


class TestRabbitIterconsume(test_utils.BaseTestCase):

    def test_iterconsume_timeout(self):
        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        deadline = time.time() + 3
        with transport._driver._get_connection(amqp.PURPOSE_LISTEN) as conn:
            conn.iterconsume(timeout=3)
            # kombu memory transport doesn't really raise error
            # so just simulate a real driver behavior
            conn.connection.connection.recoverable_channel_errors = (IOError,)
            conn.declare_fanout_consumer("notif.info", lambda msg: True)
            with mock.patch('kombu.connection.Connection.drain_events',
                            side_effect=IOError):
                try:
                    conn.consume(timeout=3)
                except driver_common.Timeout:
                    pass

        self.assertEqual(0, int(deadline - time.time()))


class TestRabbitTransportURL(test_utils.BaseTestCase):

    scenarios = [
        ('none', dict(url=None,
                      expected=["amqp://guest:guest@localhost:5672//"])),
        ('memory', dict(url='kombu+memory:////',
                        expected=["memory:///"])),
        ('empty',
         dict(url='rabbit:///',
              expected=['amqp://guest:guest@localhost:5672/'])),
        ('localhost',
         dict(url='rabbit://localhost/',
              expected=['amqp://:@localhost:5672/'])),
        ('virtual_host',
         dict(url='rabbit:///vhost',
              expected=['amqp://guest:guest@localhost:5672/vhost'])),
        ('no_creds',
         dict(url='rabbit://host/virtual_host',
              expected=['amqp://:@host:5672/virtual_host'])),
        ('no_port',
         dict(url='rabbit://user:password@host/virtual_host',
              expected=['amqp://user:password@host:5672/virtual_host'])),
        ('full_url',
         dict(url='rabbit://user:password@host:10/virtual_host',
              expected=['amqp://user:password@host:10/virtual_host'])),
        ('full_two_url',
         dict(url='rabbit://user:password@host:10,'
              'user2:password2@host2:12/virtual_host',
              expected=["amqp://user:password@host:10/virtual_host",
                        "amqp://user2:password2@host2:12/virtual_host"]
              )),
        ('qpid',
         dict(url='kombu+qpid://user:password@host:10/virtual_host',
              expected=['qpid://user:password@host:10/virtual_host'])),
        ('rabbit',
         dict(url='kombu+rabbit://user:password@host:10/virtual_host',
              expected=['amqp://user:password@host:10/virtual_host'])),
        ('rabbit_ipv6',
         dict(url='kombu+rabbit://u:p@[fd00:beef:dead:55::133]:10/vhost',
              skip_py26='python 2.6 has broken urlparse for ipv6',
              expected=['amqp://u:p@[fd00:beef:dead:55::133]:10/vhost'])),
        ('rabbit_ipv4',
         dict(url='kombu+rabbit://user:password@10.20.30.40:10/vhost',
              expected=['amqp://user:password@10.20.30.40:10/vhost'])),
    ]

    def setUp(self):
        super(TestRabbitTransportURL, self).setUp()
        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')
        self.messaging_conf.transport_driver = 'rabbit'

    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.ensure')
    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.reset')
    def test_transport_url(self, fake_ensure_connection, fake_reset):
        if hasattr(self, 'skip_py26') and sys.version_info < (2, 7):
            self.skipTest(self.skip_py26)

        transport = messaging.get_transport(self.conf, self.url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver

        # NOTE(sileht): some kombu transport can depend on library that
        # we don't want to depend yet, because selecting the transport
        # is experimental, only amqp is supported
        # for example kombu+qpid depends of qpid-tools
        # so, mock the connection.info to skip call to qpid-tools
        with mock.patch('kombu.connection.Connection.info'):
            urls = driver._get_connection()._url.split(";")
        self.assertEqual(sorted(self.expected), sorted(urls))


class TestSendReceive(test_utils.BaseTestCase):

    _n_senders = [
        ('single_sender', dict(n_senders=1)),
        ('multiple_senders', dict(n_senders=10)),
    ]

    _context = [
        ('empty_context', dict(ctxt={})),
        ('with_context', dict(ctxt={'user': 'mark'})),
    ]

    _reply = [
        ('rx_id', dict(rx_id=True, reply=None)),
        ('none', dict(rx_id=False, reply=None)),
        ('empty_list', dict(rx_id=False, reply=[])),
        ('empty_dict', dict(rx_id=False, reply={})),
        ('false', dict(rx_id=False, reply=False)),
        ('zero', dict(rx_id=False, reply=0)),
    ]

    _failure = [
        ('success', dict(failure=False)),
        ('failure', dict(failure=True, expected=False)),
        ('expected_failure', dict(failure=True, expected=True)),
    ]

    _timeout = [
        ('no_timeout', dict(timeout=None)),
        ('timeout', dict(timeout=0.01)),  # FIXME(markmc): timeout=0 is broken?
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._n_senders,
                                                         cls._context,
                                                         cls._reply,
                                                         cls._failure,
                                                         cls._timeout)

    def test_send_receive(self):
        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')
        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = messaging.Target(topic='testtopic')

        listener = driver.listen(target)

        senders = []
        replies = []
        msgs = []
        errors = []

        def stub_error(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            errors.append(str(msg) % a)

        self.stubs.Set(driver_common.LOG, 'error', stub_error)

        def send_and_wait_for_reply(i):
            try:
                replies.append(driver.send(target,
                                           self.ctxt,
                                           {'tx_id': i},
                                           wait_for_reply=True,
                                           timeout=self.timeout))
                self.assertFalse(self.failure)
                self.assertIsNone(self.timeout)
            except (ZeroDivisionError, messaging.MessagingTimeout) as e:
                replies.append(e)
                self.assertTrue(self.failure or self.timeout is not None)

        while len(senders) < self.n_senders:
            senders.append(threading.Thread(target=send_and_wait_for_reply,
                                            args=(len(senders), )))

        for i in range(len(senders)):
            senders[i].start()

            received = listener.poll()
            self.assertIsNotNone(received)
            self.assertEqual(self.ctxt, received.ctxt)
            self.assertEqual({'tx_id': i}, received.message)
            msgs.append(received)

        # reply in reverse, except reply to the first guy second from last
        order = list(range(len(senders) - 1, -1, -1))
        if len(order) > 1:
            order[-1], order[-2] = order[-2], order[-1]

        for i in order:
            if self.timeout is None:
                if self.failure:
                    try:
                        raise ZeroDivisionError
                    except Exception:
                        failure = sys.exc_info()

                    # NOTE(noelbk) confirm that Publisher exchanges
                    # are always declared with passive=True
                    outer_self = self
                    test_exchange_was_called = [False]
                    old_init = kombu.entity.Exchange.__init__

                    def new_init(self, *args, **kwargs):
                        test_exchange_was_called[0] = True
                        outer_self.assertTrue(kwargs['passive'])
                        old_init(self, *args, **kwargs)
                    kombu.entity.Exchange.__init__ = new_init

                    try:
                        msgs[i].reply(failure=failure,
                                      log_failure=not self.expected)
                    finally:
                        kombu.entity.Exchange.__init__ = old_init

                    self.assertTrue(test_exchange_was_called[0])

                elif self.rx_id:
                    msgs[i].reply({'rx_id': i})
                else:
                    msgs[i].reply(self.reply)
            senders[i].join()

        self.assertEqual(len(senders), len(replies))
        for i, reply in enumerate(replies):
            if self.timeout is not None:
                self.assertIsInstance(reply, messaging.MessagingTimeout)
            elif self.failure:
                self.assertIsInstance(reply, ZeroDivisionError)
            elif self.rx_id:
                self.assertEqual({'rx_id': order[i]}, reply)
            else:
                self.assertEqual(self.reply, reply)

        if not self.timeout and self.failure and not self.expected:
            self.assertTrue(len(errors) > 0, errors)
        else:
            self.assertEqual(0, len(errors), errors)


TestSendReceive.generate_scenarios()


class TestPollAsync(test_utils.BaseTestCase):

    def test_poll_timeout(self):
        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        target = messaging.Target(topic='testtopic')
        listener = driver.listen(target)
        received = listener.poll(timeout=0.050)
        self.assertIsNone(received)


class TestRacyWaitForReply(test_utils.BaseTestCase):

    def test_send_receive(self):
        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = messaging.Target(topic='testtopic')

        listener = driver.listen(target)

        senders = []
        replies = []
        msgs = []

        wait_conditions = []
        orig_reply_waiter = amqpdriver.ReplyWaiter.wait

        def reply_waiter(self, msg_id, timeout):
            if wait_conditions:
                cond = wait_conditions.pop()
                with cond:
                    cond.notify()
                with cond:
                    cond.wait()
            return orig_reply_waiter(self, msg_id, timeout)

        self.stubs.Set(amqpdriver.ReplyWaiter, 'wait', reply_waiter)

        def send_and_wait_for_reply(i, wait_for_reply):
            replies.append(driver.send(target,
                                       {},
                                       {'tx_id': i},
                                       wait_for_reply=wait_for_reply,
                                       timeout=None))

        while len(senders) < 2:
            t = threading.Thread(target=send_and_wait_for_reply,
                                 args=(len(senders), True))
            t.daemon = True
            senders.append(t)

        # test the case then msg_id is not set
        t = threading.Thread(target=send_and_wait_for_reply,
                             args=(len(senders), False))
        t.daemon = True
        senders.append(t)

        # Start the first guy, receive his message, but delay his polling
        notify_condition = threading.Condition()
        wait_conditions.append(notify_condition)
        with notify_condition:
            senders[0].start()
            notify_condition.wait()

        msgs.append(listener.poll())
        self.assertEqual({'tx_id': 0}, msgs[-1].message)

        # Start the second guy, receive his message
        senders[1].start()

        msgs.append(listener.poll())
        self.assertEqual({'tx_id': 1}, msgs[-1].message)

        # Reply to both in order, making the second thread queue
        # the reply meant for the first thread
        msgs[0].reply({'rx_id': 0})
        msgs[1].reply({'rx_id': 1})

        # Wait for the second thread to finish
        senders[1].join()

        # Start the 3rd guy, receive his message
        senders[2].start()

        msgs.append(listener.poll())
        self.assertEqual({'tx_id': 2}, msgs[-1].message)

        # Verify the _send_reply was not invoked by driver:
        with mock.patch.object(msgs[2], '_send_reply') as method:
            msgs[2].reply({'rx_id': 2})
            self.assertEqual(method.call_count, 0)

        # Wait for the 3rd thread to finish
        senders[2].join()

        # Let the first thread continue
        with notify_condition:
            notify_condition.notify()

        # Wait for the first thread to finish
        senders[0].join()

        # Verify replies were received out of order
        self.assertEqual(len(senders), len(replies))
        self.assertEqual({'rx_id': 1}, replies[0])
        self.assertIsNone(replies[1])
        self.assertEqual({'rx_id': 0}, replies[2])


def _declare_queue(target):
    connection = kombu.connection.BrokerConnection(transport='memory')

    # Kludge to speed up tests.
    connection.transport.polling_interval = 0.0

    connection.connect()
    channel = connection.channel()

    # work around 'memory' transport bug in 1.1.3
    channel._new_queue('ae.undeliver')

    if target.fanout:
        exchange = kombu.entity.Exchange(name=target.topic + '_fanout',
                                         type='fanout',
                                         durable=False,
                                         auto_delete=True)
        queue = kombu.entity.Queue(name=target.topic + '_fanout_12345',
                                   channel=channel,
                                   exchange=exchange,
                                   routing_key=target.topic)
    if target.server:
        exchange = kombu.entity.Exchange(name='openstack',
                                         type='topic',
                                         durable=False,
                                         auto_delete=False)
        topic = '%s.%s' % (target.topic, target.server)
        queue = kombu.entity.Queue(name=topic,
                                   channel=channel,
                                   exchange=exchange,
                                   routing_key=topic)
    else:
        exchange = kombu.entity.Exchange(name='openstack',
                                         type='topic',
                                         durable=False,
                                         auto_delete=False)
        queue = kombu.entity.Queue(name=target.topic,
                                   channel=channel,
                                   exchange=exchange,
                                   routing_key=target.topic)

    queue.declare()

    return connection, channel, queue


class TestRequestWireFormat(test_utils.BaseTestCase):

    _target = [
        ('topic_target',
         dict(topic='testtopic', server=None, fanout=False)),
        ('server_target',
         dict(topic='testtopic', server='testserver', fanout=False)),
        # NOTE(markmc): https://github.com/celery/kombu/issues/195
        ('fanout_target',
         dict(topic='testtopic', server=None, fanout=True,
              skip_msg='Requires kombu>2.5.12 to fix kombu issue #195')),
    ]

    _msg = [
        ('empty_msg',
         dict(msg={}, expected={})),
        ('primitive_msg',
         dict(msg={'foo': 'bar'}, expected={'foo': 'bar'})),
        ('complex_msg',
         dict(msg={'a': {'b': datetime.datetime(1920, 2, 3, 4, 5, 6, 7)}},
              expected={'a': {'b': '1920-02-03T04:05:06.000007'}})),
    ]

    _context = [
        ('empty_ctxt', dict(ctxt={}, expected_ctxt={})),
        ('user_project_ctxt',
         dict(ctxt={'user': 'mark', 'project': 'snarkybunch'},
              expected_ctxt={'_context_user': 'mark',
                             '_context_project': 'snarkybunch'})),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._msg,
                                                         cls._context,
                                                         cls._target)

    def setUp(self):
        super(TestRequestWireFormat, self).setUp()
        self.uuids = []
        self.orig_uuid4 = uuid.uuid4
        self.useFixture(fixtures.MonkeyPatch('uuid.uuid4', self.mock_uuid4))

    def mock_uuid4(self):
        self.uuids.append(self.orig_uuid4())
        return self.uuids[-1]

    def test_request_wire_format(self):
        if hasattr(self, 'skip_msg'):
            self.skipTest(self.skip_msg)

        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = messaging.Target(topic=self.topic,
                                  server=self.server,
                                  fanout=self.fanout)

        connection, channel, queue = _declare_queue(target)
        self.addCleanup(connection.release)

        driver.send(target, self.ctxt, self.msg)

        msgs = []

        def callback(msg):
            msg = channel.message_to_python(msg)
            msg.ack()
            msgs.append(msg.payload)

        queue.consume(callback=callback,
                      consumer_tag='1',
                      nowait=False)

        connection.drain_events()

        self.assertEqual(1, len(msgs))
        self.assertIn('oslo.message', msgs[0])

        received = msgs[0]
        received['oslo.message'] = jsonutils.loads(received['oslo.message'])

        # FIXME(markmc): add _msg_id and _reply_q check
        expected_msg = {
            '_unique_id': self.uuids[0].hex,
        }
        expected_msg.update(self.expected)
        expected_msg.update(self.expected_ctxt)

        expected = {
            'oslo.version': '2.0',
            'oslo.message': expected_msg,
        }

        self.assertEqual(expected, received)


TestRequestWireFormat.generate_scenarios()


def _create_producer(target):
    connection = kombu.connection.BrokerConnection(transport='memory')

    # Kludge to speed up tests.
    connection.transport.polling_interval = 0.0

    connection.connect()
    channel = connection.channel()

    # work around 'memory' transport bug in 1.1.3
    channel._new_queue('ae.undeliver')

    if target.fanout:
        exchange = kombu.entity.Exchange(name=target.topic + '_fanout',
                                         type='fanout',
                                         durable=False,
                                         auto_delete=True)
        producer = kombu.messaging.Producer(exchange=exchange,
                                            channel=channel,
                                            routing_key=target.topic)
    elif target.server:
        exchange = kombu.entity.Exchange(name='openstack',
                                         type='topic',
                                         durable=False,
                                         auto_delete=False)
        topic = '%s.%s' % (target.topic, target.server)
        producer = kombu.messaging.Producer(exchange=exchange,
                                            channel=channel,
                                            routing_key=topic)
    else:
        exchange = kombu.entity.Exchange(name='openstack',
                                         type='topic',
                                         durable=False,
                                         auto_delete=False)
        producer = kombu.messaging.Producer(exchange=exchange,
                                            channel=channel,
                                            routing_key=target.topic)

    return connection, producer


class TestReplyWireFormat(test_utils.BaseTestCase):

    _target = [
        ('topic_target',
         dict(topic='testtopic', server=None, fanout=False)),
        ('server_target',
         dict(topic='testtopic', server='testserver', fanout=False)),
        # NOTE(markmc): https://github.com/celery/kombu/issues/195
        ('fanout_target',
         dict(topic='testtopic', server=None, fanout=True,
              skip_msg='Requires kombu>2.5.12 to fix kombu issue #195')),
    ]

    _msg = [
        ('empty_msg',
         dict(msg={}, expected={})),
        ('primitive_msg',
         dict(msg={'foo': 'bar'}, expected={'foo': 'bar'})),
        ('complex_msg',
         dict(msg={'a': {'b': '1920-02-03T04:05:06.000007'}},
              expected={'a': {'b': '1920-02-03T04:05:06.000007'}})),
    ]

    _context = [
        ('empty_ctxt', dict(ctxt={}, expected_ctxt={})),
        ('user_project_ctxt',
         dict(ctxt={'_context_user': 'mark',
                    '_context_project': 'snarkybunch'},
              expected_ctxt={'user': 'mark', 'project': 'snarkybunch'})),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._msg,
                                                         cls._context,
                                                         cls._target)

    def test_reply_wire_format(self):
        if hasattr(self, 'skip_msg'):
            self.skipTest(self.skip_msg)

        transport = messaging.get_transport(self.conf, 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = messaging.Target(topic=self.topic,
                                  server=self.server,
                                  fanout=self.fanout)

        listener = driver.listen(target)

        connection, producer = _create_producer(target)
        self.addCleanup(connection.release)

        msg = {
            'oslo.version': '2.0',
            'oslo.message': {}
        }

        msg['oslo.message'].update(self.msg)
        msg['oslo.message'].update(self.ctxt)

        msg['oslo.message'].update({
            '_msg_id': uuid.uuid4().hex,
            '_unique_id': uuid.uuid4().hex,
            '_reply_q': 'reply_' + uuid.uuid4().hex,
        })

        msg['oslo.message'] = jsonutils.dumps(msg['oslo.message'])

        producer.publish(msg)

        received = listener.poll()
        self.assertIsNotNone(received)
        self.assertEqual(self.expected_ctxt, received.ctxt)
        self.assertEqual(self.expected, received.message)


TestReplyWireFormat.generate_scenarios()


class RpcKombuHATestCase(test_utils.BaseTestCase):

    def setUp(self):
        super(RpcKombuHATestCase, self).setUp()
        self.brokers = ['host1', 'host2', 'host3', 'host4', 'host5']
        self.config(rabbit_hosts=self.brokers,
                    rabbit_retry_interval=0.01,
                    rabbit_retry_backoff=0.01,
                    kombu_reconnect_delay=0,
                    group="oslo_messaging_rabbit")

        self.kombu_connect = mock.Mock()
        self.useFixture(mockpatch.Patch(
            'kombu.connection.Connection.connect',
            side_effect=self.kombu_connect))
        self.useFixture(mockpatch.Patch(
            'kombu.connection.Connection.channel'))

        # starting from the first broker in the list
        url = messaging.TransportURL.parse(self.conf, None)
        self.connection = rabbit_driver.Connection(self.conf, url,
                                                   amqp.PURPOSE_SEND)
        self.addCleanup(self.connection.close)

    def test_ensure_four_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=4)
        self.assertEqual(5, self.kombu_connect.call_count)
        self.assertEqual(6, mock_callback.call_count)

    def test_ensure_one_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=1)
        self.assertEqual(2, self.kombu_connect.call_count)
        self.assertEqual(3, mock_callback.call_count)

    def test_ensure_no_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=0)
        self.assertEqual(1, self.kombu_connect.call_count)
        self.assertEqual(2, mock_callback.call_count)
