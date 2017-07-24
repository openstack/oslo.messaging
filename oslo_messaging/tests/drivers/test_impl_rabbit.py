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
import ssl
import sys
import threading
import time
import uuid

import fixtures
import kombu
import kombu.transport.memory
from oslo_config import cfg
from oslo_serialization import jsonutils
import testscenarios

import oslo_messaging
from oslo_messaging._drivers import amqpdriver
from oslo_messaging._drivers import common as driver_common
from oslo_messaging._drivers import impl_rabbit as rabbit_driver
from oslo_messaging.exceptions import MessageDeliveryFailure
from oslo_messaging.tests import utils as test_utils
from six.moves import mock

load_tests = testscenarios.load_tests_apply_scenarios


class TestDeprecatedRabbitDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestDeprecatedRabbitDriverLoad, self).setUp(
            conf=cfg.ConfigOpts())
        self.messaging_conf.transport_driver = 'rabbit'
        self.config(fake_rabbit=True, group="oslo_messaging_rabbit")

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        url = driver._get_connection()._url

        self.assertIsInstance(driver, rabbit_driver.RabbitDriver)
        self.assertEqual('memory:////', url)


class TestHeartbeat(test_utils.BaseTestCase):

    @mock.patch('oslo_messaging._drivers.impl_rabbit.LOG')
    @mock.patch('kombu.connection.Connection.heartbeat_check')
    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.'
                '_heartbeat_supported_and_enabled', return_value=True)
    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.'
                'ensure_connection')
    def _do_test_heartbeat_sent(self, fake_ensure_connection,
                                fake_heartbeat_support, fake_heartbeat,
                                fake_logger, heartbeat_side_effect=None,
                                info=None):

        event = threading.Event()

        def heartbeat_check(rate=2):
            event.set()
            if heartbeat_side_effect:
                raise heartbeat_side_effect

        fake_heartbeat.side_effect = heartbeat_check

        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        conn = transport._driver._get_connection()
        conn.ensure(method=lambda: True)
        event.wait()
        conn._heartbeat_stop()

        # check heartbeat have been called
        self.assertLess(0, fake_heartbeat.call_count)

        if not heartbeat_side_effect:
            self.assertEqual(1, fake_ensure_connection.call_count)
            self.assertEqual(2, fake_logger.debug.call_count)
            self.assertEqual(0, fake_logger.info.call_count)
        else:
            self.assertEqual(2, fake_ensure_connection.call_count)
            self.assertEqual(2, fake_logger.debug.call_count)
            self.assertEqual(1, fake_logger.info.call_count)
            self.assertIn(mock.call(info, mock.ANY),
                          fake_logger.info.mock_calls)

    def test_test_heartbeat_sent_default(self):
        self._do_test_heartbeat_sent()

    def test_test_heartbeat_sent_connection_fail(self):
        self._do_test_heartbeat_sent(
            heartbeat_side_effect=kombu.exceptions.OperationalError,
            info='A recoverable connection/channel error occurred, '
            'trying to reconnect: %s')


class TestRabbitQos(test_utils.BaseTestCase):

    def connection_with(self, prefetch, purpose):
        self.config(rabbit_qos_prefetch_count=prefetch,
                    group="oslo_messaging_rabbit")
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        transport._driver._get_connection(purpose)

    @mock.patch('kombu.transport.memory.Channel.basic_qos')
    def test_qos_sent_on_listen_connection(self, fake_basic_qos):
        self.connection_with(prefetch=1, purpose=driver_common.PURPOSE_LISTEN)
        fake_basic_qos.assert_called_once_with(0, 1, False)

    @mock.patch('kombu.transport.memory.Channel.basic_qos')
    def test_qos_not_sent_when_cfg_zero(self, fake_basic_qos):
        self.connection_with(prefetch=0, purpose=driver_common.PURPOSE_LISTEN)
        fake_basic_qos.assert_not_called()

    @mock.patch('kombu.transport.memory.Channel.basic_qos')
    def test_qos_not_sent_on_send_connection(self, fake_basic_qos):
        self.connection_with(prefetch=1, purpose=driver_common.PURPOSE_SEND)
        fake_basic_qos.assert_not_called()


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
        self.config(heartbeat_timeout_threshold=60,
                    group='oslo_messaging_rabbit')
        self.messaging_conf.transport_driver = self.transport_driver
        transport = oslo_messaging.get_transport(self.conf)
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        url = driver._get_connection()._url

        self.assertIsInstance(driver, rabbit_driver.RabbitDriver)
        self.assertEqual(self.url, url)


class TestRabbitDriverLoadSSL(test_utils.BaseTestCase):
    scenarios = [
        ('no_ssl', dict(options=dict(), expected=False)),
        ('no_ssl_with_options', dict(options=dict(ssl_version='TLSv1'),
                                     expected=False)),
        ('just_ssl', dict(options=dict(ssl=True),
                          expected=True)),
        ('ssl_with_options', dict(options=dict(ssl=True,
                                               ssl_version='TLSv1',
                                               ssl_key_file='foo',
                                               ssl_cert_file='bar',
                                               ssl_ca_file='foobar'),
                                  expected=dict(ssl_version=3,
                                                keyfile='foo',
                                                certfile='bar',
                                                ca_certs='foobar',
                                                cert_reqs=ssl.CERT_REQUIRED))),
    ]

    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.ensure')
    @mock.patch('kombu.connection.Connection')
    def test_driver_load(self, connection_klass, fake_ensure):
        self.config(group="oslo_messaging_rabbit", **self.options)
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        connection = transport._driver._get_connection()
        connection_klass.assert_called_once_with(
            'memory:///', transport_options={
                'client_properties': {
                    'capabilities': {
                        'connection.blocked': True,
                        'consumer_cancel_notify': True,
                        'authentication_failure_close': True,
                    },
                    'connection_name': connection.name},
                'confirm_publish': True,
                'on_blocked': mock.ANY,
                'on_unblocked': mock.ANY},
            ssl=self.expected, login_method='AMQPLAIN',
            heartbeat=60, failover_strategy='round-robin'
        )


class TestRabbitPublisher(test_utils.BaseTestCase):
    @mock.patch('kombu.messaging.Producer.publish')
    def test_send_with_timeout(self, fake_publish):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        exchange_mock = mock.Mock()
        with transport._driver._get_connection(
                driver_common.PURPOSE_SEND) as pool_conn:
            conn = pool_conn.connection
            conn._publish(exchange_mock, 'msg', routing_key='routing_key',
                          timeout=1)

        fake_publish.assert_called_with(
            'msg', expiration=1,
            exchange=exchange_mock,
            compression=self.conf.oslo_messaging_rabbit.kombu_compression,
            routing_key='routing_key')

    @mock.patch('kombu.messaging.Producer.publish')
    def test_send_no_timeout(self, fake_publish):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        exchange_mock = mock.Mock()
        with transport._driver._get_connection(
                driver_common.PURPOSE_SEND) as pool_conn:
            conn = pool_conn.connection
            conn._publish(exchange_mock, 'msg', routing_key='routing_key')
        fake_publish.assert_called_with(
            'msg', expiration=None,
            compression=self.conf.oslo_messaging_rabbit.kombu_compression,
            exchange=exchange_mock,
            routing_key='routing_key')

    def test_declared_queue_publisher(self):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        e_passive = kombu.entity.Exchange(
            name='foobar',
            type='topic',
            passive=True)

        e_active = kombu.entity.Exchange(
            name='foobar',
            type='topic',
            passive=False)

        with transport._driver._get_connection(
                driver_common.PURPOSE_SEND) as pool_conn:
            conn = pool_conn.connection
            exc = conn.connection.channel_errors[0]

            def try_send(exchange):
                conn._ensure_publishing(
                    conn._publish_and_creates_default_queue,
                    exchange, {}, routing_key='foobar')

            with mock.patch('kombu.transport.virtual.Channel.close'):
                # Ensure the exchange does not exists
                self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                                  try_send, e_passive)
                # Create it
                try_send(e_active)
                # Ensure it creates it
                try_send(e_passive)

            with mock.patch('kombu.messaging.Producer.publish',
                            side_effect=exc):
                with mock.patch('kombu.transport.virtual.Channel.close'):
                    # Ensure the exchange is already in cache
                    self.assertIn('foobar', conn._declared_exchanges)
                    # Reset connection
                    self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                                      try_send, e_passive)
                    # Ensure the cache is empty
                    self.assertEqual(0, len(conn._declared_exchanges))

            try_send(e_active)
            self.assertIn('foobar', conn._declared_exchanges)

    def test_send_exception_remap(self):
        bad_exc = Exception("Non-oslo.messaging exception")
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        exchange_mock = mock.Mock()
        with transport._driver._get_connection(
                driver_common.PURPOSE_SEND) as pool_conn:
            conn = pool_conn.connection
            with mock.patch('kombu.messaging.Producer.publish',
                            side_effect=bad_exc):
                self.assertRaises(MessageDeliveryFailure,
                                  conn._ensure_publishing,
                                  conn._publish, exchange_mock, 'msg')


class TestRabbitConsume(test_utils.BaseTestCase):

    def test_consume_timeout(self):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        deadline = time.time() + 6
        with transport._driver._get_connection(
                driver_common.PURPOSE_LISTEN) as conn:
            self.assertRaises(driver_common.Timeout,
                              conn.consume, timeout=3)

            # kombu memory transport doesn't really raise error
            # so just simulate a real driver behavior
            conn.connection.connection.recoverable_channel_errors = (IOError,)
            conn.declare_fanout_consumer("notif.info", lambda msg: True)
            with mock.patch('kombu.connection.Connection.drain_events',
                            side_effect=IOError):
                self.assertRaises(driver_common.Timeout,
                                  conn.consume, timeout=3)

        self.assertEqual(0, int(deadline - time.time()))

    def test_consume_from_missing_queue(self):
        transport = oslo_messaging.get_transport(self.conf, 'kombu+memory://')
        self.addCleanup(transport.cleanup)
        with transport._driver._get_connection(
                driver_common.PURPOSE_LISTEN) as conn:
            with mock.patch('kombu.Queue.consume') as consume, mock.patch(
                    'kombu.Queue.declare') as declare:
                conn.declare_topic_consumer(exchange_name='test',
                                            topic='test',
                                            callback=lambda msg: True)
                import amqp
                consume.side_effect = [amqp.NotFound, None]
                conn.connection.connection.recoverable_connection_errors = ()
                conn.connection.connection.recoverable_channel_errors = ()
                self.assertEqual(1, declare.call_count)
                conn.connection.connection.drain_events = mock.Mock()
                # Ensure that a queue will be re-declared if the consume method
                # of kombu.Queue raise amqp.NotFound
                conn.consume()
                self.assertEqual(2, declare.call_count)

    def test_consume_from_missing_queue_with_io_error_on_redeclaration(self):
        transport = oslo_messaging.get_transport(self.conf, 'kombu+memory://')
        self.addCleanup(transport.cleanup)
        with transport._driver._get_connection(
                driver_common.PURPOSE_LISTEN) as conn:
            with mock.patch('kombu.Queue.consume') as consume, mock.patch(
                    'kombu.Queue.declare') as declare:
                conn.declare_topic_consumer(exchange_name='test',
                                            topic='test',
                                            callback=lambda msg: True)
                import amqp
                consume.side_effect = [amqp.NotFound, None]
                declare.side_effect = [IOError, None]

                conn.connection.connection.recoverable_connection_errors = (
                    IOError,)
                conn.connection.connection.recoverable_channel_errors = ()
                self.assertEqual(1, declare.call_count)
                conn.connection.connection.drain_events = mock.Mock()
                # Ensure that a queue will be re-declared after
                # 'queue not found' exception despite on connection error.
                conn.consume()
                self.assertEqual(3, declare.call_count)

    def test_connection_ack_have_disconnected_kombu_connection(self):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        with transport._driver._get_connection(
                driver_common.PURPOSE_LISTEN) as conn:
            channel = conn.connection.channel
            with mock.patch('kombu.connection.Connection.connected',
                            new_callable=mock.PropertyMock,
                            return_value=False):
                self.assertRaises(driver_common.Timeout,
                                  conn.connection.consume, timeout=0.01)
                # Ensure a new channel have been setuped
                self.assertNotEqual(channel, conn.connection.channel)


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
        ('rabbit_ipv6',
         dict(url='rabbit://u:p@[fd00:beef:dead:55::133]:10/vhost',
              expected=['amqp://u:p@[fd00:beef:dead:55::133]:10/vhost'])),
        ('rabbit_ipv4',
         dict(url='rabbit://user:password@10.20.30.40:10/vhost',
              expected=['amqp://user:password@10.20.30.40:10/vhost'])),
    ]

    def setUp(self):
        super(TestRabbitTransportURL, self).setUp()
        self.messaging_conf.transport_driver = 'rabbit'
        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')

    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.ensure')
    @mock.patch('oslo_messaging._drivers.impl_rabbit.Connection.reset')
    def test_transport_url(self, fake_reset, fake_ensure):
        transport = oslo_messaging.get_transport(self.conf, self.url)
        self.addCleanup(transport.cleanup)
        driver = transport._driver

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
        self.config(kombu_missing_consumer_retry_timeout=0.5,
                    group="oslo_messaging_rabbit")
        self.config(heartbeat_timeout_threshold=0,
                    group="oslo_messaging_rabbit")
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = oslo_messaging.Target(topic='testtopic')

        listener = driver.listen(target, None, None)._poll_style_listener

        senders = []
        replies = []
        msgs = []

        def send_and_wait_for_reply(i):
            try:

                timeout = self.timeout
                replies.append(driver.send(target,
                                           self.ctxt,
                                           {'tx_id': i},
                                           wait_for_reply=True,
                                           timeout=timeout))
                self.assertFalse(self.failure)
                self.assertIsNone(self.timeout)
            except (ZeroDivisionError, oslo_messaging.MessagingTimeout) as e:
                replies.append(e)
                self.assertTrue(self.failure or self.timeout is not None)

        while len(senders) < self.n_senders:
            senders.append(threading.Thread(target=send_and_wait_for_reply,
                                            args=(len(senders), )))

        for i in range(len(senders)):
            senders[i].start()

            received = listener.poll()[0]
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
                    msgs[i].reply(failure=failure)
                elif self.rx_id:
                    msgs[i].reply({'rx_id': i})
                else:
                    msgs[i].reply(self.reply)
            senders[i].join()

        self.assertEqual(len(senders), len(replies))
        for i, reply in enumerate(replies):
            if self.timeout is not None:
                self.assertIsInstance(reply, oslo_messaging.MessagingTimeout)
            elif self.failure:
                self.assertIsInstance(reply, ZeroDivisionError)
            elif self.rx_id:
                self.assertEqual({'rx_id': order[i]}, reply)
            else:
                self.assertEqual(self.reply, reply)


TestSendReceive.generate_scenarios()


class TestPollAsync(test_utils.BaseTestCase):

    def test_poll_timeout(self):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)
        driver = transport._driver
        target = oslo_messaging.Target(topic='testtopic')
        listener = driver.listen(target, None, None)._poll_style_listener
        received = listener.poll(timeout=0.050)
        self.assertEqual([], received)


class TestRacyWaitForReply(test_utils.BaseTestCase):

    def test_send_receive(self):
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = oslo_messaging.Target(topic='testtopic')

        listener = driver.listen(target, None, None)._poll_style_listener
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

        self.useFixture(fixtures.MockPatchObject(
            amqpdriver.ReplyWaiter, 'wait', reply_waiter))

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

        msgs.extend(listener.poll())
        self.assertEqual({'tx_id': 0}, msgs[-1].message)

        # Start the second guy, receive his message
        senders[1].start()

        msgs.extend(listener.poll())
        self.assertEqual({'tx_id': 1}, msgs[-1].message)

        # Reply to both in order, making the second thread queue
        # the reply meant for the first thread
        msgs[0].reply({'rx_id': 0})
        msgs[1].reply({'rx_id': 1})

        # Wait for the second thread to finish
        senders[1].join()

        # Start the 3rd guy, receive his message
        senders[2].start()

        msgs.extend(listener.poll())
        self.assertEqual({'tx_id': 2}, msgs[-1].message)

        # Verify the _send_reply was not invoked by driver:
        with mock.patch.object(msgs[2], '_send_reply') as method:
            msgs[2].reply({'rx_id': 2})
            self.assertEqual(0, method.call_count)

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
    elif target.server:
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
        ('fanout_target',
         dict(topic='testtopic', server=None, fanout=True)),
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

    _compression = [
        ('gzip_compression', dict(compression='gzip')),
        ('without_compression', dict(compression=None))
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._msg,
                                                         cls._context,
                                                         cls._target,
                                                         cls._compression)

    def setUp(self):
        super(TestRequestWireFormat, self).setUp()
        self.uuids = []
        self.orig_uuid4 = uuid.uuid4
        self.useFixture(fixtures.MonkeyPatch('uuid.uuid4', self.mock_uuid4))

    def mock_uuid4(self):
        self.uuids.append(self.orig_uuid4())
        return self.uuids[-1]

    def test_request_wire_format(self):
        self.conf.oslo_messaging_rabbit.kombu_compression = self.compression
        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = oslo_messaging.Target(topic=self.topic,
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
        ('fanout_target',
         dict(topic='testtopic', server=None, fanout=True)),
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

    _compression = [
        ('gzip_compression', dict(compression='gzip')),
        ('without_compression', dict(compression=None))
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._msg,
                                                         cls._context,
                                                         cls._target,
                                                         cls._compression)

    def test_reply_wire_format(self):
        self.conf.oslo_messaging_rabbit.kombu_compression = self.compression

        transport = oslo_messaging.get_transport(self.conf,
                                                 'kombu+memory:////')
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = oslo_messaging.Target(topic=self.topic,
                                       server=self.server,
                                       fanout=self.fanout)

        listener = driver.listen(target, None, None)._poll_style_listener

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

        received = listener.poll()[0]
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
                    heartbeat_timeout_threshold=0,
                    group="oslo_messaging_rabbit")

        self.useFixture(fixtures.MockPatch(
            'kombu.connection.Connection.connection'))
        self.useFixture(fixtures.MockPatch(
            'kombu.connection.Connection.channel'))

        # starting from the first broker in the list
        url = oslo_messaging.TransportURL.parse(self.conf, None)
        self.connection = rabbit_driver.Connection(self.conf, url,
                                                   driver_common.PURPOSE_SEND)
        self.useFixture(fixtures.MockPatch(
            'kombu.connection.Connection.connect'))
        self.addCleanup(self.connection.close)

    def test_ensure_four_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=4)
        self.assertEqual(6, mock_callback.call_count)

    def test_ensure_one_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=1)
        self.assertEqual(3, mock_callback.call_count)

    def test_ensure_no_retry(self):
        mock_callback = mock.Mock(side_effect=IOError)
        self.assertRaises(oslo_messaging.MessageDeliveryFailure,
                          self.connection.ensure, mock_callback,
                          retry=0)
        self.assertEqual(2, mock_callback.call_count)


class ConnectionLockTestCase(test_utils.BaseTestCase):
    def _thread(self, lock, sleep, heartbeat=False):
        def thread_task():
            if heartbeat:
                with lock.for_heartbeat():
                    time.sleep(sleep)
            else:
                with lock:
                    time.sleep(sleep)

        t = threading.Thread(target=thread_task)
        t.daemon = True
        t.start()
        start = time.time()

        def get_elapsed_time():
            t.join()
            return time.time() - start

        return get_elapsed_time

    def test_workers_only(self):
        l = rabbit_driver.ConnectionLock()
        t1 = self._thread(l, 1)
        t2 = self._thread(l, 1)
        self.assertAlmostEqual(1, t1(), places=0)
        self.assertAlmostEqual(2, t2(), places=0)

    def test_worker_and_heartbeat(self):
        l = rabbit_driver.ConnectionLock()
        t1 = self._thread(l, 1)
        t2 = self._thread(l, 1, heartbeat=True)
        self.assertAlmostEqual(1, t1(), places=0)
        self.assertAlmostEqual(2, t2(), places=0)

    def test_workers_and_heartbeat(self):
        l = rabbit_driver.ConnectionLock()
        t1 = self._thread(l, 1)
        t2 = self._thread(l, 1)
        t3 = self._thread(l, 1)
        t4 = self._thread(l, 1, heartbeat=True)
        t5 = self._thread(l, 1)
        self.assertAlmostEqual(1, t1(), places=0)
        self.assertAlmostEqual(2, t4(), places=0)
        self.assertAlmostEqual(3, t2(), places=0)
        self.assertAlmostEqual(4, t3(), places=0)
        self.assertAlmostEqual(5, t5(), places=0)

    def test_heartbeat(self):
        l = rabbit_driver.ConnectionLock()
        t1 = self._thread(l, 1, heartbeat=True)
        t2 = self._thread(l, 1)
        self.assertAlmostEqual(1, t1(), places=0)
        self.assertAlmostEqual(2, t2(), places=0)
