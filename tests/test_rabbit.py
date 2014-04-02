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
import uuid

import fixtures
import kombu
import testscenarios

from oslo import messaging
from oslo.messaging._drivers import amqpdriver
from oslo.messaging._drivers import common as driver_common
from oslo.messaging._drivers import impl_rabbit as rabbit_driver
from oslo.messaging.openstack.common import jsonutils
from tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class TestRabbitDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRabbitDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

    def test_driver_load(self):
        transport = messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, rabbit_driver.RabbitDriver)


class TestRabbitTransportURL(test_utils.BaseTestCase):

    scenarios = [
        ('none', dict(url=None, expected=None)),
        ('empty',
         dict(url='rabbit:///',
              expected=dict(virtual_host=''))),
        ('localhost',
         dict(url='rabbit://localhost/',
              expected=dict(hostname='localhost',
                            username='',
                            password='',
                            virtual_host=''))),
        ('virtual_host',
         dict(url='rabbit:///vhost',
              expected=dict(virtual_host='vhost'))),
        ('no_creds',
         dict(url='rabbit://host/virtual_host',
              expected=dict(hostname='host',
                            username='',
                            password='',
                            virtual_host='virtual_host'))),
        ('no_port',
         dict(url='rabbit://user:password@host/virtual_host',
              expected=dict(hostname='host',
                            username='user',
                            password='password',
                            virtual_host='virtual_host'))),
        ('full_url',
         dict(url='rabbit://user:password@host:10/virtual_host',
              expected=dict(hostname='host',
                            port=10,
                            username='user',
                            password='password',
                            virtual_host='virtual_host'))),
    ]

    def setUp(self):
        super(TestRabbitTransportURL, self).setUp()

        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

        self._server_params = []
        cnx_init = rabbit_driver.Connection.__init__

        def record_params(cnx, conf, server_params=None):
            self._server_params.append(server_params)
            return cnx_init(cnx, conf, server_params)

        def dummy_send(cnx, topic, msg, timeout=None):
            pass

        self.stubs.Set(rabbit_driver.Connection, '__init__', record_params)
        self.stubs.Set(rabbit_driver.Connection, 'topic_send', dummy_send)

        self._driver = messaging.get_transport(self.conf, self.url)._driver
        self._target = messaging.Target(topic='testtopic')

    def test_transport_url_listen(self):
        self._driver.listen(self._target)
        self.assertEqual(self.expected, self._server_params[0])

    def test_transport_url_listen_for_notification(self):
        self._driver.listen_for_notifications(
            [(messaging.Target(topic='topic'), 'info')])
        self.assertEqual(self.expected, self._server_params[0])

    def test_transport_url_send(self):
        self._driver.send(self._target, {}, {})
        self.assertEqual(self.expected, self._server_params[0])


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

    def setUp(self):
        super(TestSendReceive, self).setUp()
        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

    def test_send_receive(self):
        transport = messaging.get_transport(self.conf)
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
        order = list(range(len(senders)-1, -1, -1))
        if len(order) > 1:
            order[-1], order[-2] = order[-2], order[-1]

        for i in order:
            if self.timeout is None:
                if self.failure:
                    try:
                        raise ZeroDivisionError
                    except Exception:
                        failure = sys.exc_info()
                    msgs[i].reply(failure=failure,
                                  log_failure=not self.expected)
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


class TestRacyWaitForReply(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRacyWaitForReply, self).setUp()
        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

    def test_send_receive(self):
        transport = messaging.get_transport(self.conf)
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
                with wait_conditions[0]:
                    wait_conditions.pop().wait()
            return orig_reply_waiter(self, msg_id, timeout)

        self.stubs.Set(amqpdriver.ReplyWaiter, 'wait', reply_waiter)

        def send_and_wait_for_reply(i):
            replies.append(driver.send(target,
                                       {},
                                       {'tx_id': i},
                                       wait_for_reply=True,
                                       timeout=None))

        while len(senders) < 2:
            t = threading.Thread(target=send_and_wait_for_reply,
                                 args=(len(senders), ))
            t.daemon = True
            senders.append(t)

        # Start the first guy, receive his message, but delay his polling
        notify_condition = threading.Condition()
        wait_conditions.append(notify_condition)
        senders[0].start()

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

        # Let the first thread continue
        with notify_condition:
            notify_condition.notify()

        # Wait for the first thread to finish
        senders[0].join()

        # Verify replies were received out of order
        self.assertEqual(len(senders), len(replies))
        self.assertEqual({'rx_id': 1}, replies[0])
        self.assertEqual({'rx_id': 0}, replies[1])


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
        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

        self.uuids = []
        self.orig_uuid4 = uuid.uuid4
        self.useFixture(fixtures.MonkeyPatch('uuid.uuid4', self.mock_uuid4))

    def mock_uuid4(self):
        self.uuids.append(self.orig_uuid4())
        return self.uuids[-1]

    def test_request_wire_format(self):
        if hasattr(self, 'skip_msg'):
            self.skipTest(self.skip_msg)

        transport = messaging.get_transport(self.conf)
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

    def setUp(self):
        super(TestReplyWireFormat, self).setUp()
        self.messaging_conf.transport_driver = 'rabbit'
        self.messaging_conf.in_memory = True

    def test_reply_wire_format(self):
        if hasattr(self, 'skip_msg'):
            self.skipTest(self.skip_msg)

        transport = messaging.get_transport(self.conf)
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

    def test_reconnect_order(self):
        brokers = ['host1', 'host2', 'host3', 'host4', 'host5']
        brokers_count = len(brokers)

        self.conf.rabbit_hosts = brokers
        self.conf.rabbit_max_retries = 1

        info = {'attempt': 0}

        def _connect(myself, params):
            # do as little work that is enough to pass connection attempt
            myself.connection = kombu.connection.BrokerConnection(**params)
            myself.connection_errors = myself.connection.connection_errors

            expected_broker = brokers[info['attempt'] % brokers_count]
            self.assertEqual(expected_broker, params['hostname'])

            info['attempt'] += 1

        # just make sure connection instantiation does not fail with an
        # exception
        self.stubs.Set(rabbit_driver.Connection, '_connect', _connect)

        # starting from the first broker in the list
        connection = rabbit_driver.Connection(self.conf)

        # now that we have connection object, revert to the real 'connect'
        # implementation
        self.stubs.UnsetAll()

        for i in range(len(brokers)):
            self.assertRaises(driver_common.RPCException, connection.reconnect)

        connection.close()
