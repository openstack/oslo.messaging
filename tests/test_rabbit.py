# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
        ('empty', dict(url='rabbit:///', expected=None)),
        ('localhost',
         dict(url='rabbit://localhost/',
              expected=dict(hostname='localhost',
                            username='',
                            password='',
                            virtual_host=''))),
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

    def test_transport_url(self):
        cnx_init = rabbit_driver.Connection.__init__
        passed_params = []

        def record_params(self, conf, server_params=None):
            passed_params.append(server_params)
            return cnx_init(self, conf, server_params)

        self.stubs.Set(rabbit_driver.Connection, '__init__', record_params)

        transport = messaging.get_transport(self.conf, self.url)

        driver = transport._driver

        target = messaging.Target(topic='testtopic')

        driver.send(target, {}, {})

        self.assertEquals(passed_params[0], self.expected)


class TestSendReceive(test_utils.BaseTestCase):

    _n_senders = [
        ('single_sender', dict(n_senders=1)),
        ('multiple_senders', dict(n_senders=10)),
    ]

    _context = [
        ('empty_context', dict(ctxt={})),
        ('with_context', dict(ctxt={'user': 'mark'})),
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
                                           {'foo': i},
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
            self.assertEqual(received.ctxt, self.ctxt)
            self.assertEqual(received.message, {'foo': i})
            msgs.append(received)

        # reply in reverse, except reply to the first guy second from last
        order = range(len(senders)-1, -1, -1)
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
                else:
                    msgs[i].reply({'bar': msgs[i].message['foo']})
            senders[i].join()

        self.assertEqual(len(replies), len(senders))
        for i, reply in enumerate(replies):
            if self.timeout is not None:
                self.assertIsInstance(reply, messaging.MessagingTimeout)
            elif self.failure:
                self.assertIsInstance(reply, ZeroDivisionError)
            else:
                self.assertEqual(reply, {'bar': order[i]})

        if not self.timeout and self.failure and not self.expected:
            self.assertTrue(len(errors) > 0, errors)
        else:
            self.assertEqual(len(errors), 0, errors)


TestSendReceive.generate_scenarios()


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

        expected_msg = {
            '_msg_id': self.uuids[0].hex,
            '_unique_id': self.uuids[1].hex,
            '_reply_q': 'reply_' + self.uuids[2].hex,
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
