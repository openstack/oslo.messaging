
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
import logging
import sys
import uuid

import fixtures
import mock
import testscenarios

from oslo import messaging
from oslo.messaging.notify import _impl_log
from oslo.messaging.notify import _impl_messaging
from oslo.messaging.notify import _impl_test
from oslo.messaging.notify import notifier as msg_notifier
from oslo.messaging.openstack.common import jsonutils
from oslo.messaging.openstack.common import timeutils
from oslo.messaging import serializer as msg_serializer
from tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class _FakeTransport(object):

    def __init__(self, conf):
        self.conf = conf

    def _send_notification(self, target, ctxt, message, version):
        pass


class _ReRaiseLoggedExceptionsFixture(fixtures.Fixture):

    """Record logged exceptions and re-raise in cleanup.

    The notifier just logs notification send errors so, for the sake of
    debugging test failures, we record any exceptions logged and re-raise them
    during cleanup.
    """

    class FakeLogger(object):

        def __init__(self):
            self.exceptions = []

        def exception(self, msg, *args, **kwargs):
            self.exceptions.append(sys.exc_info()[1])

    def setUp(self):
        super(_ReRaiseLoggedExceptionsFixture, self).setUp()

        self.logger = self.FakeLogger()

        def reraise_exceptions():
            for ex in self.logger.exceptions:
                raise ex

        self.addCleanup(reraise_exceptions)


class TestMessagingNotifier(test_utils.BaseTestCase):

    _v1 = [
        ('v1', dict(v1=True)),
        ('not_v1', dict(v1=False)),
    ]

    _v2 = [
        ('v2', dict(v2=True)),
        ('not_v2', dict(v2=False)),
    ]

    _publisher_id = [
        ('ctor_pub_id', dict(ctor_pub_id='test',
                             expected_pub_id='test')),
        ('prep_pub_id', dict(prep_pub_id='test.localhost',
                             expected_pub_id='test.localhost')),
        ('override', dict(ctor_pub_id='test',
                          prep_pub_id='test.localhost',
                          expected_pub_id='test.localhost')),
    ]

    _topics = [
        ('no_topics', dict(topics=[])),
        ('single_topic', dict(topics=['notifications'])),
        ('multiple_topic2', dict(topics=['foo', 'bar'])),
    ]

    _priority = [
        ('audit', dict(priority='audit')),
        ('debug', dict(priority='debug')),
        ('info', dict(priority='info')),
        ('warn', dict(priority='warn')),
        ('error', dict(priority='error')),
        ('sample', dict(priority='sample')),
        ('critical', dict(priority='critical')),
    ]

    _payload = [
        ('payload', dict(payload={'foo': 'bar'})),
    ]

    _context = [
        ('ctxt', dict(ctxt={'user': 'bob'})),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._v1,
                                                         cls._v2,
                                                         cls._publisher_id,
                                                         cls._topics,
                                                         cls._priority,
                                                         cls._payload,
                                                         cls._context)

    def setUp(self):
        super(TestMessagingNotifier, self).setUp()

        self.conf.register_opts(msg_notifier._notifier_opts)

        self.logger = self.useFixture(_ReRaiseLoggedExceptionsFixture()).logger
        self.stubs.Set(_impl_messaging, 'LOG', self.logger)
        self.stubs.Set(msg_notifier, '_LOG', self.logger)

    @mock.patch('oslo.messaging.openstack.common.timeutils.utcnow')
    def test_notifier(self, mock_utcnow):
        drivers = []
        if self.v1:
            drivers.append('messaging')
        if self.v2:
            drivers.append('messagingv2')

        self.config(notification_driver=drivers)
        self.config(notification_topics=self.topics)

        transport = _FakeTransport(self.conf)

        if hasattr(self, 'ctor_pub_id'):
            notifier = messaging.Notifier(transport,
                                          publisher_id=self.ctor_pub_id)
        else:
            notifier = messaging.Notifier(transport)

        if hasattr(self, 'prep_pub_id'):
            notifier = notifier.prepare(publisher_id=self.prep_pub_id)

        self.mox.StubOutWithMock(transport, '_send_notification')

        message_id = uuid.uuid4()
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn(message_id)

        mock_utcnow.return_value = datetime.datetime.utcnow()

        message = {
            'message_id': str(message_id),
            'publisher_id': self.expected_pub_id,
            'event_type': 'test.notify',
            'priority': self.priority.upper(),
            'payload': self.payload,
            'timestamp': str(timeutils.utcnow()),
        }

        sends = []
        if self.v1:
            sends.append(dict(version=1.0))
        if self.v2:
            sends.append(dict(version=2.0))

        for send_kwargs in sends:
            for topic in self.topics:
                target = messaging.Target(topic='%s.%s' % (topic,
                                                           self.priority))
                transport._send_notification(target, self.ctxt, message,
                                             **send_kwargs)

        self.mox.ReplayAll()

        method = getattr(notifier, self.priority)
        method(self.ctxt, 'test.notify', self.payload)


TestMessagingNotifier.generate_scenarios()


class TestSerializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSerializer, self).setUp()
        self.addCleanup(_impl_test.reset)

    @mock.patch('oslo.messaging.openstack.common.timeutils.utcnow')
    def test_serializer(self, mock_utcnow):
        transport = _FakeTransport(self.conf)

        serializer = msg_serializer.NoOpSerializer()

        notifier = messaging.Notifier(transport,
                                      'test.localhost',
                                      driver='test',
                                      topic='test',
                                      serializer=serializer)

        message_id = uuid.uuid4()
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn(message_id)

        mock_utcnow.return_value = datetime.datetime.utcnow()

        self.mox.StubOutWithMock(serializer, 'serialize_context')
        self.mox.StubOutWithMock(serializer, 'serialize_entity')
        serializer.serialize_context(dict(user='bob')).\
            AndReturn(dict(user='alice'))
        serializer.serialize_entity(dict(user='bob'), 'bar').AndReturn('sbar')

        self.mox.ReplayAll()

        notifier.info(dict(user='bob'), 'test.notify', 'bar')

        message = {
            'message_id': str(message_id),
            'publisher_id': 'test.localhost',
            'event_type': 'test.notify',
            'priority': 'INFO',
            'payload': 'sbar',
            'timestamp': str(timeutils.utcnow()),
        }

        self.assertEqual(_impl_test.NOTIFICATIONS,
                         [(dict(user='alice'), message, 'INFO')])


class TestLogNotifier(test_utils.BaseTestCase):

    def setUp(self):
        super(TestLogNotifier, self).setUp()
        self.conf.register_opts(msg_notifier._notifier_opts)

    @mock.patch('oslo.messaging.openstack.common.timeutils.utcnow')
    def test_notifier(self, mock_utcnow):
        self.config(notification_driver=['log'])

        transport = _FakeTransport(self.conf)

        notifier = messaging.Notifier(transport, 'test.localhost')

        message_id = uuid.uuid4()
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn(message_id)

        mock_utcnow.return_value = datetime.datetime.utcnow()

        message = {
            'message_id': str(message_id),
            'publisher_id': 'test.localhost',
            'event_type': 'test.notify',
            'priority': 'INFO',
            'payload': 'bar',
            'timestamp': str(timeutils.utcnow()),
        }

        logger = self.mox.CreateMockAnything()

        self.mox.StubOutWithMock(logging, 'getLogger')
        logging.getLogger('oslo.messaging.notification.test.notify').\
            AndReturn(logger)

        logger.info(jsonutils.dumps(message))

        self.mox.ReplayAll()

        notifier.info({}, 'test.notify', 'bar')

    def test_sample_priority(self):
        # Ensure logger drops sample-level notifications.
        driver = _impl_log.LogDriver(None, None, None)

        logger = self.mox.CreateMock(
            logging.getLogger('oslo.messaging.notification.foo'))
        logger.sample = None
        self.mox.StubOutWithMock(logging, 'getLogger')
        logging.getLogger('oslo.messaging.notification.foo').\
            AndReturn(logger)

        self.mox.ReplayAll()

        msg = {'event_type': 'foo'}
        driver.notify(None, msg, "sample")
