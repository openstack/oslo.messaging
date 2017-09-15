
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
from oslo_serialization import jsonutils
from oslo_utils import strutils
from oslo_utils import timeutils
from stevedore import dispatch
from stevedore import extension
import testscenarios
import yaml

import oslo_messaging
from oslo_messaging.notify import _impl_log
from oslo_messaging.notify import _impl_test
from oslo_messaging.notify import messaging
from oslo_messaging.notify import notifier as msg_notifier
from oslo_messaging import serializer as msg_serializer
from oslo_messaging.tests import utils as test_utils
from six.moves import mock

load_tests = testscenarios.load_tests_apply_scenarios


class JsonMessageMatcher(object):
    def __init__(self, message):
        self.message = message

    def __eq__(self, other):
        return self.message == jsonutils.loads(other)


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

        def warning(self, msg, *args, **kwargs):
            return

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

    _retry = [
        ('unconfigured', dict()),
        ('None', dict(retry=None)),
        ('0', dict(retry=0)),
        ('5', dict(retry=5)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._v1,
                                                         cls._v2,
                                                         cls._publisher_id,
                                                         cls._topics,
                                                         cls._priority,
                                                         cls._payload,
                                                         cls._context,
                                                         cls._retry)

    def setUp(self):
        super(TestMessagingNotifier, self).setUp()

        self.logger = self.useFixture(_ReRaiseLoggedExceptionsFixture()).logger
        self.useFixture(fixtures.MockPatchObject(
            messaging, 'LOG', self.logger))
        self.useFixture(fixtures.MockPatchObject(
            msg_notifier, '_LOG', self.logger))

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_notifier(self, mock_utcnow):
        drivers = []
        if self.v1:
            drivers.append('messaging')
        if self.v2:
            drivers.append('messagingv2')

        self.config(driver=drivers,
                    topics=self.topics,
                    group='oslo_messaging_notifications')

        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        if hasattr(self, 'ctor_pub_id'):
            notifier = oslo_messaging.Notifier(transport,
                                               publisher_id=self.ctor_pub_id)
        else:
            notifier = oslo_messaging.Notifier(transport)

        prepare_kwds = {}
        if hasattr(self, 'retry'):
            prepare_kwds['retry'] = self.retry
        if hasattr(self, 'prep_pub_id'):
            prepare_kwds['publisher_id'] = self.prep_pub_id
        if prepare_kwds:
            notifier = notifier.prepare(**prepare_kwds)

        transport._send_notification = mock.Mock()

        message_id = uuid.uuid4()
        uuid.uuid4 = mock.Mock(return_value=message_id)

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

        calls = []
        for send_kwargs in sends:
            for topic in self.topics:
                if hasattr(self, 'retry'):
                    send_kwargs['retry'] = self.retry
                else:
                    send_kwargs['retry'] = -1
                target = oslo_messaging.Target(topic='%s.%s' % (topic,
                                                                self.priority))
                calls.append(mock.call(target,
                                       self.ctxt,
                                       message,
                                       **send_kwargs))

        method = getattr(notifier, self.priority)
        method(self.ctxt, 'test.notify', self.payload)

        uuid.uuid4.assert_called_once_with()
        transport._send_notification.assert_has_calls(calls, any_order=True)

        self.assertTrue(notifier.is_enabled())

TestMessagingNotifier.generate_scenarios()


class TestSerializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSerializer, self).setUp()
        self.addCleanup(_impl_test.reset)

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_serializer(self, mock_utcnow):
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        serializer = msg_serializer.NoOpSerializer()

        notifier = oslo_messaging.Notifier(transport,
                                           'test.localhost',
                                           driver='test',
                                           topics=['test'],
                                           serializer=serializer)

        message_id = uuid.uuid4()
        uuid.uuid4 = mock.Mock(return_value=message_id)

        mock_utcnow.return_value = datetime.datetime.utcnow()

        serializer.serialize_context = mock.Mock()
        serializer.serialize_context.return_value = dict(user='alice')

        serializer.serialize_entity = mock.Mock()
        serializer.serialize_entity.return_value = 'sbar'

        notifier.info(dict(user='bob'), 'test.notify', 'bar')

        message = {
            'message_id': str(message_id),
            'publisher_id': 'test.localhost',
            'event_type': 'test.notify',
            'priority': 'INFO',
            'payload': 'sbar',
            'timestamp': str(timeutils.utcnow()),
        }

        self.assertEqual([(dict(user='alice'), message, 'INFO', -1)],
                         _impl_test.NOTIFICATIONS)

        uuid.uuid4.assert_called_once_with()
        serializer.serialize_context.assert_called_once_with(dict(user='bob'))
        serializer.serialize_entity.assert_called_once_with(dict(user='bob'),
                                                            'bar')


class TestNotifierTopics(test_utils.BaseTestCase):

    def test_topics_from_config(self):
        self.config(driver=['log'],
                    group='oslo_messaging_notifications')
        self.config(topics=['topic1', 'topic2'],
                    group='oslo_messaging_notifications')
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        notifier = oslo_messaging.Notifier(transport, 'test.localhost')
        self.assertEqual(['topic1', 'topic2'], notifier._topics)

    def test_topics_from_kwargs(self):
        self.config(driver=['log'],
                    group='oslo_messaging_notifications')
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        notifier = oslo_messaging.Notifier(transport, 'test.localhost',
                                           topics=['topic1', 'topic2'])
        self.assertEqual(['topic1', 'topic2'], notifier._topics)


class TestLogNotifier(test_utils.BaseTestCase):

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_notifier(self, mock_utcnow):
        self.config(driver=['log'],
                    group='oslo_messaging_notifications')

        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        notifier = oslo_messaging.Notifier(transport, 'test.localhost')

        message_id = uuid.uuid4()
        uuid.uuid4 = mock.Mock()
        uuid.uuid4.return_value = message_id

        mock_utcnow.return_value = datetime.datetime.utcnow()

        logger = mock.Mock()

        message = {
            'message_id': str(message_id),
            'publisher_id': 'test.localhost',
            'event_type': 'test.notify',
            'priority': 'INFO',
            'payload': 'bar',
            'timestamp': str(timeutils.utcnow()),
        }

        with mock.patch.object(logging, 'getLogger') as gl:
            gl.return_value = logger

            notifier.info({}, 'test.notify', 'bar')

            uuid.uuid4.assert_called_once_with()
            logging.getLogger.assert_called_once_with(
                'oslo.messaging.notification.test.notify')

        logger.info.assert_called_once_with(JsonMessageMatcher(message))

        self.assertTrue(notifier.is_enabled())

    def test_sample_priority(self):
        # Ensure logger drops sample-level notifications.
        driver = _impl_log.LogDriver(None, None, None)

        logger = mock.Mock(spec=logging.getLogger('oslo.messaging.'
                                                  'notification.foo'))
        logger.sample = None

        msg = {'event_type': 'foo'}

        with mock.patch.object(logging, 'getLogger') as gl:
            gl.return_value = logger

            driver.notify(None, msg, "sample", None)

            logging.getLogger.assert_called_once_with('oslo.messaging.'
                                                      'notification.foo')

    def test_mask_passwords(self):
        # Ensure that passwords are masked with notifications
        driver = _impl_log.LogDriver(None, None, None)
        logger = mock.MagicMock()
        logger.info = mock.MagicMock()
        message = {'password': 'passw0rd', 'event_type': 'foo'}
        mask_str = jsonutils.dumps(strutils.mask_dict_password(message))

        with mock.patch.object(logging, 'getLogger') as gl:
            gl.return_value = logger
            driver.notify(None, message, 'info', 0)

        logger.info.assert_called_once_with(mask_str)


class TestNotificationConfig(test_utils.BaseTestCase):

    def test_retry_config(self):
        conf = self.messaging_conf.conf
        self.config(driver=['messaging'],
                    group='oslo_messaging_notifications')

        conf.set_override('retry', 3, group='oslo_messaging_notifications')
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')
        notifier = oslo_messaging.Notifier(transport)

        self.assertEqual(3, notifier.retry)

    def test_notifier_retry_config(self):
        conf = self.messaging_conf.conf
        self.config(driver=['messaging'],
                    group='oslo_messaging_notifications')

        conf.set_override('retry', 3, group='oslo_messaging_notifications')
        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')
        notifier = oslo_messaging.Notifier(transport, retry=5)

        self.assertEqual(5, notifier.retry)


class TestRoutingNotifier(test_utils.BaseTestCase):
    def setUp(self):
        super(TestRoutingNotifier, self).setUp()
        self.config(driver=['routing'],
                    group='oslo_messaging_notifications')

        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')
        self.notifier = oslo_messaging.Notifier(transport)
        self.router = self.notifier._driver_mgr['routing'].obj

        self.assertTrue(self.notifier.is_enabled())

    def _fake_extension_manager(self, ext):
        return extension.ExtensionManager.make_test_instance(
            [extension.Extension('test', None, None, ext), ])

    def _empty_extension_manager(self):
        return extension.ExtensionManager.make_test_instance([])

    def test_should_load_plugin(self):
        self.router.used_drivers = set(["zoo", "blah"])
        ext = mock.MagicMock()
        ext.name = "foo"
        self.assertFalse(self.router._should_load_plugin(ext))
        ext.name = "zoo"
        self.assertTrue(self.router._should_load_plugin(ext))

    def test_load_notifiers_no_config(self):
        # default routing_config=""
        self.router._load_notifiers()
        self.assertEqual({}, self.router.routing_groups)
        self.assertEqual(0, len(self.router.used_drivers))

    def test_load_notifiers_no_extensions(self):
        self.config(routing_config="routing_notifier.yaml",
                    group='oslo_messaging_notifications')
        routing_config = r""
        config_file = mock.MagicMock()
        config_file.return_value = routing_config

        with mock.patch.object(self.router, '_get_notifier_config_file',
                               config_file):
            with mock.patch('stevedore.dispatch.DispatchExtensionManager',
                            return_value=self._empty_extension_manager()):
                with mock.patch('oslo_messaging.notify.'
                                '_impl_routing.LOG') as mylog:
                    self.router._load_notifiers()
                    self.assertFalse(mylog.debug.called)
        self.assertEqual({}, self.router.routing_groups)

    def test_load_notifiers_config(self):
        self.config(routing_config="routing_notifier.yaml",
                    group='oslo_messaging_notifications')
        routing_config = r"""
group_1:
   rpc : foo
group_2:
   rpc : blah
        """

        config_file = mock.MagicMock()
        config_file.return_value = routing_config

        with mock.patch.object(self.router, '_get_notifier_config_file',
                               config_file):
            with mock.patch('stevedore.dispatch.DispatchExtensionManager',
                            return_value=self._fake_extension_manager(
                                mock.MagicMock())):
                with mock.patch('oslo_messaging.notify.'
                                '_impl_routing.LOG'):
                    self.router._load_notifiers()
                    groups = list(self.router.routing_groups.keys())
                    groups.sort()
                    self.assertEqual(['group_1', 'group_2'], groups)

    def test_get_drivers_for_message_accepted_events(self):
        config = r"""
group_1:
   rpc:
       accepted_events:
          - foo.*
          - blah.zoo.*
          - zip
        """
        groups = yaml.safe_load(config)
        group = groups['group_1']

        # No matching event ...
        self.assertEqual([],
                         self.router._get_drivers_for_message(
                             group, "unknown", "info"))

        # Child of foo ...
        self.assertEqual(['rpc'],
                         self.router._get_drivers_for_message(
                             group, "foo.1", "info"))

        # Foo itself ...
        self.assertEqual([],
                         self.router._get_drivers_for_message(
                             group, "foo", "info"))

        # Child of blah.zoo
        self.assertEqual(['rpc'],
                         self.router._get_drivers_for_message(
                             group, "blah.zoo.zing", "info"))

    def test_get_drivers_for_message_accepted_priorities(self):
        config = r"""
group_1:
   rpc:
       accepted_priorities:
          - info
          - error
        """
        groups = yaml.safe_load(config)
        group = groups['group_1']

        # No matching priority
        self.assertEqual([],
                         self.router._get_drivers_for_message(
                             group, None, "unknown"))

        # Info ...
        self.assertEqual(['rpc'],
                         self.router._get_drivers_for_message(
                             group, None, "info"))

        # Error (to make sure the list is getting processed) ...
        self.assertEqual(['rpc'],
                         self.router._get_drivers_for_message(
                             group, None, "error"))

    def test_get_drivers_for_message_both(self):
        config = r"""
group_1:
   rpc:
       accepted_priorities:
          - info
       accepted_events:
          - foo.*
   driver_1:
       accepted_priorities:
          - info
   driver_2:
      accepted_events:
          - foo.*
        """
        groups = yaml.safe_load(config)
        group = groups['group_1']

        # Valid event, but no matching priority
        self.assertEqual(['driver_2'],
                         self.router._get_drivers_for_message(
                             group, 'foo.blah', "unknown"))

        # Valid priority, but no matching event
        self.assertEqual(['driver_1'],
                         self.router._get_drivers_for_message(
                             group, 'unknown', "info"))

        # Happy day ...
        x = self.router._get_drivers_for_message(group, 'foo.blah', "info")
        x.sort()
        self.assertEqual(['driver_1', 'driver_2', 'rpc'], x)

    def test_filter_func(self):
        ext = mock.MagicMock()
        ext.name = "rpc"

        # Good ...
        self.assertTrue(self.router._filter_func(ext, {}, {}, 'info',
                        None, ['foo', 'rpc']))

        # Bad
        self.assertFalse(self.router._filter_func(ext, {}, {}, 'info',
                                                  None, ['foo']))

    def test_notify(self):
        self.router.routing_groups = {'group_1': None, 'group_2': None}
        drivers_mock = mock.MagicMock()
        drivers_mock.side_effect = [['rpc'], ['foo']]

        with mock.patch.object(self.router, 'plugin_manager') as pm:
            with mock.patch.object(self.router, '_get_drivers_for_message',
                                   drivers_mock):
                self.notifier.info({}, 'my_event', {})
                self.assertEqual(sorted(['rpc', 'foo']),
                                 sorted(pm.map.call_args[0][6]))

    def test_notify_filtered(self):
        self.config(routing_config="routing_notifier.yaml",
                    group='oslo_messaging_notifications')
        routing_config = r"""
group_1:
    rpc:
        accepted_events:
          - my_event
    rpc2:
        accepted_priorities:
          - info
    bar:
        accepted_events:
            - nothing
        """
        config_file = mock.MagicMock()
        config_file.return_value = routing_config

        rpc_driver = mock.Mock()
        rpc2_driver = mock.Mock()
        bar_driver = mock.Mock()

        pm = dispatch.DispatchExtensionManager.make_test_instance(
            [extension.Extension('rpc', None, None, rpc_driver),
             extension.Extension('rpc2', None, None, rpc2_driver),
             extension.Extension('bar', None, None, bar_driver)],
        )

        with mock.patch.object(self.router, '_get_notifier_config_file',
                               config_file):
            with mock.patch('stevedore.dispatch.DispatchExtensionManager',
                            return_value=pm):
                with mock.patch('oslo_messaging.notify.'
                                '_impl_routing.LOG'):

                    self.notifier.info({}, 'my_event', {})
                    self.assertFalse(bar_driver.info.called)
                    rpc_driver.notify.assert_called_once_with(
                        {}, mock.ANY, 'INFO', -1)
                    rpc2_driver.notify.assert_called_once_with(
                        {}, mock.ANY, 'INFO', -1)


class TestNoOpNotifier(test_utils.BaseTestCase):

    def test_notifier(self):
        self.config(driver=['noop'],
                    group='oslo_messaging_notifications')

        transport = oslo_messaging.get_notification_transport(self.conf,
                                                              url='fake:')

        notifier = oslo_messaging.Notifier(transport, 'test.localhost')

        self.assertFalse(notifier.is_enabled())


class TestNotifierTransportWarning(test_utils.BaseTestCase):

    @mock.patch('oslo_messaging.notify.notifier._LOG')
    def test_warning_when_rpc_transport(self, log):
        transport = oslo_messaging.get_rpc_transport(self.conf)
        oslo_messaging.Notifier(transport, 'test.localhost')
        log.warning.assert_called_once_with(
            "Using RPC transport for notifications. Please use "
            "get_notification_transport to obtain a "
            "notification transport instance.")
