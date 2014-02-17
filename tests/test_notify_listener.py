
# Copyright 2013 eNovance
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

import threading

import mock
from oslo.config import cfg
import testscenarios

from oslo import messaging
from oslo.messaging.notify import dispatcher
from tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class ListenerSetupMixin(object):

    class Listener(object):
        def __init__(self, transport, topics, endpoints, expect_messages):
            targets = [messaging.Target(topic=topic)
                       for topic in topics]
            self._expect_messages = expect_messages
            self._received_msgs = 0
            self._listener = messaging.get_notification_listener(
                transport, targets, endpoints + [self])

        def info(self, ctxt, publisher_id, event_type, payload):
            self._received_msgs += 1
            if self._expect_messages == self._received_msgs:
                # Check start() does nothing with a running listener
                self._listener.start()
                self._listener.stop()
                self._listener.wait()

        def start(self):
            self._listener.start()

    def _setup_listener(self, transport, endpoints, expect_messages,
                        topics=None):
        listener = self.Listener(transport,
                                 topics=topics or ['testtopic'],
                                 expect_messages=expect_messages,
                                 endpoints=endpoints)

        thread = threading.Thread(target=listener.start)
        thread.daemon = True
        thread.start()
        return thread

    def _stop_listener(self, thread):
        thread.join(timeout=5)

    def _setup_notifier(self, transport, topic='testtopic',
                        publisher_id='testpublisher'):
        return messaging.Notifier(transport, topic=topic,
                                  driver='messaging',
                                  publisher_id=publisher_id)


class TestNotifyListener(test_utils.BaseTestCase, ListenerSetupMixin):

    def __init__(self, *args):
        super(TestNotifyListener, self).__init__(*args)
        ListenerSetupMixin.__init__(self)

    def setUp(self):
        super(TestNotifyListener, self).setUp(conf=cfg.ConfigOpts())

    def test_constructor(self):
        transport = messaging.get_transport(self.conf, url='fake:')
        target = messaging.Target(topic='foo')
        endpoints = [object()]

        listener = messaging.get_notification_listener(transport, [target],
                                                       endpoints)

        self.assertIs(listener.conf, self.conf)
        self.assertIs(listener.transport, transport)
        self.assertIsInstance(listener.dispatcher,
                              dispatcher.NotificationDispatcher)
        self.assertIs(listener.dispatcher.endpoints, endpoints)
        self.assertIs(listener.executor, 'blocking')

    def test_no_target_topic(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        listener = messaging.get_notification_listener(transport,
                                                       [messaging.Target()],
                                                       [mock.Mock()])
        try:
            listener.start()
        except Exception as ex:
            self.assertIsInstance(ex, messaging.InvalidTarget, ex)
        else:
            self.assertTrue(False)

    def test_unknown_executor(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        try:
            messaging.get_notification_listener(transport, [], [],
                                                executor='foo')
        except Exception as ex:
            self.assertIsInstance(ex, messaging.ExecutorLoadFailure)
            self.assertEqual(ex.executor, 'foo')
        else:
            self.assertTrue(False)

    def test_one_topic(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info = mock.Mock()
        listener_thread = self._setup_listener(transport, [endpoint], 1)

        notifier = self._setup_notifier(transport)
        notifier.info({}, 'an_event.start', 'test message')

        self._stop_listener(listener_thread)

        endpoint.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test message')

    def test_two_topics(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info = mock.Mock()
        topics = ["topic1", "topic2"]
        listener_thread = self._setup_listener(transport, [endpoint], 2,
                                               topics=topics)
        notifier = self._setup_notifier(transport, topic='topic1')
        notifier.info({}, 'an_event.start1', 'test')
        notifier = self._setup_notifier(transport, topic='topic2')
        notifier.info({}, 'an_event.start2', 'test')

        self._stop_listener(listener_thread)

        expected = [mock.call({}, 'testpublisher', 'an_event.start1', 'test'),
                    mock.call({}, 'testpublisher', 'an_event.start2', 'test')]
        self.assertEqual(sorted(endpoint.info.call_args_list), expected)

    def test_two_endpoints(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        endpoint1 = mock.Mock()
        endpoint1.info = mock.Mock()
        endpoint2 = mock.Mock()
        endpoint2.info = mock.Mock()
        listener_thread = self._setup_listener(transport,
                                               [endpoint1, endpoint2], 1)
        notifier = self._setup_notifier(transport)
        notifier.info({}, 'an_event.start', 'test')

        self._stop_listener(listener_thread)

        endpoint1.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test')
        endpoint2.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test')
