
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

from oslo_config import cfg
import testscenarios

import oslo_messaging
from oslo_messaging.notify import dispatcher
from oslo_messaging.notify import notifier as msg_notifier
from oslo_messaging.tests import utils as test_utils
import six
from six.moves import mock

load_tests = testscenarios.load_tests_apply_scenarios


class RestartableServerThread(object):
    def __init__(self, server):
        self.server = server
        self.thread = None

    def start(self):
        if self.thread is None:
            self.thread = test_utils.ServerThreadHelper(self.server)
            self.thread.start()

    def stop(self):
        if self.thread is not None:
            self.thread.stop()
            self.thread.join(timeout=15)
            ret = self.thread.isAlive()
            self.thread = None
            return ret
        return True


class ListenerSetupMixin(object):

    class ThreadTracker(object):
        def __init__(self):
            self._received_msgs = 0
            self.threads = []
            self.lock = threading.Condition()

        def info(self, *args, **kwargs):
            # NOTE(sileht): this run into an other thread
            with self.lock:
                self._received_msgs += 1
                self.lock.notify_all()

        def wait_for_messages(self, expect_messages):
            with self.lock:
                while self._received_msgs < expect_messages:
                    self.lock.wait()

        def stop(self):
            for thread in self.threads:
                thread.stop()
            self.threads = []

        def start(self, thread):
            self.threads.append(thread)
            thread.start()

    def setUp(self):
        self.trackers = {}
        self.addCleanup(self._stop_trackers)

    def _stop_trackers(self):
        for pool in self.trackers:
            self.trackers[pool].stop()
        self.trackers = {}

    def _setup_listener(self, transport, endpoints,
                        targets=None, pool=None, batch=False):

        if pool is None:
            tracker_name = '__default__'
        else:
            tracker_name = pool

        if targets is None:
            targets = [oslo_messaging.Target(topic='testtopic')]

        tracker = self.trackers.setdefault(
            tracker_name, self.ThreadTracker())
        if batch:
            listener = oslo_messaging.get_batch_notification_listener(
                transport, targets=targets, endpoints=[tracker] + endpoints,
                allow_requeue=True, pool=pool, executor='eventlet',
                batch_size=batch[0], batch_timeout=batch[1])
        else:
            listener = oslo_messaging.get_notification_listener(
                transport, targets=targets, endpoints=[tracker] + endpoints,
                allow_requeue=True, pool=pool, executor='eventlet')

        thread = RestartableServerThread(listener)
        tracker.start(thread)
        return thread

    def wait_for_messages(self, expect_messages, tracker_name='__default__'):
        self.trackers[tracker_name].wait_for_messages(expect_messages)

    def _setup_notifier(self, transport, topics=['testtopic'],
                        publisher_id='testpublisher'):
        return oslo_messaging.Notifier(transport, topics=topics,
                                       driver='messaging',
                                       publisher_id=publisher_id)


class TestNotifyListener(test_utils.BaseTestCase, ListenerSetupMixin):

    def __init__(self, *args):
        super(TestNotifyListener, self).__init__(*args)
        ListenerSetupMixin.__init__(self)

    def setUp(self):
        super(TestNotifyListener, self).setUp(conf=cfg.ConfigOpts())
        ListenerSetupMixin.setUp(self)

    @mock.patch('debtcollector.deprecate')
    def test_constructor(self, deprecate):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')
        target = oslo_messaging.Target(topic='foo')
        endpoints = [object()]

        listener = oslo_messaging.get_notification_listener(
            transport, [target], endpoints)

        self.assertIs(listener.conf, self.conf)
        self.assertIs(listener.transport, transport)
        self.assertIsInstance(listener.dispatcher,
                              dispatcher.NotificationDispatcher)
        self.assertIs(listener.dispatcher.endpoints, endpoints)
        self.assertEqual('blocking', listener.executor_type)
        deprecate.assert_called_once_with(
            'blocking executor is deprecated. Executor default will be '
            'removed. Use explicitly threading or eventlet instead',
            removal_version='rocky', version='pike',
            category=FutureWarning)

    def test_no_target_topic(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        listener = oslo_messaging.get_notification_listener(
            transport,
            [oslo_messaging.Target()],
            [mock.Mock()])
        try:
            listener.start()
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.InvalidTarget, ex)
        else:
            self.assertTrue(False)

    def test_unknown_executor(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        try:
            oslo_messaging.get_notification_listener(transport, [], [],
                                                     executor='foo')
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.ExecutorLoadFailure)
            self.assertEqual('foo', ex.executor)
        else:
            self.assertTrue(False)

    def test_batch_timeout(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.return_value = None
        listener_thread = self._setup_listener(transport, [endpoint],
                                               batch=(5, 1))

        notifier = self._setup_notifier(transport)
        for i in six.moves.range(12):
            notifier.info({}, 'an_event.start', 'test message')

        self.wait_for_messages(3)
        self.assertFalse(listener_thread.stop())

        messages = [dict(ctxt={},
                         publisher_id='testpublisher',
                         event_type='an_event.start',
                         payload='test message',
                         metadata={'message_id': mock.ANY,
                                   'timestamp': mock.ANY})]

        endpoint.info.assert_has_calls([mock.call(messages * 5),
                                        mock.call(messages * 5),
                                        mock.call(messages * 2)])

    def test_batch_size(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.return_value = None
        listener_thread = self._setup_listener(transport, [endpoint],
                                               batch=(5, None))

        notifier = self._setup_notifier(transport)
        for i in six.moves.range(10):
            notifier.info({}, 'an_event.start', 'test message')

        self.wait_for_messages(2)
        self.assertFalse(listener_thread.stop())

        messages = [dict(ctxt={},
                         publisher_id='testpublisher',
                         event_type='an_event.start',
                         payload='test message',
                         metadata={'message_id': mock.ANY,
                                   'timestamp': mock.ANY})]

        endpoint.info.assert_has_calls([mock.call(messages * 5),
                                        mock.call(messages * 5)])

    def test_batch_size_exception_path(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.side_effect = [None, Exception('boom!')]
        listener_thread = self._setup_listener(transport, [endpoint],
                                               batch=(5, None))

        notifier = self._setup_notifier(transport)
        for i in six.moves.range(10):
            notifier.info({}, 'an_event.start', 'test message')

        self.wait_for_messages(2)
        self.assertFalse(listener_thread.stop())

        messages = [dict(ctxt={},
                         publisher_id='testpublisher',
                         event_type='an_event.start',
                         payload='test message',
                         metadata={'message_id': mock.ANY,
                                   'timestamp': mock.ANY})]

        endpoint.info.assert_has_calls([mock.call(messages * 5)])

    def test_one_topic(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.return_value = None
        listener_thread = self._setup_listener(transport, [endpoint])

        notifier = self._setup_notifier(transport)
        notifier.info({}, 'an_event.start', 'test message')

        self.wait_for_messages(1)
        self.assertFalse(listener_thread.stop())

        endpoint.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test message',
            {'message_id': mock.ANY, 'timestamp': mock.ANY})

    def test_two_topics(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.return_value = None
        targets = [oslo_messaging.Target(topic="topic1"),
                   oslo_messaging.Target(topic="topic2")]
        listener_thread = self._setup_listener(transport, [endpoint],
                                               targets=targets)
        notifier = self._setup_notifier(transport, topics=['topic1'])
        notifier.info({'ctxt': '1'}, 'an_event.start1', 'test')
        notifier = self._setup_notifier(transport, topics=['topic2'])
        notifier.info({'ctxt': '2'}, 'an_event.start2', 'test')

        self.wait_for_messages(2)
        self.assertFalse(listener_thread.stop())

        endpoint.info.assert_has_calls([
            mock.call({'ctxt': '1'}, 'testpublisher',
                      'an_event.start1', 'test',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY}),
            mock.call({'ctxt': '2'}, 'testpublisher',
                      'an_event.start2', 'test',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY})],
            any_order=True)

    def test_two_exchanges(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint = mock.Mock()
        endpoint.info.return_value = None
        targets = [oslo_messaging.Target(topic="topic",
                                         exchange="exchange1"),
                   oslo_messaging.Target(topic="topic",
                                         exchange="exchange2")]
        listener_thread = self._setup_listener(transport, [endpoint],
                                               targets=targets)

        notifier = self._setup_notifier(transport, topics=["topic"])

        def mock_notifier_exchange(name):
            def side_effect(target, ctxt, message, version, retry):
                target.exchange = name
                return transport._driver.send_notification(target, ctxt,
                                                           message, version,
                                                           retry=retry)
            transport._send_notification = mock.MagicMock(
                side_effect=side_effect)

        notifier.info({'ctxt': '0'},
                      'an_event.start', 'test message default exchange')
        mock_notifier_exchange('exchange1')
        notifier.info({'ctxt': '1'},
                      'an_event.start', 'test message exchange1')
        mock_notifier_exchange('exchange2')
        notifier.info({'ctxt': '2'},
                      'an_event.start', 'test message exchange2')

        self.wait_for_messages(2)
        self.assertFalse(listener_thread.stop())

        endpoint.info.assert_has_calls([
            mock.call({'ctxt': '1'}, 'testpublisher', 'an_event.start',
                      'test message exchange1',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY}),
            mock.call({'ctxt': '2'}, 'testpublisher', 'an_event.start',
                      'test message exchange2',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY})],
            any_order=True)

    def test_two_endpoints(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint1 = mock.Mock()
        endpoint1.info.return_value = None
        endpoint2 = mock.Mock()
        endpoint2.info.return_value = oslo_messaging.NotificationResult.HANDLED
        listener_thread = self._setup_listener(transport,
                                               [endpoint1, endpoint2])
        notifier = self._setup_notifier(transport)
        notifier.info({}, 'an_event.start', 'test')

        self.wait_for_messages(1)
        self.assertFalse(listener_thread.stop())

        endpoint1.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test', {
                'timestamp': mock.ANY,
                'message_id': mock.ANY})

        endpoint2.info.assert_called_once_with(
            {}, 'testpublisher', 'an_event.start', 'test', {
                'timestamp': mock.ANY,
                'message_id': mock.ANY})

    def test_requeue(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')
        endpoint = mock.Mock()
        endpoint.info = mock.Mock()

        def side_effect_requeue(*args, **kwargs):
            if endpoint.info.call_count == 1:
                return oslo_messaging.NotificationResult.REQUEUE
            return oslo_messaging.NotificationResult.HANDLED

        endpoint.info.side_effect = side_effect_requeue
        listener_thread = self._setup_listener(transport, [endpoint])
        notifier = self._setup_notifier(transport)
        notifier.info({}, 'an_event.start', 'test')

        self.wait_for_messages(2)
        self.assertFalse(listener_thread.stop())

        endpoint.info.assert_has_calls([
            mock.call({}, 'testpublisher', 'an_event.start', 'test',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY}),
            mock.call({}, 'testpublisher', 'an_event.start', 'test',
                      {'timestamp': mock.ANY, 'message_id': mock.ANY})])

    def test_two_pools(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint1 = mock.Mock()
        endpoint1.info.return_value = None
        endpoint2 = mock.Mock()
        endpoint2.info.return_value = None

        targets = [oslo_messaging.Target(topic="topic")]
        listener1_thread = self._setup_listener(transport, [endpoint1],
                                                targets=targets, pool="pool1")
        listener2_thread = self._setup_listener(transport, [endpoint2],
                                                targets=targets, pool="pool2")

        notifier = self._setup_notifier(transport, topics=["topic"])
        notifier.info({'ctxt': '0'}, 'an_event.start', 'test message0')
        notifier.info({'ctxt': '1'}, 'an_event.start', 'test message1')

        self.wait_for_messages(2, "pool1")
        self.wait_for_messages(2, "pool2")
        self.assertFalse(listener2_thread.stop())
        self.assertFalse(listener1_thread.stop())

        def mocked_endpoint_call(i):
            return mock.call({'ctxt': '%d' % i}, 'testpublisher',
                             'an_event.start', 'test message%d' % i,
                             {'timestamp': mock.ANY, 'message_id': mock.ANY})

        endpoint1.info.assert_has_calls([mocked_endpoint_call(0),
                                         mocked_endpoint_call(1)])
        endpoint2.info.assert_has_calls([mocked_endpoint_call(0),
                                         mocked_endpoint_call(1)])

    def test_two_pools_three_listener(self):
        transport = msg_notifier.get_notification_transport(
            self.conf, url='fake:')

        endpoint1 = mock.Mock()
        endpoint1.info.return_value = None
        endpoint2 = mock.Mock()
        endpoint2.info.return_value = None
        endpoint3 = mock.Mock()
        endpoint3.info.return_value = None

        targets = [oslo_messaging.Target(topic="topic")]
        listener1_thread = self._setup_listener(transport, [endpoint1],
                                                targets=targets, pool="pool1")
        listener2_thread = self._setup_listener(transport, [endpoint2],
                                                targets=targets, pool="pool2")
        listener3_thread = self._setup_listener(transport, [endpoint3],
                                                targets=targets, pool="pool2")

        def mocked_endpoint_call(i):
            return mock.call({'ctxt': '%d' % i}, 'testpublisher',
                             'an_event.start', 'test message%d' % i,
                             {'timestamp': mock.ANY, 'message_id': mock.ANY})

        notifier = self._setup_notifier(transport, topics=["topic"])
        mocked_endpoint1_calls = []
        for i in range(0, 25):
            notifier.info({'ctxt': '%d' % i}, 'an_event.start',
                          'test message%d' % i)
            mocked_endpoint1_calls.append(mocked_endpoint_call(i))

        self.wait_for_messages(25, 'pool2')
        listener2_thread.stop()

        for i in range(0, 25):
            notifier.info({'ctxt': '%d' % i}, 'an_event.start',
                          'test message%d' % i)
            mocked_endpoint1_calls.append(mocked_endpoint_call(i))

        self.wait_for_messages(50, 'pool2')
        listener2_thread.start()
        listener3_thread.stop()

        for i in range(0, 25):
            notifier.info({'ctxt': '%d' % i}, 'an_event.start',
                          'test message%d' % i)
            mocked_endpoint1_calls.append(mocked_endpoint_call(i))

        self.wait_for_messages(75, 'pool2')
        listener3_thread.start()

        for i in range(0, 25):
            notifier.info({'ctxt': '%d' % i}, 'an_event.start',
                          'test message%d' % i)
            mocked_endpoint1_calls.append(mocked_endpoint_call(i))

        self.wait_for_messages(100, 'pool1')
        self.wait_for_messages(100, 'pool2')

        self.assertFalse(listener3_thread.stop())
        self.assertFalse(listener2_thread.stop())
        self.assertFalse(listener1_thread.stop())

        self.assertEqual(100, endpoint1.info.call_count)
        endpoint1.info.assert_has_calls(mocked_endpoint1_calls)

        self.assertLessEqual(25, endpoint2.info.call_count)
        self.assertLessEqual(25, endpoint3.info.call_count)

        self.assertEqual(100, endpoint2.info.call_count +
                         endpoint3.info.call_count)
        for call in mocked_endpoint1_calls:
            self.assertIn(call, endpoint2.info.mock_calls +
                          endpoint3.info.mock_calls)
