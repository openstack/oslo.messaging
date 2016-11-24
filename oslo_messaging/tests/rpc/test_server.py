
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

import eventlet
import threading

from oslo_config import cfg
import testscenarios

import mock
import oslo_messaging
from oslo_messaging import rpc
from oslo_messaging.rpc import server as rpc_server_module
from oslo_messaging import server as server_module
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class ServerSetupMixin(object):

    class Server(object):
        def __init__(self, transport, topic, server, endpoint, serializer):
            self.controller = ServerSetupMixin.ServerController()
            target = oslo_messaging.Target(topic=topic, server=server)
            self.server = oslo_messaging.get_rpc_server(transport,
                                                        target,
                                                        [endpoint,
                                                         self.controller],
                                                        serializer=serializer)

        def wait(self):
            # Wait for the executor to process the stop message, indicating all
            # test messages have been processed
            self.controller.stopped.wait()

            # Check start() does nothing with a running server
            self.server.start()
            self.server.stop()
            self.server.wait()

        def start(self):
            self.server.start()

    class ServerController(object):
        def __init__(self):
            self.stopped = threading.Event()

        def stop(self, ctxt):
            self.stopped.set()

    class TestSerializer(object):

        def serialize_entity(self, ctxt, entity):
            return ('s' + entity) if entity else entity

        def deserialize_entity(self, ctxt, entity):
            return ('d' + entity) if entity else entity

        def serialize_context(self, ctxt):
            return dict([(k, 's' + v) for k, v in ctxt.items()])

        def deserialize_context(self, ctxt):
            return dict([(k, 'd' + v) for k, v in ctxt.items()])

    def __init__(self):
        self.serializer = self.TestSerializer()

    def _setup_server(self, transport, endpoint, topic=None, server=None):
        server = self.Server(transport,
                             topic=topic or 'testtopic',
                             server=server or 'testserver',
                             endpoint=endpoint,
                             serializer=self.serializer)

        server.start()
        return server

    def _stop_server(self, client, server, topic=None):
        if topic is not None:
            client = client.prepare(topic=topic)
        client.cast({}, 'stop')
        server.wait()

    def _setup_client(self, transport, topic='testtopic'):
        return oslo_messaging.RPCClient(transport,
                                        oslo_messaging.Target(topic=topic),
                                        serializer=self.serializer)


class TestRPCServer(test_utils.BaseTestCase, ServerSetupMixin):

    def __init__(self, *args):
        super(TestRPCServer, self).__init__(*args)
        ServerSetupMixin.__init__(self)

    def setUp(self):
        super(TestRPCServer, self).setUp(conf=cfg.ConfigOpts())

    def test_constructor(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')
        target = oslo_messaging.Target(topic='foo', server='bar')
        endpoints = [object()]
        serializer = object()

        server = oslo_messaging.get_rpc_server(transport, target, endpoints,
                                               serializer=serializer)

        self.assertIs(server.conf, self.conf)
        self.assertIs(server.transport, transport)
        self.assertIsInstance(server.dispatcher, oslo_messaging.RPCDispatcher)
        self.assertIs(server.dispatcher.endpoints, endpoints)
        self.assertIs(server.dispatcher.serializer, serializer)
        self.assertEqual('blocking', server.executor_type)

    def test_server_wait_method(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')
        target = oslo_messaging.Target(topic='foo', server='bar')
        endpoints = [object()]
        serializer = object()

        class MagicMockIgnoreArgs(mock.MagicMock):
            """MagicMock ignores arguments.

            A MagicMock which can never misinterpret the arguments passed to
            it during construction.
            """

            def __init__(self, *args, **kwargs):
                super(MagicMockIgnoreArgs, self).__init__()

        server = oslo_messaging.get_rpc_server(transport, target, endpoints,
                                               serializer=serializer)
        # Mocking executor
        server._executor_cls = MagicMockIgnoreArgs
        server._create_listener = MagicMockIgnoreArgs()
        server.dispatcher = MagicMockIgnoreArgs()
        # Here assigning executor's listener object to listener variable
        # before calling wait method, because in wait method we are
        # setting executor to None.
        server.start()
        listener = server.listener
        server.stop()
        # call server wait method
        server.wait()
        self.assertEqual(1, listener.cleanup.call_count)

    def test_no_target_server(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        server = oslo_messaging.get_rpc_server(
            transport,
            oslo_messaging.Target(topic='testtopic'),
            [])
        try:
            server.start()
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.InvalidTarget, ex)
            self.assertEqual('testtopic', ex.target.topic)
        else:
            self.assertTrue(False)

    def test_no_server_topic(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')
        target = oslo_messaging.Target(server='testserver')
        server = oslo_messaging.get_rpc_server(transport, target, [])
        try:
            server.start()
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.InvalidTarget, ex)
            self.assertEqual('testserver', ex.target.server)
        else:
            self.assertTrue(False)

    def _test_no_client_topic(self, call=True):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        client = self._setup_client(transport, topic=None)

        method = client.call if call else client.cast

        try:
            method({}, 'ping', arg='foo')
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.InvalidTarget, ex)
            self.assertIsNotNone(ex.target)
        else:
            self.assertTrue(False)

    def test_no_client_topic_call(self):
        self._test_no_client_topic(call=True)

    def test_no_client_topic_cast(self):
        self._test_no_client_topic(call=False)

    def test_client_call_timeout(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        finished = False
        wait = threading.Condition()

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                with wait:
                    if not finished:
                        wait.wait()

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        try:
            client.prepare(timeout=0).call({}, 'ping', arg='foo')
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.MessagingTimeout, ex)
        else:
            self.assertTrue(False)

        with wait:
            finished = True
            wait.notify()

        self._stop_server(client, server_thread)

    def test_unknown_executor(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        try:
            oslo_messaging.get_rpc_server(transport, None, [], executor='foo')
        except Exception as ex:
            self.assertIsInstance(ex, oslo_messaging.ExecutorLoadFailure)
            self.assertEqual('foo', ex.executor)
        else:
            self.assertTrue(False)

    def test_cast(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def __init__(self):
                self.pings = []

            def ping(self, ctxt, arg):
                self.pings.append(arg)

        endpoint = TestEndpoint()
        server_thread = self._setup_server(transport, endpoint)
        client = self._setup_client(transport)

        client.cast({}, 'ping', arg='foo')
        client.cast({}, 'ping', arg='bar')

        self._stop_server(client, server_thread)

        self.assertEqual(['dsfoo', 'dsbar'], endpoint.pings)

    def test_call(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                return arg

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        self.assertIsNone(client.call({}, 'ping', arg=None))
        self.assertEqual(0, client.call({}, 'ping', arg=0))
        self.assertFalse(client.call({}, 'ping', arg=False))
        self.assertEqual([], client.call({}, 'ping', arg=[]))
        self.assertEqual({}, client.call({}, 'ping', arg={}))
        self.assertEqual('dsdsfoo', client.call({}, 'ping', arg='foo'))

        self._stop_server(client, server_thread)

    def test_direct_call(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                return arg

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        direct = client.prepare(server='testserver')
        self.assertIsNone(direct.call({}, 'ping', arg=None))
        self.assertEqual(0, client.call({}, 'ping', arg=0))
        self.assertFalse(client.call({}, 'ping', arg=False))
        self.assertEqual([], client.call({}, 'ping', arg=[]))
        self.assertEqual({}, client.call({}, 'ping', arg={}))
        self.assertEqual('dsdsfoo', direct.call({}, 'ping', arg='foo'))

        self._stop_server(client, server_thread)

    def test_context(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ctxt_check(self, ctxt, key):
                return ctxt[key]

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        self.assertEqual('dsdsb',
                         client.call({'dsa': 'b'},
                                     'ctxt_check',
                                     key='a'))

        self._stop_server(client, server_thread)

    def test_failure(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                raise ValueError(arg)

        debugs = []
        errors = []

        def stub_debug(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            debugs.append(str(msg) % a)

        def stub_error(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            errors.append(str(msg) % a)

        self.stubs.Set(rpc_server_module.LOG, 'debug', stub_debug)
        self.stubs.Set(rpc_server_module.LOG, 'error', stub_error)

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        try:
            client.call({}, 'ping', arg='foo')
        except Exception as ex:
            self.assertIsInstance(ex, ValueError)
            self.assertEqual('dsfoo', str(ex))
            self.assertTrue(len(debugs) == 0)
            self.assertGreater(len(errors), 0)
        else:
            self.assertTrue(False)

        self._stop_server(client, server_thread)

    def test_expected_failure(self):
        transport = oslo_messaging.get_transport(self.conf, url='fake:')

        debugs = []
        errors = []

        def stub_debug(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            debugs.append(str(msg) % a)

        def stub_error(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            errors.append(str(msg) % a)

        self.stubs.Set(rpc_server_module.LOG, 'debug', stub_debug)
        self.stubs.Set(rpc_server_module.LOG, 'error', stub_error)

        class TestEndpoint(object):
            @oslo_messaging.expected_exceptions(ValueError)
            def ping(self, ctxt, arg):
                raise ValueError(arg)

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        try:
            client.call({}, 'ping', arg='foo')
        except Exception as ex:
            self.assertIsInstance(ex, ValueError)
            self.assertEqual('dsfoo', str(ex))
            self.assertGreater(len(debugs), 0)
            self.assertTrue(len(errors) == 0)
        else:
            self.assertTrue(False)

        self._stop_server(client, server_thread)


class TestMultipleServers(test_utils.BaseTestCase, ServerSetupMixin):

    _exchanges = [
        ('same_exchange', dict(exchange1=None, exchange2=None)),
        ('diff_exchange', dict(exchange1='x1', exchange2='x2')),
    ]

    _topics = [
        ('same_topic', dict(topic1='t', topic2='t')),
        ('diff_topic', dict(topic1='t1', topic2='t2')),
    ]

    _server = [
        ('same_server', dict(server1=None, server2=None)),
        ('diff_server', dict(server1='s1', server2='s2')),
    ]

    _fanout = [
        ('not_fanout', dict(fanout1=None, fanout2=None)),
        ('fanout', dict(fanout1=True, fanout2=True)),
    ]

    _method = [
        ('call', dict(call1=True, call2=True)),
        ('cast', dict(call1=False, call2=False)),
    ]

    _endpoints = [
        ('one_endpoint',
         dict(multi_endpoints=False,
              expect1=['ds1', 'ds2'],
              expect2=['ds1', 'ds2'])),
        ('two_endpoints',
         dict(multi_endpoints=True,
              expect1=['ds1'],
              expect2=['ds2'])),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._exchanges,
                                                         cls._topics,
                                                         cls._server,
                                                         cls._fanout,
                                                         cls._method,
                                                         cls._endpoints)

        # fanout call not supported
        def filter_fanout_call(scenario):
            params = scenario[1]
            fanout = params['fanout1'] or params['fanout2']
            call = params['call1'] or params['call2']
            return not (call and fanout)

        # listening multiple times on same topic/server pair not supported
        def filter_same_topic_and_server(scenario):
            params = scenario[1]
            single_topic = params['topic1'] == params['topic2']
            single_server = params['server1'] == params['server2']
            return not (single_topic and single_server)

        # fanout to multiple servers on same topic and exchange
        # each endpoint will receive both messages
        def fanout_to_servers(scenario):
            params = scenario[1]
            fanout = params['fanout1'] or params['fanout2']
            single_exchange = params['exchange1'] == params['exchange2']
            single_topic = params['topic1'] == params['topic2']
            multi_servers = params['server1'] != params['server2']
            if fanout and single_exchange and single_topic and multi_servers:
                params['expect1'] = params['expect1'][:] + params['expect1']
                params['expect2'] = params['expect2'][:] + params['expect2']
            return scenario

        # multiple endpoints on same topic and exchange
        # either endpoint can get either message
        def single_topic_multi_endpoints(scenario):
            params = scenario[1]
            single_exchange = params['exchange1'] == params['exchange2']
            single_topic = params['topic1'] == params['topic2']
            if single_topic and single_exchange and params['multi_endpoints']:
                params['expect_either'] = (params['expect1'] +
                                           params['expect2'])
                params['expect1'] = params['expect2'] = []
            else:
                params['expect_either'] = []
            return scenario

        for f in [filter_fanout_call, filter_same_topic_and_server]:
            cls.scenarios = [i for i in cls.scenarios if f(i)]
        for m in [fanout_to_servers, single_topic_multi_endpoints]:
            cls.scenarios = [m(i) for i in cls.scenarios]

    def __init__(self, *args):
        super(TestMultipleServers, self).__init__(*args)
        ServerSetupMixin.__init__(self)

    def setUp(self):
        super(TestMultipleServers, self).setUp(conf=cfg.ConfigOpts())

    def test_multiple_servers(self):
        url1 = 'fake:///' + (self.exchange1 or '')
        url2 = 'fake:///' + (self.exchange2 or '')

        transport1 = oslo_messaging.get_transport(self.conf, url=url1)
        if url1 != url2:
            transport2 = oslo_messaging.get_transport(self.conf, url=url1)
        else:
            transport2 = transport1

        class TestEndpoint(object):
            def __init__(self):
                self.pings = []

            def ping(self, ctxt, arg):
                self.pings.append(arg)

            def alive(self, ctxt):
                return 'alive'

        if self.multi_endpoints:
            endpoint1, endpoint2 = TestEndpoint(), TestEndpoint()
        else:
            endpoint1 = endpoint2 = TestEndpoint()

        server1 = self._setup_server(transport1, endpoint1,
                                     topic=self.topic1, server=self.server1)
        server2 = self._setup_server(transport2, endpoint2,
                                     topic=self.topic2, server=self.server2)

        client1 = self._setup_client(transport1, topic=self.topic1)
        client2 = self._setup_client(transport2, topic=self.topic2)

        client1 = client1.prepare(server=self.server1)
        client2 = client2.prepare(server=self.server2)

        if self.fanout1:
            client1.call({}, 'alive')
            client1 = client1.prepare(fanout=True)
        if self.fanout2:
            client2.call({}, 'alive')
            client2 = client2.prepare(fanout=True)

        (client1.call if self.call1 else client1.cast)({}, 'ping', arg='1')
        (client2.call if self.call2 else client2.cast)({}, 'ping', arg='2')

        self._stop_server(client1.prepare(fanout=None),
                          server1, topic=self.topic1)
        self._stop_server(client2.prepare(fanout=None),
                          server2, topic=self.topic2)

        def check(pings, expect):
            self.assertEqual(len(expect), len(pings))
            for a in expect:
                self.assertIn(a, pings)

        if self.expect_either:
            check(endpoint1.pings + endpoint2.pings, self.expect_either)
        else:
            check(endpoint1.pings, self.expect1)
            check(endpoint2.pings, self.expect2)


TestMultipleServers.generate_scenarios()


class TestServerLocking(test_utils.BaseTestCase):
    def setUp(self):
        super(TestServerLocking, self).setUp(conf=cfg.ConfigOpts())

        def _logmethod(name):
            def method(self, *args, **kwargs):
                with self._lock:
                    self._calls.append(name)
            return method

        executors = []

        class FakeExecutor(object):
            def __init__(self, *args, **kwargs):
                self._lock = threading.Lock()
                self._calls = []
                executors.append(self)

            submit = _logmethod('submit')
            shutdown = _logmethod('shutdown')

        self.executors = executors

        class MessageHandlingServerImpl(oslo_messaging.MessageHandlingServer):
            def _create_listener(self):
                return mock.Mock()

            def _process_incoming(self, incoming):
                pass

        self.server = MessageHandlingServerImpl(mock.Mock(), mock.Mock())
        self.server._executor_cls = FakeExecutor

    def test_start_stop_wait(self):
        # Test a simple execution of start, stop, wait in order

        eventlet.spawn(self.server.start)
        self.server.stop()
        self.server.wait()

        self.assertEqual(1, len(self.executors))
        self.assertEqual(['shutdown'], self.executors[0]._calls)
        self.assertTrue(self.server.listener.cleanup.called)

    def test_reversed_order(self):
        # Test that if we call wait, stop, start, these will be correctly
        # reordered

        eventlet.spawn(self.server.wait)
        # This is non-deterministic, but there's not a great deal we can do
        # about that
        eventlet.sleep(0)

        eventlet.spawn(self.server.stop)
        eventlet.sleep(0)

        eventlet.spawn(self.server.start)

        self.server.wait()

        self.assertEqual(1, len(self.executors))
        self.assertEqual(['shutdown'], self.executors[0]._calls)

    def test_wait_for_running_task(self):
        # Test that if 2 threads call a method simultaneously, both will wait,
        # but only 1 will call the underlying executor method.

        start_event = threading.Event()
        finish_event = threading.Event()

        running_event = threading.Event()
        done_event = threading.Event()

        _runner = [None]

        class SteppingFakeExecutor(self.server._executor_cls):
            def __init__(self, *args, **kwargs):
                # Tell the test which thread won the race
                _runner[0] = eventlet.getcurrent()
                running_event.set()

                start_event.wait()
                super(SteppingFakeExecutor, self).__init__(*args, **kwargs)
                done_event.set()

                finish_event.wait()

        self.server._executor_cls = SteppingFakeExecutor

        start1 = eventlet.spawn(self.server.start)
        start2 = eventlet.spawn(self.server.start)

        # Wait until one of the threads starts running
        running_event.wait()
        runner = _runner[0]
        waiter = start2 if runner == start1 else start2

        waiter_finished = threading.Event()
        waiter.link(lambda _: waiter_finished.set())

        # At this point, runner is running start(), and waiter() is waiting for
        # it to complete. runner has not yet logged anything.
        self.assertEqual(0, len(self.executors))
        self.assertFalse(waiter_finished.is_set())

        # Let the runner log the call
        start_event.set()
        done_event.wait()

        # We haven't signalled completion yet, so submit shouldn't have run
        self.assertEqual(1, len(self.executors))
        self.assertEqual([], self.executors[0]._calls)
        self.assertFalse(waiter_finished.is_set())

        # Let the runner complete
        finish_event.set()
        waiter.wait()
        runner.wait()

        # Check that both threads have finished, start was only called once,
        # and execute ran
        self.assertTrue(waiter_finished.is_set())
        self.assertEqual(1, len(self.executors))
        self.assertEqual([], self.executors[0]._calls)

    def test_start_stop_wait_stop_wait(self):
        # Test that we behave correctly when calling stop/wait more than once.
        # Subsequent calls should be noops.

        self.server.start()
        self.server.stop()
        self.server.wait()
        self.server.stop()
        self.server.wait()

        self.assertEqual(len(self.executors), 1)
        self.assertEqual(['shutdown'], self.executors[0]._calls)
        self.assertTrue(self.server.listener.cleanup.called)

    def test_state_wrapping(self):
        # Test that we behave correctly if a thread waits, and the server state
        # has wrapped when it it next scheduled

        # Ensure that if 2 threads wait for the completion of 'start', the
        # first will wait until complete_event is signalled, but the second
        # will continue
        complete_event = threading.Event()
        complete_waiting_callback = threading.Event()

        start_state = self.server._states['start']
        old_wait_for_completion = start_state.wait_for_completion
        waited = [False]

        def new_wait_for_completion(*args, **kwargs):
            if not waited[0]:
                waited[0] = True
                complete_waiting_callback.set()
                complete_event.wait()
            old_wait_for_completion(*args, **kwargs)

        start_state.wait_for_completion = new_wait_for_completion

        # thread1 will wait for start to complete until we signal it
        thread1 = eventlet.spawn(self.server.stop)
        thread1_finished = threading.Event()
        thread1.link(lambda _: thread1_finished.set())

        self.server.start()
        complete_waiting_callback.wait()

        # The server should have started, but stop should not have been called
        self.assertEqual(1, len(self.executors))
        self.assertEqual([], self.executors[0]._calls)
        self.assertFalse(thread1_finished.is_set())

        self.server.stop()
        self.server.wait()

        # We should have gone through all the states, and thread1 should still
        # be waiting
        self.assertEqual(1, len(self.executors))
        self.assertEqual(['shutdown'], self.executors[0]._calls)
        self.assertFalse(thread1_finished.is_set())

        # Start again
        self.server.start()

        # We should now record 4 executors (2 for each server)
        self.assertEqual(2, len(self.executors))
        self.assertEqual(['shutdown'], self.executors[0]._calls)
        self.assertEqual([], self.executors[1]._calls)
        self.assertFalse(thread1_finished.is_set())

        # Allow thread1 to complete
        complete_event.set()
        thread1_finished.wait()

        # thread1 should now have finished, and stop should not have been
        # called again on either the first or second executor
        self.assertEqual(2, len(self.executors))
        self.assertEqual(['shutdown'], self.executors[0]._calls)
        self.assertEqual([], self.executors[1]._calls)
        self.assertTrue(thread1_finished.is_set())

    @mock.patch.object(server_module, 'DEFAULT_LOG_AFTER', 1)
    @mock.patch.object(server_module, 'LOG')
    def test_logging(self, mock_log):
        # Test that we generate a log message if we wait longer than
        # DEFAULT_LOG_AFTER

        log_event = threading.Event()
        mock_log.warning.side_effect = lambda _, __: log_event.set()

        # Call stop without calling start. We should log a wait after 1 second
        thread = eventlet.spawn(self.server.stop)
        log_event.wait()

        # Redundant given that we already waited, but it's nice to assert
        self.assertTrue(mock_log.warning.called)
        thread.kill()

    @mock.patch.object(server_module, 'LOG')
    def test_logging_explicit_wait(self, mock_log):
        # Test that we generate a log message if we wait longer than
        # the number of seconds passed to log_after

        log_event = threading.Event()
        mock_log.warning.side_effect = lambda _, __: log_event.set()

        # Call stop without calling start. We should log a wait after 1 second
        thread = eventlet.spawn(self.server.stop, log_after=1)
        log_event.wait()

        # Redundant given that we already waited, but it's nice to assert
        self.assertTrue(mock_log.warning.called)
        thread.kill()

    @mock.patch.object(server_module, 'LOG')
    def test_logging_with_timeout(self, mock_log):
        # Test that we log a message after log_after seconds if we've also
        # specified an absolute timeout

        log_event = threading.Event()
        mock_log.warning.side_effect = lambda _, __: log_event.set()

        # Call stop without calling start. We should log a wait after 1 second
        thread = eventlet.spawn(self.server.stop, log_after=1, timeout=2)
        log_event.wait()

        # Redundant given that we already waited, but it's nice to assert
        self.assertTrue(mock_log.warning.called)
        thread.kill()

    def test_timeout_wait(self):
        # Test that we will eventually timeout when passing the timeout option
        # if a preceding condition is not satisfied.

        self.assertRaises(server_module.TaskTimeout,
                          self.server.stop, timeout=1)

    def test_timeout_running(self):
        # Test that we will eventually timeout if we're waiting for another
        # thread to complete this task

        # Start the server, which will also instantiate an executor
        self.server.start()
        self.server.stop()
        shutdown_called = threading.Event()

        # Patch the executor's stop method to be very slow
        def slow_shutdown(wait):
            shutdown_called.set()
            eventlet.sleep(10)
        self.executors[0].shutdown = slow_shutdown

        # Call wait in a new thread
        thread = eventlet.spawn(self.server.wait)

        # Wait until the thread is in the slow stop method
        shutdown_called.wait()

        # Call wait again in the main thread with a timeout
        self.assertRaises(server_module.TaskTimeout,
                          self.server.wait, timeout=1)
        thread.kill()

    @mock.patch.object(server_module, 'LOG')
    def test_log_after_zero(self, mock_log):
        # Test that we do not log a message after DEFAULT_LOG_AFTER if the
        # caller gave log_after=1

        # Call stop without calling start.
        self.assertRaises(server_module.TaskTimeout,
                          self.server.stop, log_after=0, timeout=2)

        # We timed out. Ensure we didn't log anything.
        self.assertFalse(mock_log.warning.called)


class TestRPCExposeDecorator(test_utils.BaseTestCase):

    def foo(self):
        pass

    @rpc.expose
    def bar(self):
        """bar docstring"""
        pass

    def test_undecorated(self):
        self.assertRaises(AttributeError, lambda: self.foo.exposed)

    def test_decorated(self):
        self.assertEqual(True, self.bar.exposed)
        self.assertEqual("""bar docstring""", self.bar.__doc__)
        self.assertEqual('bar', self.bar.__name__)
