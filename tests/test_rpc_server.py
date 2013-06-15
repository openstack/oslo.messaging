
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

import threading

from oslo.config import cfg

from oslo import messaging
from tests import utils as test_utils


class TestRPCServer(test_utils.BaseTestCase):

    class Server(object):
        def __init__(self, transport, topic, server, endpoint, serializer):
            target = messaging.Target(topic=topic, server=server)
            self._server = messaging.get_rpc_server(transport,
                                                    target,
                                                    [endpoint, self],
                                                    serializer=serializer)

        def stop(self, ctxt):
            self._server.stop()

        def start(self):
            self._server.start()

    class TestSerializer(object):

        def serialize_entity(self, ctxt, entity):
            return 's' + (entity or '')

        def deserialize_entity(self, ctxt, entity):
            return 'd' + (entity or '')

    def setUp(self):
        super(TestRPCServer, self).setUp(conf=cfg.ConfigOpts())
        self.serializer = self.TestSerializer()

    def _setup_server(self, transport, endpoint, topic='testtopic'):
        server = self.Server(transport,
                             topic=topic,
                             server='testserver',
                             endpoint=endpoint,
                             serializer=self.serializer)

        thread = threading.Thread(target=server.start)
        thread.daemon = True
        thread.start()

        return thread

    def _stop_server(self, client, server_thread, topic=None):
        if topic is not None:
            client = client.prepare(topic=topic)
        client.cast({}, 'stop')
        server_thread.join(timeout=30)

    def _setup_client(self, transport):
        return messaging.RPCClient(transport,
                                   messaging.Target(topic='testtopic'),
                                   serializer=self.serializer)

    def test_constructor(self):
        transport = messaging.get_transport(self.conf, url='fake:')
        target = messaging.Target(topic='foo', server='bar')
        endpoints = [object()]
        serializer = object()

        server = messaging.get_rpc_server(transport, target, endpoints,
                                          serializer=serializer)

        self.assertTrue(server.conf is self.conf)
        self.assertTrue(server.transport is transport)
        self.assertTrue(server.target is target)
        self.assertTrue(isinstance(server.dispatcher, messaging.RPCDispatcher))
        self.assertTrue(server.dispatcher.endpoints is endpoints)
        self.assertTrue(server.dispatcher.serializer is serializer)
        self.assertTrue(server.executor is 'blocking')

    def test_no_target_server(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        server = messaging.get_rpc_server(transport,
                                          messaging.Target(topic='testtopic'),
                                          [])
        try:
            server.start()
        except Exception as ex:
            self.assertTrue(isinstance(ex, messaging.ServerListenError), ex)
            self.assertEquals(ex.target.topic, 'testtopic')
        else:
            self.assertTrue(False)

    def test_no_target_topic(self):
        transport = messaging.get_transport(self.conf, url='fake:')
        target = messaging.Target(server='testserver')
        server = messaging.get_rpc_server(transport, target, [])
        try:
            server.start()
        except Exception as ex:
            self.assertTrue(isinstance(ex, messaging.ServerListenError), ex)
            self.assertEquals(ex.target.server, 'testserver')
        else:
            self.assertTrue(False)

    def test_unknown_executor(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        try:
            messaging.get_rpc_server(transport, None, [], executor='foo')
        except Exception as ex:
            self.assertTrue(isinstance(ex, messaging.ExecutorLoadFailure))
            self.assertEquals(ex.executor, 'foo')
        else:
            self.assertTrue(False)

    def test_cast(self):
        transport = messaging.get_transport(self.conf, url='fake:')

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

        self.assertEquals(endpoint.pings, ['dsfoo', 'dsbar'])

    def test_call(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                return arg

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        self.assertEquals(client.call({}, 'ping', arg='foo'), 'dsdsfoo')

        self._stop_server(client, server_thread)

    def test_direct_call(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                return arg

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        direct = client.prepare(server='testserver')
        self.assertEquals(direct.call({}, 'ping', arg='foo'), 'dsdsfoo')

        self._stop_server(client, server_thread)

    def test_context(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def ctxt_check(self, ctxt, key):
                return ctxt[key]

        server_thread = self._setup_server(transport, TestEndpoint())
        client = self._setup_client(transport)

        self.assertEquals(client.call({'dsa': 'b'},
                                      'ctxt_check',
                                      key='a'),
                          'dsb')

        self._stop_server(client, server_thread)

    def test_multiple_servers(self):
        transport = messaging.get_transport(self.conf, url='fake:')

        class TestEndpoint(object):
            def __init__(self):
                self.pings = []

            def ping(self, ctxt, arg):
                self.pings.append(arg)

        endpoint = TestEndpoint()

        thread1 = self._setup_server(transport, endpoint, topic='topic1')
        thread2 = self._setup_server(transport, endpoint, topic='topic2')

        client = self._setup_client(transport)

        client.prepare(topic='topic1').cast({}, 'ping', arg='1')
        client.prepare(topic='topic2').cast({}, 'ping', arg='2')

        self.assertTrue(thread1.isAlive())
        self._stop_server(client, thread1, topic='topic1')
        self.assertTrue(thread2.isAlive())
        self._stop_server(client, thread2, topic='topic2')

        self.assertEquals(len(endpoint.pings), 2)
        self.assertTrue('ds1' in endpoint.pings)
        self.assertTrue('ds2' in endpoint.pings)
