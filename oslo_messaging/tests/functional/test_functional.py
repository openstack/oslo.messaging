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

import requests
import subprocess
import time
import uuid

import concurrent.futures
from oslo_config import cfg
from testtools import matchers

import oslo_messaging
from oslo_messaging.tests.functional import utils


class CallTestCase(utils.SkipIfNoTransportURL):

    def setUp(self):
        super().setUp(conf=cfg.ConfigOpts())
        if self.rpc_url.startswith("kafka://"):
            self.skipTest("kafka does not support RPC API")

        self.conf.prog = "test_prog"
        self.conf.project = "test_project"

        self.config(heartbeat_timeout_threshold=0,
                    group='oslo_messaging_rabbit')

    def test_specific_server(self):
        group = self.useFixture(utils.RpcServerGroupFixture(
            self.conf, self.rpc_url)
        )
        client = group.client(1)
        client.append(text='open')
        self.assertEqual('openstack', client.append(text='stack'))
        client.add(increment=2)
        self.assertEqual(12, client.add(increment=10))
        self.assertEqual(9, client.subtract(increment=3))
        self.assertEqual('openstack', group.servers[1].endpoint.sval)
        self.assertEqual(9, group.servers[1].endpoint.ival)
        for i in [0, 2]:
            self.assertEqual('', group.servers[i].endpoint.sval)
            self.assertEqual(0, group.servers[i].endpoint.ival)

    def test_server_in_group(self):
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url)
        )

        client = group.client()
        data = [c for c in 'abcdefghijklmn']
        for i in data:
            client.append(text=i)

        for s in group.servers:
            self.assertThat(len(s.endpoint.sval), matchers.GreaterThan(0))
        actual = [[c for c in s.endpoint.sval] for s in group.servers]
        self.assertThat(actual, utils.IsValidDistributionOf(data))

    def test_different_exchanges(self):
        # If the different exchanges are not honoured, then the
        # teardown may hang unless we broadcast all control messages
        # to each server
        group1 = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url,
                                        use_fanout_ctrl=True))
        group2 = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url, exchange="a",
                                        use_fanout_ctrl=True))
        group3 = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url, exchange="b",
                                        use_fanout_ctrl=True))

        client1 = group1.client(1)
        data1 = [c for c in 'abcdefghijklmn']
        for i in data1:
            client1.append(text=i)

        client2 = group2.client()
        data2 = [c for c in 'opqrstuvwxyz']
        for i in data2:
            client2.append(text=i)

        actual1 = [[c for c in s.endpoint.sval] for s in group1.servers]
        self.assertThat(actual1, utils.IsValidDistributionOf(data1))
        actual1 = [c for c in group1.servers[1].endpoint.sval]
        self.assertThat([actual1], utils.IsValidDistributionOf(data1))
        for s in group1.servers:
            expected = len(data1) if group1.servers.index(s) == 1 else 0
            self.assertEqual(expected, len(s.endpoint.sval))
            self.assertEqual(0, s.endpoint.ival)

        actual2 = [[c for c in s.endpoint.sval] for s in group2.servers]
        for s in group2.servers:
            self.assertThat(len(s.endpoint.sval), matchers.GreaterThan(0))
            self.assertEqual(0, s.endpoint.ival)
        self.assertThat(actual2, utils.IsValidDistributionOf(data2))

        for s in group3.servers:
            self.assertEqual(0, len(s.endpoint.sval))
            self.assertEqual(0, s.endpoint.ival)

    def test_timeout(self):
        transport = self.useFixture(
            utils.RPCTransportFixture(self.conf, self.rpc_url)
        )
        target = oslo_messaging.Target(topic="no_such_topic")
        c = utils.ClientStub(transport.transport, target, timeout=1)
        self.assertThat(c.ping,
                        matchers.raises(oslo_messaging.MessagingTimeout))

    def test_exception(self):
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url)
        )
        client = group.client(1)
        client.add(increment=2)
        self.assertRaises(ValueError, client.subtract, increment=3)

    def test_timeout_with_concurrently_queues(self):
        transport = self.useFixture(
            utils.RPCTransportFixture(self.conf, self.rpc_url)
        )
        target = oslo_messaging.Target(topic="topic_" + str(uuid.uuid4()),
                                       server="server_" + str(uuid.uuid4()))
        server = self.useFixture(
            utils.RpcServerFixture(self.conf, self.rpc_url, target,
                                   executor="threading"))
        client = utils.ClientStub(transport.transport, target,
                                  cast=False, timeout=5)

        def short_periodical_tasks():
            for i in range(10):
                client.add(increment=1)
                time.sleep(1)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future = executor.submit(client.long_running_task, seconds=10)
            executor.submit(short_periodical_tasks)
            self.assertRaises(oslo_messaging.MessagingTimeout, future.result)

        self.assertEqual(10, server.endpoint.ival)

    def test_mandatory_call(self):
        if not self.rpc_url.startswith("rabbit://"):
            self.skipTest("backend does not support call monitoring")

        transport = self.useFixture(utils.RPCTransportFixture(self.conf,
                                                              self.rpc_url))
        target = oslo_messaging.Target(topic='topic_' + str(uuid.uuid4()),
                                       server='server_' + str(uuid.uuid4()))

        # test for mandatory flag using transport-options, see:
        # https://blueprints.launchpad.net/oslo.messaging/+spec/transport-options
        # first test with `at_least_once=False` raises a "MessagingTimeout"
        # error since there is no control if the queue actually exists.
        # (Default behavior)
        options = oslo_messaging.TransportOptions(at_least_once=False)
        client1 = utils.ClientStub(transport.transport, target,
                                   cast=False, timeout=1,
                                   transport_options=options)

        self.assertRaises(oslo_messaging.MessagingTimeout,
                          client1.delay)

        # second test with `at_least_once=True` raises a "MessageUndeliverable"
        # caused by mandatory flag.
        # the MessageUndeliverable error is raised immediately without waiting
        # any timeout
        options2 = oslo_messaging.TransportOptions(at_least_once=True)
        client2 = utils.ClientStub(transport.transport, target,
                                   cast=False, timeout=60,
                                   transport_options=options2)

        self.assertRaises(oslo_messaging.MessageUndeliverable,
                          client2.delay)

    def test_monitor_long_call(self):
        if not (self.rpc_url.startswith("rabbit://") or
                self.rpc_url.startswith("amqp://")):
            self.skipTest("backend does not support call monitoring")

        transport = self.useFixture(utils.RPCTransportFixture(self.conf,
                                                              self.rpc_url))
        target = oslo_messaging.Target(topic='topic_' + str(uuid.uuid4()),
                                       server='server_' + str(uuid.uuid4()))

        class _endpoint:
            def delay(self, ctxt, seconds):
                time.sleep(seconds)
                return seconds

        self.useFixture(
            utils.RpcServerFixture(self.conf, self.rpc_url, target,
                                   executor='threading',
                                   endpoint=_endpoint()))

        # First case, no monitoring, ensure we timeout normally when the
        # server side runs long
        client1 = utils.ClientStub(transport.transport, target,
                                   cast=False, timeout=1)
        self.assertRaises(oslo_messaging.MessagingTimeout,
                          client1.delay, seconds=4)

        # Second case, set a short call monitor timeout and a very
        # long overall timeout. If we didn't honor the call monitor
        # timeout, we would wait an hour, past the test timeout. If
        # the server was not sending message heartbeats, we'd time out
        # after two seconds.
        client2 = utils.ClientStub(transport.transport, target,
                                   cast=False, timeout=3600,
                                   call_monitor_timeout=2)
        self.assertEqual(4, client2.delay(seconds=4))

    def test_endpoint_version_namespace(self):
        # verify endpoint version and namespace are checked
        target = oslo_messaging.Target(topic="topic_" + str(uuid.uuid4()),
                                       server="server_" + str(uuid.uuid4()),
                                       namespace="Name1",
                                       version="7.5")

        class _endpoint:
            def __init__(self, target):
                self.target = target()

            def test(self, ctxt, echo):
                return echo

        transport = self.useFixture(
            utils.RPCTransportFixture(self.conf, self.rpc_url)
        )
        self.useFixture(
            utils.RpcServerFixture(self.conf, self.rpc_url, target,
                                   executor="threading",
                                   endpoint=_endpoint(target)))
        client1 = utils.ClientStub(transport.transport, target,
                                   cast=False, timeout=5)
        self.assertEqual("Hi there", client1.test(echo="Hi there"))

        # unsupported version
        target2 = target()
        target2.version = "7.6"
        client2 = utils.ClientStub(transport.transport,
                                   target2,
                                   cast=False, timeout=5)
        self.assertRaises(oslo_messaging.rpc.client.RemoteError,
                          client2.test,
                          echo="Expect failure")

        # no matching namespace
        target3 = oslo_messaging.Target(topic=target.topic,
                                        server=target.server,
                                        version=target.version,
                                        namespace="Name2")
        client3 = utils.ClientStub(transport.transport,
                                   target3,
                                   cast=False, timeout=5)
        self.assertRaises(oslo_messaging.rpc.client.RemoteError,
                          client3.test,
                          echo="Expect failure")

    def test_bad_endpoint(self):
        # 'target' attribute is reserved and should be of type Target

        class _endpoint:
            def target(self, ctxt, echo):
                return echo

        target = oslo_messaging.Target(topic="topic_" + str(uuid.uuid4()),
                                       server="server_" + str(uuid.uuid4()))
        transport = self.useFixture(
            utils.RPCTransportFixture(self.conf, self.rpc_url)
        )
        self.assertRaises(TypeError,
                          oslo_messaging.get_rpc_server,
                          transport=transport.transport,
                          target=target,
                          endpoints=[_endpoint()],
                          executor="threading")


class CastTestCase(utils.SkipIfNoTransportURL):
    # Note: casts return immediately, so these tests utilise a special
    # internal sync() cast to ensure prior casts are complete before
    # making the necessary assertions.

    def setUp(self):
        super().setUp()
        if self.rpc_url.startswith("kafka://"):
            self.skipTest("kafka does not support RPC API")

    def test_specific_server(self):
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url)
        )
        client = group.client(1, cast=True)
        client.append(text='open')
        client.append(text='stack')
        client.add(increment=2)
        client.add(increment=10)
        time.sleep(0.3)
        client.sync()

        group.sync(1)
        self.assertIn(group.servers[1].endpoint.sval,
                      ["openstack", "stackopen"])
        self.assertEqual(12, group.servers[1].endpoint.ival)
        for i in [0, 2]:
            self.assertEqual('', group.servers[i].endpoint.sval)
            self.assertEqual(0, group.servers[i].endpoint.ival)

    def test_server_in_group(self):
        if self.rpc_url.startswith("amqp:"):
            self.skipTest("QPID-6307")
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url)
        )
        client = group.client(cast=True)
        for i in range(20):
            client.add(increment=1)
        for i in range(len(group.servers)):
            # expect each server to get a sync
            client.sync()
        group.sync(server="all")
        total = 0
        for s in group.servers:
            ival = s.endpoint.ival
            self.assertThat(ival, matchers.GreaterThan(0))
            self.assertThat(ival, matchers.LessThan(20))
            total += ival
        self.assertEqual(20, total)

    def test_fanout(self):
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url)
        )
        client = group.client('all', cast=True)
        client.append(text='open')
        client.append(text='stack')
        client.add(increment=2)
        client.add(increment=10)
        time.sleep(0.3)
        client.sync()
        group.sync(server='all')
        for s in group.servers:
            self.assertIn(s.endpoint.sval, ["openstack", "stackopen"])
            self.assertEqual(12, s.endpoint.ival)


class NotifyTestCase(utils.SkipIfNoTransportURL):
    # NOTE(sileht): Each test must not use the same topics
    # to be run in parallel

    # NOTE(ansmith): kafka partition assignment delay requires
    # longer timeouts for test completion

    def test_simple(self):
        get_timeout = 1
        if self.notify_url.startswith("kafka://"):
            get_timeout = 5
            self.conf.set_override('consumer_group', 'test_simple',
                                   group='oslo_messaging_kafka')

        listener = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url,
                                      ['test_simple']))
        notifier = listener.notifier('abc')

        notifier.info({}, 'test', 'Hello World!')
        event = listener.events.get(timeout=get_timeout)
        self.assertEqual('info', event[0])
        self.assertEqual('test', event[1])
        self.assertEqual('Hello World!', event[2])
        self.assertEqual('abc', event[3])

    def test_multiple_topics(self):
        get_timeout = 1
        if self.notify_url.startswith("kafka://"):
            get_timeout = 5
            self.conf.set_override('consumer_group', 'test_multiple_topics',
                                   group='oslo_messaging_kafka')

        listener = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url, ['a', 'b']))
        a = listener.notifier('pub-a', topics=['a'])
        b = listener.notifier('pub-b', topics=['b'])

        sent = {
            'pub-a': [a, 'test-a', 'payload-a'],
            'pub-b': [b, 'test-b', 'payload-b']
        }
        for e in sent.values():
            e[0].info({}, e[1], e[2])

        received = {}
        while len(received) < len(sent):
            e = listener.events.get(timeout=get_timeout)
            received[e[3]] = e

        for key in received:
            actual = received[key]
            expected = sent[key]
            self.assertEqual('info', actual[0])
            self.assertEqual(expected[1], actual[1])
            self.assertEqual(expected[2], actual[2])

    def test_multiple_servers(self):
        timeout = 0.5
        if self.notify_url.startswith("amqp:"):
            self.skipTest("QPID-6307")
        if self.notify_url.startswith("kafka://"):
            self.skipTest("Kafka: needs to be fixed")
            timeout = 5
            self.conf.set_override('consumer_group',
                                   'test_multiple_servers',
                                   group='oslo_messaging_kafka')

        listener_a = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url,
                                      ['test-topic']))

        listener_b = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url,
                                      ['test-topic']))

        n = listener_a.notifier('pub')

        events_out = [('test-%s' % c, 'payload-%s' % c) for c in 'abcdefgh']
        for event_type, payload in events_out:
            n.info({}, event_type, payload)

        events_in = [[(e[1], e[2]) for e in listener_a.get_events(timeout)],
                     [(e[1], e[2]) for e in listener_b.get_events(timeout)]]

        self.assertThat(events_in, utils.IsValidDistributionOf(events_out))
        for stream in events_in:
            self.assertThat(len(stream), matchers.GreaterThan(0))

    def test_independent_topics(self):
        get_timeout = 0.5
        if self.notify_url.startswith("kafka://"):
            get_timeout = 5
            self.conf.set_override('consumer_group',
                                   'test_independent_topics_a',
                                   group='oslo_messaging_kafka')
        listener_a = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url, ['1']))

        if self.notify_url.startswith("kafka://"):
            self.conf.set_override('consumer_group',
                                   'test_independent_topics_b',
                                   group='oslo_messaging_kafka')
        listener_b = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url, ['2']))

        a = listener_a.notifier('pub-1', topics=['1'])
        b = listener_b.notifier('pub-2', topics=['2'])

        a_out = [('test-1-%s' % c, 'payload-1-%s' % c) for c in 'abcdefgh']
        for event_type, payload in a_out:
            a.info({}, event_type, payload)

        b_out = [('test-2-%s' % c, 'payload-2-%s' % c) for c in 'ijklmnop']
        for event_type, payload in b_out:
            b.info({}, event_type, payload)

        def check_received(listener, publisher, messages):
            actuals = sorted([listener.events.get(timeout=get_timeout)
                              for __ in range(len(a_out))])
            expected = sorted([['info', m[0], m[1], publisher]
                               for m in messages])
            self.assertEqual(expected, actuals)

        check_received(listener_a, "pub-1", a_out)
        check_received(listener_b, "pub-2", b_out)

    def test_all_categories(self):
        get_timeout = 1
        if self.notify_url.startswith("kafka://"):
            get_timeout = 5
            self.conf.set_override('consumer_group', 'test_all_categories',
                                   group='oslo_messaging_kafka')

        listener = self.useFixture(utils.NotificationFixture(
            self.conf, self.notify_url, ['test_all_categories']))
        n = listener.notifier('abc')

        cats = ['debug', 'audit', 'info', 'warn', 'error', 'critical']
        events = [(getattr(n, c), c, 'type-' + c, c + '-data') for c in cats]
        for e in events:
            e[0]({}, e[2], e[3])

        # order between events with different categories is not guaranteed
        received = {}
        for expected in events:
            e = listener.events.get(timeout=get_timeout)
            received[e[0]] = e

        for expected in events:
            actual = received[expected[1]]
            self.assertEqual(expected[1], actual[0])
            self.assertEqual(expected[2], actual[1])
            self.assertEqual(expected[3], actual[2])

    def test_simple_batch(self):
        get_timeout = 3
        batch_timeout = 2
        if self.notify_url.startswith("kafka://"):
            get_timeout = 10
            batch_timeout = 5
            self.conf.set_override('consumer_group', 'test_simple_batch',
                                   group='oslo_messaging_kafka')

        listener = self.useFixture(
            utils.BatchNotificationFixture(self.conf, self.notify_url,
                                           ['test_simple_batch'],
                                           batch_size=100,
                                           batch_timeout=batch_timeout))
        notifier = listener.notifier('abc')

        for i in range(0, 205):
            notifier.info({}, 'test%s' % i, 'Hello World!')
        events = listener.get_events(timeout=get_timeout)
        self.assertEqual(3, len(events))
        self.assertEqual(100, len(events[0][1]))
        self.assertEqual(100, len(events[1][1]))
        self.assertEqual(5, len(events[2][1]))

    def test_compression(self):
        get_timeout = 1
        if self.notify_url.startswith("amqp:"):
            self.conf.set_override('kombu_compression', 'gzip',
                                   group='oslo_messaging_rabbit')
        if self.notify_url.startswith("kafka://"):
            get_timeout = 5
            self.conf.set_override('compression_codec', 'gzip',
                                   group='oslo_messaging_kafka')
            self.conf.set_override('consumer_group', 'test_compression',
                                   group='oslo_messaging_kafka')

        listener = self.useFixture(
            utils.NotificationFixture(self.conf, self.notify_url,
                                      ['test_compression']))
        notifier = listener.notifier('abc')

        notifier.info({}, 'test', 'Hello World!')
        event = listener.events.get(timeout=get_timeout)
        self.assertEqual('info', event[0])
        self.assertEqual('test', event[1])
        self.assertEqual('Hello World!', event[2])
        self.assertEqual('abc', event[3])


class MetricsTestCase(utils.SkipIfNoTransportURL):

    def setUp(self):
        super().setUp(conf=cfg.ConfigOpts())
        if self.rpc_url.startswith("kafka://"):
            self.skipTest("kafka does not support RPC API")

        self.config(metrics_enabled=True,
                    group='oslo_messaging_metrics')

    def test_functional(self):
        # verify call metrics is sent and reflected in oslo.metrics
        self.config(metrics_socket_file='/var/tmp/metrics_collector.sock',
                    group='oslo_messaging_metrics')
        metric_server = subprocess.Popen(["python3", "-m", "oslo_metrics"])
        time.sleep(1)
        group = self.useFixture(
            utils.RpcServerGroupFixture(self.conf, self.rpc_url))
        client = group.client(1)
        client.add(increment=1)
        time.sleep(1)
        r = requests.get('http://localhost:3000', timeout=10)
        for line in r.text.split('\n'):
            if 'client_invocation_start_total{' in line:
                self.assertEqual('1.0', line[-3:])
            elif 'client_invocation_end_total{' in line:
                self.assertEqual('1.0', line[-3:])
            elif 'client_processing_seconds_count{' in line:
                self.assertEqual('1.0', line[-3:])
        metric_server.terminate()
