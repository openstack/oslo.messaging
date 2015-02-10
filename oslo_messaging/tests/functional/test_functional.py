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

import oslo_messaging

from testtools import matchers

from oslo_messaging.tests.functional import utils


class CallTestCase(utils.SkipIfNoTransportURL):
    def test_specific_server(self):
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))
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
        if self.url.startswith("amqp:"):
            self.skipTest("QPID-6307")
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))

        client = group.client()
        data = [c for c in 'abcdefghijklmn']
        for i in data:
            client.append(text=i)

        for s in group.servers:
            self.assertThat(len(s.endpoint.sval), matchers.GreaterThan(0))
        actual = [[c for c in s.endpoint.sval] for s in group.servers]
        self.assertThat(actual, utils.IsValidDistributionOf(data))

    def test_different_exchanges(self):
        if self.url.startswith("amqp:"):
            self.skipTest("QPID-6307")
        t = self.useFixture(utils.TransportFixture(self.url))
        # If the different exchanges are not honoured, then the
        # teardown may hang unless we broadcast all control messages
        # to each server
        group1 = self.useFixture(
            utils.RpcServerGroupFixture(self.url, transport=t,
                                        use_fanout_ctrl=True))
        group2 = self.useFixture(
            utils.RpcServerGroupFixture(self.url, exchange="a",
                                        transport=t,
                                        use_fanout_ctrl=True))
        group3 = self.useFixture(
            utils.RpcServerGroupFixture(self.url, exchange="b",
                                        transport=t,
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
        transport = self.useFixture(utils.TransportFixture(self.url))
        target = oslo_messaging.Target(topic="no_such_topic")
        c = utils.ClientStub(transport.transport, target, timeout=1)
        self.assertThat(c.ping,
                        matchers.raises(oslo_messaging.MessagingTimeout))

    def test_exception(self):
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))
        client = group.client(1)
        client.add(increment=2)
        f = lambda: client.subtract(increment=3)
        self.assertThat(f, matchers.raises(ValueError))


class CastTestCase(utils.SkipIfNoTransportURL):
    # Note: casts return immediately, so these tests utilise a special
    # internal sync() cast to ensure prior casts are complete before
    # making the necessary assertions.

    def test_specific_server(self):
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))
        client = group.client(1, cast=True)
        client.append(text='open')
        client.append(text='stack')
        client.add(increment=2)
        client.add(increment=10)
        group.sync()

        self.assertEqual('openstack', group.servers[1].endpoint.sval)
        self.assertEqual(12, group.servers[1].endpoint.ival)
        for i in [0, 2]:
            self.assertEqual('', group.servers[i].endpoint.sval)
            self.assertEqual(0, group.servers[i].endpoint.ival)

    def test_server_in_group(self):
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))
        client = group.client(cast=True)
        for i in range(20):
            client.add(increment=1)
        group.sync()
        total = 0
        for s in group.servers:
            ival = s.endpoint.ival
            self.assertThat(ival, matchers.GreaterThan(0))
            self.assertThat(ival, matchers.LessThan(20))
            total += ival
        self.assertEqual(20, total)

    def test_fanout(self):
        group = self.useFixture(utils.RpcServerGroupFixture(self.url))
        client = group.client('all', cast=True)
        client.append(text='open')
        client.append(text='stack')
        client.add(increment=2)
        client.add(increment=10)
        group.sync(server='all')
        for s in group.servers:
            self.assertEqual('openstack', s.endpoint.sval)
            self.assertEqual(12, s.endpoint.ival)


class NotifyTestCase(utils.SkipIfNoTransportURL):
    # NOTE(sileht): Each test must not use the same topics
    # to be run in parallel

    def test_simple(self):
        transport = self.useFixture(utils.TransportFixture(self.url))
        listener = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['test_simple']))
        transport.wait()
        notifier = listener.notifier('abc')

        notifier.info({}, 'test', 'Hello World!')
        event = listener.events.get(timeout=1)
        self.assertEqual('info', event[0])
        self.assertEqual('test', event[1])
        self.assertEqual('Hello World!', event[2])
        self.assertEqual('abc', event[3])

    def test_multiple_topics(self):
        transport = self.useFixture(utils.TransportFixture(self.url))
        listener = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['a', 'b']))
        transport.wait()
        a = listener.notifier('pub-a', topic='a')
        b = listener.notifier('pub-b', topic='b')

        sent = {
            'pub-a': [a, 'test-a', 'payload-a'],
            'pub-b': [b, 'test-b', 'payload-b']
        }
        for e in sent.values():
            e[0].info({}, e[1], e[2])

        received = {}
        while len(received) < len(sent):
            e = listener.events.get(timeout=1)
            received[e[3]] = e

        for key in received:
            actual = received[key]
            expected = sent[key]
            self.assertEqual('info', actual[0])
            self.assertEqual(expected[1], actual[1])
            self.assertEqual(expected[2], actual[2])

    def test_multiple_servers(self):
        transport = self.useFixture(utils.TransportFixture(self.url))
        listener_a = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['test-topic']))
        listener_b = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['test-topic']))
        transport.wait()
        n = listener_a.notifier('pub')

        events_out = [('test-%s' % c, 'payload-%s' % c) for c in 'abcdefgh']

        for event_type, payload in events_out:
            n.info({}, event_type, payload)

        events_in = [[(e[1], e[2]) for e in listener_a.get_events()],
                     [(e[1], e[2]) for e in listener_b.get_events()]]

        self.assertThat(events_in, utils.IsValidDistributionOf(events_out))
        for stream in events_in:
            self.assertThat(len(stream), matchers.GreaterThan(0))

    def test_independent_topics(self):
        transport = self.useFixture(utils.TransportFixture(self.url))
        listener_a = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['1']))
        listener_b = self.useFixture(
            utils.NotificationFixture(transport.transport,
                                      ['2']))
        transport.wait()

        a = listener_a.notifier('pub-1', topic='1')
        b = listener_b.notifier('pub-2', topic='2')

        a_out = [('test-1-%s' % c, 'payload-1-%s' % c) for c in 'abcdefgh']
        for event_type, payload in a_out:
            a.info({}, event_type, payload)

        b_out = [('test-2-%s' % c, 'payload-2-%s' % c) for c in 'ijklmnop']
        for event_type, payload in b_out:
            b.info({}, event_type, payload)

        for expected in a_out:
            actual = listener_a.events.get(timeout=0.5)
            self.assertEqual('info', actual[0])
            self.assertEqual(expected[0], actual[1])
            self.assertEqual(expected[1], actual[2])
            self.assertEqual('pub-1', actual[3])

        for expected in b_out:
            actual = listener_b.events.get(timeout=0.5)
            self.assertEqual('info', actual[0])
            self.assertEqual(expected[0], actual[1])
            self.assertEqual(expected[1], actual[2])
            self.assertEqual('pub-2', actual[3])

    def test_all_categories(self):
        transport = self.useFixture(utils.TransportFixture(self.url))
        listener = self.useFixture(utils.NotificationFixture(
            transport.transport, ['test_all_categories']))
        transport.wait()
        n = listener.notifier('abc')

        cats = ['debug', 'audit', 'info', 'warn', 'error', 'critical']
        events = [(getattr(n, c), c, 'type-' + c, c + '-data') for c in cats]
        for e in events:
            e[0]({}, e[2], e[3])

        # order between events with different categories is not guaranteed
        received = {}
        for expected in events:
            e = listener.events.get(timeout=0.5)
            received[e[0]] = e

        for expected in events:
            actual = received[expected[1]]
            self.assertEqual(expected[1], actual[0])
            self.assertEqual(expected[2], actual[1])
            self.assertEqual(expected[3], actual[2])
