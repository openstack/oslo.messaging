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

import os
import queue
import time
import uuid

import fixtures
from oslo_config import cfg

import oslo_messaging
from oslo_messaging._drivers.kafka_driver import kafka_options
from oslo_messaging.notify import notifier
from oslo_messaging.tests import utils as test_utils


class TestServerEndpoint:
    """This MessagingServer that will be used during functional testing."""

    def __init__(self):
        self.ival = 0
        self.sval = ''

    def add(self, ctxt, increment):
        self.ival += increment
        return self.ival

    def subtract(self, ctxt, increment):
        if self.ival < increment:
            raise ValueError("ival can't go negative!")
        self.ival -= increment
        return self.ival

    def append(self, ctxt, text):
        self.sval += text
        return self.sval

    def long_running_task(self, ctxt, seconds):
        time.sleep(seconds)


class TransportFixture(fixtures.Fixture):
    """Fixture defined to setup the oslo_messaging transport."""

    def __init__(self, conf, url):
        self.conf = conf
        self.url = url

    def setUp(self):
        super().setUp()
        self.transport = oslo_messaging.get_transport(self.conf, url=self.url)

    def cleanUp(self):
        try:
            self.transport.cleanup()
        except fixtures.TimeoutException:
            pass
        super().cleanUp()

    def wait(self):
        # allow time for the server to connect to the broker
        time.sleep(0.5)


class RPCTransportFixture(TransportFixture):
    """Fixture defined to setup RPC transport."""

    def setUp(self):
        super().setUp()
        self.transport = oslo_messaging.get_rpc_transport(self.conf,
                                                          url=self.url)


class NotificationTransportFixture(TransportFixture):
    """Fixture defined to setup notification transport."""

    def setUp(self):
        super().setUp()
        self.transport = oslo_messaging.get_notification_transport(
            self.conf, url=self.url)


class RpcServerFixture(fixtures.Fixture):
    """Fixture to setup the TestServerEndpoint."""

    def __init__(self, conf, url, target, endpoint=None, ctrl_target=None,
                 executor='eventlet'):
        super().__init__()
        self.conf = conf
        self.url = url
        self.target = target
        self.endpoint = endpoint or TestServerEndpoint()
        self.executor = executor
        self.syncq = queue.Queue()
        self.ctrl_target = ctrl_target or self.target

    def setUp(self):
        super().setUp()
        endpoints = [self.endpoint, self]
        transport = self.useFixture(RPCTransportFixture(self.conf, self.url))
        self.server = oslo_messaging.get_rpc_server(
            transport=transport.transport,
            target=self.target,
            endpoints=endpoints,
            executor=self.executor)
        self._ctrl = oslo_messaging.get_rpc_client(transport.transport,
                                                   self.ctrl_target)
        self._start()
        transport.wait()

    def cleanUp(self):
        self._stop()
        super().cleanUp()

    def _start(self):
        self.thread = test_utils.ServerThreadHelper(self.server)
        self.thread.start()

    def _stop(self):
        self.thread.stop()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise Exception("Server did not shutdown correctly")

    def ping(self, ctxt):
        pass

    def sync(self, ctxt):
        self.syncq.put('x')


class RpcServerGroupFixture(fixtures.Fixture):
    def __init__(self, conf, url, topic=None, names=None, exchange=None,
                 use_fanout_ctrl=False, endpoint=None):
        self.conf = conf
        self.url = url
        # NOTE(sileht): topic and server_name must be unique
        # to be able to run all tests in parallel
        self.topic = topic or str(uuid.uuid4())
        self.names = names or ["server_%i_%s" % (i, str(uuid.uuid4())[:8])
                               for i in range(3)]
        self.exchange = exchange
        self.targets = [self._target(server=n) for n in self.names]
        self.use_fanout_ctrl = use_fanout_ctrl
        self.endpoint = endpoint

    def setUp(self):
        super().setUp()
        self.servers = [self.useFixture(self._server(t)) for t in self.targets]

    def _target(self, server=None, fanout=False):
        t = oslo_messaging.Target(exchange=self.exchange, topic=self.topic)
        t.server = server
        t.fanout = fanout
        return t

    def _server(self, target):
        ctrl = None
        if self.use_fanout_ctrl:
            ctrl = self._target(fanout=True)
        server = RpcServerFixture(self.conf, self.url, target,
                                  endpoint=self.endpoint,
                                  ctrl_target=ctrl)
        return server

    def client(self, server=None, cast=False):
        if server is None:
            target = self._target()
        else:
            if server == 'all':
                target = self._target(fanout=True)
            elif 0 <= server < len(self.targets):
                target = self.targets[server]
            else:
                raise ValueError("Invalid value for server: %r" % server)

        transport = self.useFixture(RPCTransportFixture(self.conf, self.url))
        client = ClientStub(transport.transport, target, cast=cast,
                            timeout=5)
        transport.wait()
        return client

    def sync(self, server=None):
        if server is None:
            for i in range(len(self.servers)):
                self.client(i).ping()
        else:
            if server == 'all':
                for s in self.servers:
                    s.syncq.get(timeout=5)
            elif 0 <= server < len(self.targets):
                self.servers[server].syncq.get(timeout=5)
            else:
                raise ValueError("Invalid value for server: %r" % server)


class RpcCall:
    def __init__(self, client, method, context):
        self.client = client
        self.method = method
        self.context = context

    def __call__(self, **kwargs):
        self.context['time'] = time.ctime()
        self.context['cast'] = False
        result = self.client.call(self.context, self.method, **kwargs)
        return result


class RpcCast(RpcCall):
    def __call__(self, **kwargs):
        self.context['time'] = time.ctime()
        self.context['cast'] = True
        self.client.cast(self.context, self.method, **kwargs)


class ClientStub:
    def __init__(self, transport, target, cast=False, name=None,
                 transport_options=None, **kwargs):
        self.name = name or "functional-tests"
        self.cast = cast
        self.client = oslo_messaging.get_rpc_client(
            transport=transport,
            target=target,
            transport_options=transport_options,
            **kwargs)

    def __getattr__(self, name):
        context = {"application": self.name}
        if self.cast:
            return RpcCast(self.client, name, context)
        else:
            return RpcCall(self.client, name, context)


class InvalidDistribution:
    def __init__(self, original, received):
        self.original = original
        self.received = received
        self.missing = []
        self.extra = []
        self.wrong_order = []

    def describe(self):
        text = "Sent {}, got {}; ".format(self.original, self.received)
        e1 = ["%r was missing" % m for m in self.missing]
        e2 = ["%r was not expected" % m for m in self.extra]
        e3 = ["{!r} expected before {!r}".format(
            m[0], m[1]) for m in self.wrong_order]
        return text + ", ".join(e1 + e2 + e3)

    def __len__(self):
        return len(self.extra) + len(self.missing) + len(self.wrong_order)

    def get_details(self):
        return {}


class IsValidDistributionOf:
    """Test whether a given list can be split into particular
    sub-lists. All items in the original list must be in exactly one
    sub-list, and must appear in that sub-list in the same order with
    respect to any other items as in the original list.
    """
    def __init__(self, original):
        self.original = original

    def __str__(self):
        return 'IsValidDistribution(%s)' % self.original

    def match(self, actual):
        errors = InvalidDistribution(self.original, actual)
        received = [[idx for idx in act] for act in actual]

        def _remove(obj, lists):
            for li in lists:
                if obj in li:
                    front = li[0]
                    li.remove(obj)
                    return front
            return None

        for item in self.original:
            o = _remove(item, received)
            if not o:
                errors.missing += item
            elif item != o:
                errors.wrong_order.append([item, o])
        for li in received:
            errors.extra += li
        return errors or None


class SkipIfNoTransportURL(test_utils.BaseTestCase):
    def setUp(self, conf=cfg.CONF):
        super().setUp(conf=conf)

        self.rpc_url = os.environ.get('RPC_TRANSPORT_URL')
        self.notify_url = os.environ.get('NOTIFY_TRANSPORT_URL')

        if not (self.rpc_url or self.notify_url):
            self.skipTest("No transport url configured")

        transport_url = oslo_messaging.TransportURL.parse(conf,
                                                          self.notify_url)

        kafka_options.register_opts(conf, transport_url)


class NotificationFixture(fixtures.Fixture):
    def __init__(self, conf, url, topics, batch=None):
        super().__init__()
        self.conf = conf
        self.url = url
        self.topics = topics
        self.events = queue.Queue()
        self.name = str(id(self))
        self.batch = batch

    def setUp(self):
        super().setUp()
        targets = [oslo_messaging.Target(topic=t) for t in self.topics]
        # add a special topic for internal notifications
        targets.append(oslo_messaging.Target(topic=self.name))
        transport = self.useFixture(NotificationTransportFixture(self.conf,
                                                                 self.url))
        self.server = self._get_server(transport, targets)
        self._ctrl = self.notifier('internal', topics=[self.name])
        self._start()
        transport.wait()

    def cleanUp(self):
        self._stop()
        super().cleanUp()

    def _get_server(self, transport, targets):
        return oslo_messaging.get_notification_listener(
            transport.transport,
            targets,
            [self], 'eventlet')

    def _start(self):
        self.thread = test_utils.ServerThreadHelper(self.server)
        self.thread.start()

    def _stop(self):
        self.thread.stop()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise Exception("Server did not shutdown properly")

    def notifier(self, publisher, topics=None):
        transport = self.useFixture(NotificationTransportFixture(self.conf,
                                                                 self.url))
        n = notifier.Notifier(transport.transport,
                              publisher,
                              driver='messaging',
                              topics=topics or self.topics)
        transport.wait()
        return n

    def debug(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['debug', event_type, payload, publisher])

    def audit(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['audit', event_type, payload, publisher])

    def info(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['info', event_type, payload, publisher])

    def warn(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['warn', event_type, payload, publisher])

    def error(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['error', event_type, payload, publisher])

    def critical(self, ctxt, publisher, event_type, payload, metadata):
        self.events.put(['critical', event_type, payload, publisher])

    def sample(self, ctxt, publisher, event_type, payload, metadata):
        pass  # Just used for internal shutdown control

    def get_events(self, timeout=0.5):
        results = []
        try:
            while True:
                results.append(self.events.get(timeout=timeout))
        except queue.Empty:
            pass
        return results


class BatchNotificationFixture(NotificationFixture):
    def __init__(self, conf, url, topics, batch_size=5, batch_timeout=2):
        super().__init__(conf, url, topics)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    def _get_server(self, transport, targets):
        return oslo_messaging.get_batch_notification_listener(
            transport.transport,
            targets,
            [self], 'eventlet',
            batch_timeout=self.batch_timeout,
            batch_size=self.batch_size)

    def debug(self, messages):
        self.events.put(['debug', messages])

    def audit(self, messages):
        self.events.put(['audit', messages])

    def info(self, messages):
        self.events.put(['info', messages])

    def warn(self, messages):
        self.events.put(['warn', messages])

    def error(self, messages):
        self.events.put(['error', messages])

    def critical(self, messages):
        self.events.put(['critical', messages])

    def sample(self, messages):
        pass  # Just used for internal shutdown control
