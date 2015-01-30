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
import threading
import time
import uuid

import fixtures
from oslo_config import cfg
from six import moves

import oslo_messaging
from oslo_messaging.notify import notifier
from oslo_messaging.tests import utils as test_utils


class TestServerEndpoint(object):
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


class TransportFixture(fixtures.Fixture):
    """Fixture defined to setup the oslo_messaging transport."""

    def __init__(self, url):
        self.url = url

    def setUp(self):
        super(TransportFixture, self).setUp()
        self.transport = oslo_messaging.get_transport(cfg.CONF, url=self.url)

    def cleanUp(self):
        self.transport.cleanup()
        super(TransportFixture, self).cleanUp()

    def wait(self):
        if self.url.startswith("rabbit") or self.url.startswith("qpid"):
            time.sleep(0.5)


class RpcServerFixture(fixtures.Fixture):
    """Fixture to setup the TestServerEndpoint."""

    def __init__(self, transport, target, endpoint=None, ctrl_target=None):
        super(RpcServerFixture, self).__init__()
        self.transport = transport
        self.target = target
        self.endpoint = endpoint or TestServerEndpoint()
        self.syncq = moves.queue.Queue()
        self.ctrl_target = ctrl_target or self.target

    def setUp(self):
        super(RpcServerFixture, self).setUp()
        endpoints = [self.endpoint, self]
        self.server = oslo_messaging.get_rpc_server(self.transport,
                                                    self.target,
                                                    endpoints)
        self._ctrl = oslo_messaging.RPCClient(self.transport, self.ctrl_target)
        self._start()

    def cleanUp(self):
        self._stop()
        super(RpcServerFixture, self).cleanUp()

    def _start(self):
        self.thread = threading.Thread(target=self.server.start)
        self.thread.daemon = True
        self.thread.start()

    def _stop(self):
        self.server.stop()
        self._ctrl.cast({}, 'ping')
        self.server.wait()
        self.thread.join()

    def ping(self, ctxt):
        pass

    def sync(self, ctxt, item):
        self.syncq.put(item)


class RpcServerGroupFixture(fixtures.Fixture):
    def __init__(self, url, topic=None, names=None, exchange=None,
                 transport=None, use_fanout_ctrl=False):
        self.url = url
        # NOTE(sileht): topic and servier_name must be uniq
        # to be able to run all tests in parallel
        self.topic = topic or str(uuid.uuid4())
        self.names = names or ["server_%i_%s" % (i, uuid.uuid4())
                               for i in range(3)]
        self.exchange = exchange
        self.targets = [self._target(server=n) for n in self.names]
        self.transport = transport
        self.use_fanout_ctrl = use_fanout_ctrl

    def setUp(self):
        super(RpcServerGroupFixture, self).setUp()
        if not self.transport:
            self.transport = self.useFixture(TransportFixture(self.url))
        self.servers = [self.useFixture(self._server(t)) for t in self.targets]
        self.transport.wait()

    def _target(self, server=None, fanout=False):
        t = oslo_messaging.Target(exchange=self.exchange, topic=self.topic)
        t.server = server
        t.fanout = fanout
        return t

    def _server(self, target):
        ctrl = None
        if self.use_fanout_ctrl:
            ctrl = self._target(fanout=True)
        return RpcServerFixture(self.transport.transport, target,
                                ctrl_target=ctrl)

    def client(self, server=None, cast=False):
        if server:
            if server == 'all':
                target = self._target(fanout=True)
            elif server >= 0 and server < len(self.targets):
                target = self.targets[server]
            else:
                raise ValueError("Invalid value for server: %r" % server)
        else:
            target = self._target()
        return ClientStub(self.transport.transport, target, cast=cast,
                          timeout=5)

    def sync(self, server=None):
        if server:
            if server == 'all':
                c = self.client(server='all', cast=True)
                c.sync(item='x')
                for s in self.servers:
                    s.syncq.get(timeout=5)
            elif server >= 0 and server < len(self.targets):
                c = self.client(server=server, cast=True)
                c.sync(item='x')
                self.servers[server].syncq.get(timeout=5)
            else:
                raise ValueError("Invalid value for server: %r" % server)
        else:
            for i in range(len(self.servers)):
                self.client(i).ping()


class RpcCall(object):
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


class ClientStub(object):
    def __init__(self, transport, target, cast=False, name=None, **kwargs):
        self.name = name or "functional-tests"
        self.cast = cast
        self.client = oslo_messaging.RPCClient(transport, target, **kwargs)

    def __getattr__(self, name):
        context = {"application": self.name}
        if self.cast:
            return RpcCast(self.client, name, context)
        else:
            return RpcCall(self.client, name, context)


class InvalidDistribution(object):
    def __init__(self, original, received):
        self.original = original
        self.received = received
        self.missing = []
        self.extra = []
        self.wrong_order = []

    def describe(self):
        text = "Sent %s, got %s; " % (self.original, self.received)
        e1 = ["%r was missing" % m for m in self.missing]
        e2 = ["%r was not expected" % m for m in self.extra]
        e3 = ["%r expected before %r" % (m[0], m[1]) for m in self.wrong_order]
        return text + ", ".join(e1 + e2 + e3)

    def __len__(self):
        return len(self.extra) + len(self.missing) + len(self.wrong_order)

    def get_details(self):
        return {}


class IsValidDistributionOf(object):
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
        received = [[i for i in l] for l in actual]

        def _remove(obj, lists):
            for l in lists:
                if obj in l:
                    front = l[0]
                    l.remove(obj)
                    return front
            return None

        for item in self.original:
            o = _remove(item, received)
            if not o:
                errors.missing += item
            elif item != o:
                errors.wrong_order.append([item, o])
        for l in received:
            errors.extra += l
        return errors or None


class SkipIfNoTransportURL(test_utils.BaseTestCase):
    def setUp(self):
        super(SkipIfNoTransportURL, self).setUp()
        self.url = os.environ.get('TRANSPORT_URL')
        if not self.url:
            self.skipTest("No transport url configured")
        zmq_matchmaker = os.environ.get('ZMQ_MATCHMAKER')
        if zmq_matchmaker:
            self.conf.rpc_zmq_matchmaker = zmq_matchmaker


class NotificationFixture(fixtures.Fixture):
    def __init__(self, transport, topics):
        super(NotificationFixture, self).__init__()
        self.transport = transport
        self.topics = topics
        self.events = moves.queue.Queue()
        self.name = str(id(self))

    def setUp(self):
        super(NotificationFixture, self).setUp()
        targets = [oslo_messaging.Target(topic=t) for t in self.topics]
        # add a special topic for internal notifications
        targets.append(oslo_messaging.Target(topic=self.name))
        self.server = oslo_messaging.get_notification_listener(
            self.transport,
            targets,
            [self])
        self._ctrl = self.notifier('internal', topic=self.name)
        self._start()

    def cleanUp(self):
        self._stop()
        super(NotificationFixture, self).cleanUp()

    def _start(self):
        self.thread = threading.Thread(target=self.server.start)
        self.thread.daemon = True
        self.thread.start()

    def _stop(self):
        self.server.stop()
        self._ctrl.sample({}, 'shutdown', 'shutdown')
        self.server.wait()
        self.thread.join()

    def notifier(self, publisher, topic=None):
        return notifier.Notifier(self.transport,
                                 publisher,
                                 driver='messaging',
                                 topic=topic or self.topics[0])

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
        except moves.queue.Empty:
            pass
        return results
