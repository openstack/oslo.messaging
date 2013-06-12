
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

from oslo.config import cfg

from openstack.common import messaging
from openstack.common.messaging.rpc import client as rpc_client
from openstack.common.messaging import serializer as msg_serializer
from tests import utils as test_utils

# FIXME(markmc): I'm having touble using testscenarios with nose
# import testscenarios
# load_tests = testscenarios.load_tests_apply_scenarios


def _test_scenario(name, scenarios, obj, method):
    for n, args in scenarios:
        if n == name:
            for k, v in args.items():
                setattr(obj, k, v)
            break
    else:
        raise RuntimeError('Invalid scenario', name)
    method()


class _FakeTransport(object):

    def __init__(self, conf):
        self.conf = conf

    def _send(self, *args, **kwargs):
        pass


class TestCastCall(test_utils.BaseTestCase):

    scenarios = [
        ('cast_no_ctxt_no_args', dict(call=False, ctxt={}, args={})),
        ('call_no_ctxt_no_args', dict(call=True, ctxt={}, args={})),
        ('cast_ctxt_and_args',
         dict(call=False,
              ctxt=dict(user='testuser', project='testtenant'),
              args=dict(bar='blaa', foobar=11.01))),
        ('call_ctxt_and_args',
         dict(call=True,
              ctxt=dict(user='testuser', project='testtenant'),
              args=dict(bar='blaa', foobar=11.01))),
    ]

    def setUp(self):
        super(TestCastCall, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(rpc_client._client_opts)

    def _test_cast_call(self):
        self.config(rpc_response_timeout=None)

        transport = _FakeTransport(self.conf)
        client = messaging.RPCClient(transport, messaging.Target())

        self.mox.StubOutWithMock(transport, '_send')

        msg = dict(method='foo', args=self.args)
        kwargs = {}
        if self.call:
            kwargs['wait_for_reply'] = True
            kwargs['timeout'] = None

        transport._send(messaging.Target(), self.ctxt, msg, **kwargs)
        self.mox.ReplayAll()

        method = client.call if self.call else client.cast
        method(self.ctxt, 'foo', **self.args)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_cast_call)

    def test_cast_no_ctxt_no_args(self):
        self._test_scenario('cast_no_ctxt_no_args')

    def test_call_no_ctxt_no_args(self):
        self._test_scenario('call_no_ctxt_no_args')

    def test_cast_ctxt_and_args(self):
        self._test_scenario('cast_ctxt_and_args')

    def test_call_ctxt_and_args(self):
        self._test_scenario('call_ctxt_and_args')


class TestCastToTarget(test_utils.BaseTestCase):

    scenarios = [
        ('all_none', dict(ctor={}, prepare={}, expect={})),
        ('ctor_exchange',
         dict(ctor=dict(exchange='testexchange'),
              prepare={},
              expect=dict(exchange='testexchange'))),
        ('prepare_exchange',
         dict(ctor={},
              prepare=dict(exchange='testexchange'),
              expect=dict(exchange='testexchange'))),
        ('prepare_exchange_none',
         dict(ctor=dict(exchange='testexchange'),
              prepare=dict(exchange=None),
              expect={})),
        ('both_exchange',
         dict(ctor=dict(exchange='ctorexchange'),
              prepare=dict(exchange='testexchange'),
              expect=dict(exchange='testexchange'))),
        ('ctor_topic',
         dict(ctor=dict(topic='testtopic'),
              prepare={},
              expect=dict(topic='testtopic'))),
        ('prepare_topic',
         dict(ctor={},
              prepare=dict(topic='testtopic'),
              expect=dict(topic='testtopic'))),
        ('prepare_topic_none',
         dict(ctor=dict(topic='testtopic'),
              prepare=dict(topic=None),
              expect={})),
        ('both_topic',
         dict(ctor=dict(topic='ctortopic'),
              prepare=dict(topic='testtopic'),
              expect=dict(topic='testtopic'))),
        ('ctor_namespace',
         dict(ctor=dict(namespace='testnamespace'),
              prepare={},
              expect=dict(namespace='testnamespace'))),
        ('prepare_namespace',
         dict(ctor={},
              prepare=dict(namespace='testnamespace'),
              expect=dict(namespace='testnamespace'))),
        ('prepare_namespace_none',
         dict(ctor=dict(namespace='testnamespace'),
              prepare=dict(namespace=None),
              expect={})),
        ('both_namespace',
         dict(ctor=dict(namespace='ctornamespace'),
              prepare=dict(namespace='testnamespace'),
              expect=dict(namespace='testnamespace'))),
        ('ctor_version',
         dict(ctor=dict(version='testversion'),
              prepare={},
              expect=dict(version='testversion'))),
        ('prepare_version',
         dict(ctor={},
              prepare=dict(version='testversion'),
              expect=dict(version='testversion'))),
        ('prepare_version_none',
         dict(ctor=dict(version='testversion'),
              prepare=dict(version=None),
              expect={})),
        ('both_version',
         dict(ctor=dict(version='ctorversion'),
              prepare=dict(version='testversion'),
              expect=dict(version='testversion'))),
        ('ctor_server',
         dict(ctor=dict(server='testserver'),
              prepare={},
              expect=dict(server='testserver'))),
        ('prepare_server',
         dict(ctor={},
              prepare=dict(server='testserver'),
              expect=dict(server='testserver'))),
        ('prepare_server_none',
         dict(ctor=dict(server='testserver'),
              prepare=dict(server=None),
              expect={})),
        ('both_server',
         dict(ctor=dict(server='ctorserver'),
              prepare=dict(server='testserver'),
              expect=dict(server='testserver'))),
        ('ctor_fanout',
         dict(ctor=dict(fanout=True),
              prepare={},
              expect=dict(fanout=True))),
        ('prepare_fanout',
         dict(ctor={},
              prepare=dict(fanout=True),
              expect=dict(fanout=True))),
        ('prepare_fanout_none',
         dict(ctor=dict(fanout=True),
              prepare=dict(fanout=None),
              expect={})),
        ('both_fanout',
         dict(ctor=dict(fanout=True),
              prepare=dict(fanout=False),
              expect=dict(fanout=False))),
    ]

    def setUp(self):
        super(TestCastToTarget, self).setUp(conf=cfg.ConfigOpts())

    def _test_cast_to_target(self):
        target = messaging.Target(**self.ctor)
        expect_target = messaging.Target(**self.expect)

        transport = _FakeTransport(self.conf)
        client = messaging.RPCClient(transport, target)

        self.mox.StubOutWithMock(transport, '_send')

        msg = dict(method='foo', args={})
        if 'namespace' in self.expect:
            msg['namespace'] = self.expect['namespace']
        if 'version' in self.expect:
            msg['version'] = self.expect['version']
        transport._send(expect_target, {}, msg)

        self.mox.ReplayAll()

        if self.prepare:
            client = client.prepare(**self.prepare)
        client.cast({}, 'foo')

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_cast_to_target)

    def test_cast_to_target_all_none(self):
        self._test_scenario('all_none')

    def test_cast_to_target_ctor_exchange(self):
        self._test_scenario('ctor_exchange')

    def test_cast_to_target_prepare_exchange(self):
        self._test_scenario('prepare_exchange')

    def test_cast_to_target_prepare_exchange_none(self):
        self._test_scenario('prepare_exchange_none')

    def test_cast_to_target_both_exchange(self):
        self._test_scenario('both_exchange')

    def test_cast_to_target_ctor_topic(self):
        self._test_scenario('ctor_topic')

    def test_cast_to_target_prepare_topic(self):
        self._test_scenario('prepare_topic')

    def test_cast_to_target_prepare_topic_none(self):
        self._test_scenario('prepare_topic_none')

    def test_cast_to_target_both_topic(self):
        self._test_scenario('both_topic')

    def test_cast_to_target_ctor_namespace(self):
        self._test_scenario('ctor_namespace')

    def test_cast_to_target_prepare_namespace(self):
        self._test_scenario('prepare_namespace')

    def test_cast_to_target_prepare_namespace_none(self):
        self._test_scenario('prepare_namespace_none')

    def test_cast_to_target_both_namespace(self):
        self._test_scenario('both_namespace')

    def test_cast_to_target_ctor_version(self):
        self._test_scenario('ctor_version')

    def test_cast_to_target_prepare_version(self):
        self._test_scenario('prepare_version')

    def test_cast_to_target_prepare_version_none(self):
        self._test_scenario('prepare_version_none')

    def test_cast_to_target_both_version(self):
        self._test_scenario('both_version')

    def test_cast_to_target_ctor_server(self):
        self._test_scenario('ctor_server')

    def test_cast_to_target_prepare_server(self):
        self._test_scenario('prepare_server')

    def test_cast_to_target_prepare_server_none(self):
        self._test_scenario('prepare_server_none')

    def test_cast_to_target_both_server(self):
        self._test_scenario('both_server')

    def test_cast_to_target_ctor_fanout(self):
        self._test_scenario('ctor_fanout')

    def test_cast_to_target_prepare_fanout(self):
        self._test_scenario('prepare_fanout')

    def test_cast_to_target_prepare_fanout_none(self):
        self._test_scenario('prepare_fanout_none')

    def test_cast_to_target_both_fanout(self):
        self._test_scenario('both_fanout')


_notset = object()


class TestCallTimeout(test_utils.BaseTestCase):

    scenarios = [
        ('all_none',
         dict(confval=None, ctor=None, prepare=_notset, expect=None)),
        ('confval',
         dict(confval=21.1, ctor=None, prepare=_notset, expect=21.1)),
        ('ctor',
         dict(confval=None, ctor=21.1, prepare=_notset, expect=21.1)),
        ('ctor_zero',
         dict(confval=None, ctor=0, prepare=_notset, expect=0)),
        ('prepare',
         dict(confval=None, ctor=None, prepare=21.1, expect=21.1)),
        ('prepare_override',
         dict(confval=None, ctor=10.1, prepare=21.1, expect=21.1)),
        ('prepare_zero',
         dict(confval=None, ctor=None, prepare=0, expect=0)),
    ]

    def setUp(self):
        super(TestCallTimeout, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(rpc_client._client_opts)

    def _test_call_timeout(self):
        self.config(rpc_response_timeout=self.confval)

        transport = _FakeTransport(self.conf)
        client = messaging.RPCClient(transport, messaging.Target(),
                                     timeout=self.ctor)

        self.mox.StubOutWithMock(transport, '_send')

        msg = dict(method='foo', args={})
        kwargs = dict(wait_for_reply=True, timeout=self.expect)
        transport._send(messaging.Target(), {}, msg, **kwargs)

        self.mox.ReplayAll()

        if self.prepare is not _notset:
            client = client.prepare(timeout=self.prepare)
        client.call({}, 'foo')

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_call_timeout)

    def test_call_timeout_all_none(self):
        self._test_scenario('all_none')

    def test_call_timeout_confval(self):
        self._test_scenario('confval')

    def test_call_timeout_ctor(self):
        self._test_scenario('ctor')

    def test_call_timeout_ctor_zero(self):
        self._test_scenario('ctor_zero')

    def test_call_timeout_prepare(self):
        self._test_scenario('prepare')

    def test_call_timeout_prepare_override(self):
        self._test_scenario('prepare_override')

    def test_call_timeout_prepare_zero(self):
        self._test_scenario('prepare_zero')


class TestSerializer(test_utils.BaseTestCase):

    scenarios = [
        ('cast',
         dict(call=False,
              ctxt=dict(user='bob'),
              args=dict(a='a', b='b', c='c'),
              retval=None)),
        ('call',
         dict(call=True,
              ctxt=dict(user='bob'),
              args=dict(a='a', b='b', c='c'),
              retval='d')),
    ]

    def setUp(self):
        super(TestSerializer, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(rpc_client._client_opts)

    def _test_call_serializer(self):
        self.config(rpc_response_timeout=None)

        transport = _FakeTransport(self.conf)
        serializer = msg_serializer.NoOpSerializer()

        client = messaging.RPCClient(transport, messaging.Target(),
                                     serializer=serializer)

        self.mox.StubOutWithMock(transport, '_send')

        msg = dict(method='foo',
                   args=dict([(k, 's' + v) for k, v in self.args.items()]))
        kwargs = dict(wait_for_reply=True, timeout=None) if self.call else {}
        transport._send(messaging.Target(), self.ctxt, msg, **kwargs).\
            AndReturn(self.retval)

        self.mox.StubOutWithMock(serializer, 'serialize_entity')
        self.mox.StubOutWithMock(serializer, 'deserialize_entity')

        for arg in self.args:
            serializer.serialize_entity(self.ctxt, arg).AndReturn('s' + arg)

        if self.call:
            serializer.deserialize_entity(self.ctxt, self.retval).\
                AndReturn('d' + self.retval)

        self.mox.ReplayAll()

        method = client.call if self.call else client.cast
        retval = method(self.ctxt, 'foo', **self.args)
        if self.retval is not None:
            self.assertEquals(retval, 'd' + self.retval)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_call_serializer)

    def test_serializer_cast(self):
        self._test_scenario('cast')

    def test_serializer_call(self):
        self._test_scenario('call')


class TestVersionCap(test_utils.BaseTestCase):

    scenarios = []

    _scenarios_template = [
        ('all_none',
         dict(cap=None, prepare_cap=_notset,
              version=None, prepare_version=_notset,
              success=True)),
        ('ctor_cap_ok',
         dict(cap='1.1', prepare_cap=_notset,
              version='1.0', prepare_version=_notset,
              success=True)),
        ('ctor_cap_override_ok',
         dict(cap='2.0', prepare_cap='1.1',
              version='1.0', prepare_version='1.0',
              success=True)),
        ('ctor_cap_override_none_ok',
         dict(cap='1.1', prepare_cap=None,
              version='1.0', prepare_version=_notset,
              success=True)),
        ('ctor_cap_minor_fail',
         dict(cap='1.0', prepare_cap=_notset,
              version='1.1', prepare_version=_notset,
              success=False)),
        ('ctor_cap_major_fail',
         dict(cap='2.0', prepare_cap=_notset,
              version=None, prepare_version='1.0',
              success=False)),
    ]

    @classmethod
    def setup_scenarios(cls):
        for s in cls._scenarios_template:
            s = ('call_' + s[0], s[1].copy())
            s[1]['call'] = True
            cls.scenarios.append(s)
        for s in cls._scenarios_template:
            s = ('cast_' + s[0], s[1].copy())
            s[1]['call'] = False
            cls.scenarios.append(s)

    def setUp(self):
        super(TestVersionCap, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(rpc_client._client_opts)

    def _test_version_cap(self):
        self.config(rpc_response_timeout=None)

        transport = _FakeTransport(self.conf)

        target = messaging.Target(version=self.version)
        client = messaging.RPCClient(transport, target,
                                     version_cap=self.cap)

        if self.success:
            self.mox.StubOutWithMock(transport, '_send')

            if self.prepare_version is not _notset:
                target = target(version=self.prepare_version)

            msg = dict(method='foo', args={})
            if target.version is not None:
                msg['version'] = target.version

            kwargs = {}
            if self.call:
                kwargs['wait_for_reply'] = True
                kwargs['timeout'] = None

            transport._send(target, {}, msg, **kwargs)

            self.mox.ReplayAll()

        prep_kwargs = {}
        if self.prepare_cap is not _notset:
            prep_kwargs['version_cap'] = self.prepare_cap
        if self.prepare_version is not _notset:
            prep_kwargs['version'] = self.prepare_version
        if prep_kwargs:
            client = client.prepare(**prep_kwargs)

        method = client.call if self.call else client.cast
        try:
            method({}, 'foo')
        except Exception as ex:
            self.assertTrue(isinstance(ex, messaging.RPCVersionCapError), ex)
            self.assertTrue(not self.success)
        else:
            self.assertTrue(self.success)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_version_cap)

    def test_version_cap_call_all_none(self):
        self._test_scenario('call_all_none')

    def test_version_call_ctor_cap_ok(self):
        self._test_scenario('call_ctor_cap_ok')

    def test_version_call_ctor_cap_override_ok(self):
        self._test_scenario('call_ctor_cap_override_ok')

    def test_version_call_ctor_cap_override_none(self):
        self._test_scenario('call_ctor_cap_override_none_ok')

    def test_version_call_ctor_cap_minor_fail(self):
        self._test_scenario('call_ctor_cap_minor_fail')

    def test_version_call_ctor_cap_major_fail(self):
        self._test_scenario('call_ctor_cap_major_fail')

    def test_version_cap_cast_all_none(self):
        self._test_scenario('cast_all_none')

    def test_version_cast_ctor_cap_ok(self):
        self._test_scenario('cast_ctor_cap_ok')

    def test_version_cast_ctor_cap_override_ok(self):
        self._test_scenario('cast_ctor_cap_override_ok')

    def test_version_cast_ctor_cap_override_none(self):
        self._test_scenario('cast_ctor_cap_override_none_ok')

    def test_version_cast_ctor_cap_minor_fail(self):
        self._test_scenario('cast_ctor_cap_minor_fail')

    def test_version_cast_ctor_cap_major_fail(self):
        self._test_scenario('cast_ctor_cap_major_fail')


TestVersionCap.setup_scenarios()


class TestCheckForLock(test_utils.BaseTestCase):

    scenarios = [
        ('none',
         dict(locks_held=None, warning=None)),
        ('one',
         dict(locks_held=['foo'], warning="held are ['foo']")),
        ('two',
         dict(locks_held=['foo', 'bar'], warning="held are ['foo', 'bar']")),
    ]

    def setUp(self):
        super(TestCheckForLock, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(rpc_client._client_opts)

    def _test_check_for_lock(self):
        self.config(rpc_response_timeout=None)

        transport = _FakeTransport(self.conf)

        def check_for_lock(conf):
            self.assertTrue(conf is self.conf)
            return self.locks_held

        client = messaging.RPCClient(transport, messaging.Target(),
                                     check_for_lock=check_for_lock)

        self.mox.StubOutWithMock(transport, '_send')
        transport._send(messaging.Target(), {},
                        dict(method='foo', args={}),
                        wait_for_reply=True, timeout=None)
        self.mox.ReplayAll()

        warnings = []

        def stub_warn(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            warnings.append(msg % a)

        self.stubs.Set(rpc_client._LOG, 'warn', stub_warn)

        client.call({}, 'foo')

        if self.warning:
            self.assertEquals(len(warnings), 1)
            self.assertTrue(self.warning in warnings[0])
        else:
            self.assertEquals(len(warnings), 0)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_check_for_lock)

    def test_check_for_lock_none(self):
        self._test_scenario('none')

    def test_check_for_lock_one(self):
        self._test_scenario('one')

    def test_check_for_lock_two(self):
        self._test_scenario('two')
