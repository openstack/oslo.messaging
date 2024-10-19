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

from oslo_config import cfg
import testscenarios
from unittest import mock

import oslo_messaging
from oslo_messaging import exceptions
from oslo_messaging import serializer as msg_serializer
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


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

    def test_cast_call(self):
        self.config(rpc_response_timeout=None)
        transport_options = oslo_messaging.TransportOptions()
        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(
            transport, oslo_messaging.Target(),
            transport_options=transport_options)

        transport._send = mock.Mock()

        msg = dict(method='foo', args=self.args)
        kwargs = {'retry': None, 'transport_options': transport_options}
        if self.call:
            kwargs['wait_for_reply'] = True
            kwargs['timeout'] = None
            kwargs['call_monitor_timeout'] = None

        method = client.call if self.call else client.cast
        method(self.ctxt, 'foo', **self.args)
        self.assertFalse(transport_options.at_least_once)
        transport._send.assert_called_once_with(oslo_messaging.Target(),
                                                self.ctxt,
                                                msg,
                                                **kwargs)

    def test_cast_call_with_transport_options(self):
        self.config(rpc_response_timeout=None)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')

        transport_options = oslo_messaging.TransportOptions(at_least_once=True)
        client = oslo_messaging.get_rpc_client(
            transport,
            oslo_messaging.Target(),
            transport_options=transport_options)

        transport._send = mock.Mock()

        msg = dict(method='foo', args=self.args)
        kwargs = {'retry': None,
                  'transport_options': transport_options}
        if self.call:
            kwargs['wait_for_reply'] = True
            kwargs['timeout'] = None
            kwargs['call_monitor_timeout'] = None

        method = client.call if self.call else client.cast
        method(self.ctxt, 'foo', **self.args)

        self.assertTrue(transport_options.at_least_once)
        transport._send.assert_called_once_with(oslo_messaging.Target(),
                                                self.ctxt,
                                                msg,
                                                **kwargs)


class TestCastToTarget(test_utils.BaseTestCase):

    _base = [
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
         dict(ctor=dict(version='1.1'),
              prepare={},
              expect=dict(version='1.1'))),
        ('prepare_version',
         dict(ctor={},
              prepare=dict(version='1.1'),
              expect=dict(version='1.1'))),
        ('prepare_version_none',
         dict(ctor=dict(version='1.1'),
              prepare=dict(version=None),
              expect={})),
        ('both_version',
         dict(ctor=dict(version='ctorversion'),
              prepare=dict(version='1.1'),
              expect=dict(version='1.1'))),
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

    _prepare = [
        ('single_prepare', dict(double_prepare=False)),
        ('double_prepare', dict(double_prepare=True)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._base,
                                                         cls._prepare)

    def setUp(self):
        super().setUp(conf=cfg.ConfigOpts())

    def test_cast_to_target(self):
        target = oslo_messaging.Target(**self.ctor)
        expect_target = oslo_messaging.Target(**self.expect)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(transport, target)

        transport._send = mock.Mock()

        msg = dict(method='foo', args={})
        if 'namespace' in self.expect:
            msg['namespace'] = self.expect['namespace']
        if 'version' in self.expect:
            msg['version'] = self.expect['version']

        if self.prepare:
            client = client.prepare(**self.prepare)
            if self.double_prepare:
                client = client.prepare(**self.prepare)
        client.cast({}, 'foo')

        transport._send.assert_called_once_with(expect_target,
                                                {},
                                                msg,
                                                retry=None,
                                                transport_options=None)


TestCastToTarget.generate_scenarios()


_notset = object()


class TestCallTimeout(test_utils.BaseTestCase):

    scenarios = [
        ('all_none',
         dict(confval=None, ctor=None, prepare=_notset, expect=None, cm=None)),
        ('confval',
         dict(confval=21, ctor=None, prepare=_notset, expect=21, cm=None)),
        ('ctor',
         dict(confval=None, ctor=21.1, prepare=_notset, expect=21.1, cm=None)),
        ('ctor_zero',
         dict(confval=None, ctor=0, prepare=_notset, expect=0, cm=None)),
        ('prepare',
         dict(confval=None, ctor=None, prepare=21.1, expect=21.1, cm=None)),
        ('prepare_override',
         dict(confval=None, ctor=10.1, prepare=21.1, expect=21.1, cm=None)),
        ('prepare_zero',
         dict(confval=None, ctor=None, prepare=0, expect=0, cm=None)),
        ('call_monitor',
         dict(confval=None, ctor=None, prepare=60, expect=60, cm=30)),
    ]

    def test_call_timeout(self):
        self.config(rpc_response_timeout=self.confval)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(
            transport, oslo_messaging.Target(), timeout=self.ctor,
            call_monitor_timeout=self.cm)

        transport._send = mock.Mock()

        msg = dict(method='foo', args={})
        kwargs = dict(wait_for_reply=True, timeout=self.expect, retry=None,
                      call_monitor_timeout=self.cm, transport_options=None)

        if self.prepare is not _notset:
            client = client.prepare(timeout=self.prepare)
        client.call({}, 'foo')

        transport._send.assert_called_once_with(oslo_messaging.Target(),
                                                {},
                                                msg,
                                                **kwargs)


class TestCallRetry(test_utils.BaseTestCase):

    scenarios = [
        ('all_none', dict(ctor=None, prepare=_notset, expect=None)),
        ('ctor', dict(ctor=21, prepare=_notset, expect=21)),
        ('ctor_zero', dict(ctor=0, prepare=_notset, expect=0)),
        ('prepare', dict(ctor=None, prepare=21, expect=21)),
        ('prepare_override', dict(ctor=10, prepare=21, expect=21)),
        ('prepare_zero', dict(ctor=None, prepare=0, expect=0)),
    ]

    def test_call_retry(self):
        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(
            transport, oslo_messaging.Target(),
            retry=self.ctor)

        transport._send = mock.Mock()

        msg = dict(method='foo', args={})
        kwargs = dict(wait_for_reply=True, timeout=60,
                      retry=self.expect, call_monitor_timeout=None,
                      transport_options=None)

        if self.prepare is not _notset:
            client = client.prepare(retry=self.prepare)
        client.call({}, 'foo')

        transport._send.assert_called_once_with(oslo_messaging.Target(),
                                                {},
                                                msg,
                                                **kwargs)


class TestCallFanout(test_utils.BaseTestCase):

    scenarios = [
        ('target', dict(prepare=_notset, target={'fanout': True})),
        ('prepare', dict(prepare={'fanout': True}, target={})),
        ('both', dict(prepare={'fanout': True}, target={'fanout': True})),
    ]

    def test_call_fanout(self):
        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(
            transport, oslo_messaging.Target(**self.target))

        if self.prepare is not _notset:
            client = client.prepare(**self.prepare)

        self.assertRaises(exceptions.InvalidTarget,
                          client.call, {}, 'foo')


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

    def test_call_serializer(self):
        self.config(rpc_response_timeout=None)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        serializer = msg_serializer.NoOpSerializer()

        client = oslo_messaging.get_rpc_client(
            transport, oslo_messaging.Target(), serializer=serializer)

        transport._send = mock.Mock()
        kwargs = dict(wait_for_reply=True,
                      timeout=None) if self.call else {}
        kwargs['retry'] = None
        if self.call:
            kwargs['call_monitor_timeout'] = None

        transport._send.return_value = self.retval

        serializer.serialize_entity = mock.Mock()
        serializer.deserialize_entity = mock.Mock()
        serializer.serialize_context = mock.Mock()

        def _stub(ctxt, arg):
            return 's' + arg

        msg = dict(method='foo', args=dict())
        for k, v in self.args.items():
            msg['args'][k] = 's' + v
        serializer.serialize_entity.side_effect = _stub

        if self.call:
            serializer.deserialize_entity.return_value = 'd' + self.retval

        serializer.serialize_context.return_value = dict(user='alice')

        method = client.call if self.call else client.cast
        retval = method(self.ctxt, 'foo', **self.args)
        if self.retval is not None:
            self.assertEqual('d' + self.retval, retval)

        transport._send.assert_called_once_with(oslo_messaging.Target(),
                                                dict(user='alice'),
                                                msg,
                                                transport_options=None,
                                                **kwargs)
        expected_calls = [mock.call(self.ctxt, arg) for arg in self.args]
        self.assertEqual(expected_calls,
                         serializer.serialize_entity.mock_calls)
        if self.call:
            serializer.deserialize_entity.assert_called_once_with(self.ctxt,
                                                                  self.retval)
        serializer.serialize_context.assert_called_once_with(self.ctxt)


class TestVersionCap(test_utils.BaseTestCase):

    _call_vs_cast = [
        ('call', dict(call=True)),
        ('cast', dict(call=False)),
    ]

    _cap_scenarios = [
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
        ('ctor_cap_none_version_ok',
         dict(cap=None, prepare_cap=_notset,
              version='1.0', prepare_version=_notset,
              success=True)),
        ('ctor_cap_version_none_fail',
         dict(cap='1.0', prepare_cap=_notset,
              version=None, prepare_version=_notset,
              success=False)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = (
            testscenarios.multiply_scenarios(cls._call_vs_cast,
                                             cls._cap_scenarios))

    def test_version_cap(self):
        self.config(rpc_response_timeout=None)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')

        target = oslo_messaging.Target(version=self.version)
        client = oslo_messaging.get_rpc_client(transport, target,
                                               version_cap=self.cap)

        if self.success:
            transport._send = mock.Mock()

            if self.prepare_version is not _notset:
                target = target(version=self.prepare_version)

            msg = dict(method='foo', args={})
            if target.version is not None:
                msg['version'] = target.version

            kwargs = {'retry': None}
            if self.call:
                kwargs['wait_for_reply'] = True
                kwargs['timeout'] = None
                kwargs['call_monitor_timeout'] = None

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
            self.assertIsInstance(ex, oslo_messaging.RPCVersionCapError, ex)
            self.assertFalse(self.success)
        else:
            self.assertTrue(self.success)
            transport._send.assert_called_once_with(target, {}, msg,
                                                    transport_options=None,
                                                    **kwargs)


TestVersionCap.generate_scenarios()


class TestCanSendVersion(test_utils.BaseTestCase):

    scenarios = [
        ('all_none',
         dict(cap=None, prepare_cap=_notset,
              version=None, prepare_version=_notset,
              can_send_version=_notset,
              can_send=True)),
        ('ctor_cap_ok',
         dict(cap='1.1', prepare_cap=_notset,
              version='1.0', prepare_version=_notset,
              can_send_version=_notset,
              can_send=True)),
        ('ctor_cap_override_ok',
         dict(cap='2.0', prepare_cap='1.1',
              version='1.0', prepare_version='1.0',
              can_send_version=_notset,
              can_send=True)),
        ('ctor_cap_override_none_ok',
         dict(cap='1.1', prepare_cap=None,
              version='1.0', prepare_version=_notset,
              can_send_version=_notset,
              can_send=True)),
        ('ctor_cap_can_send_ok',
         dict(cap='1.1', prepare_cap=None,
              version='1.0', prepare_version=_notset,
              can_send_version='1.1',
              can_send=True)),
        ('ctor_cap_can_send_none_ok',
         dict(cap='1.1', prepare_cap=None,
              version='1.0', prepare_version=_notset,
              can_send_version=None,
              can_send=True)),
        ('ctor_cap_minor_fail',
         dict(cap='1.0', prepare_cap=_notset,
              version='1.1', prepare_version=_notset,
              can_send_version=_notset,
              can_send=False)),
        ('ctor_cap_major_fail',
         dict(cap='2.0', prepare_cap=_notset,
              version=None, prepare_version='1.0',
              can_send_version=_notset,
              can_send=False)),
        ('ctor_cap_none_version_ok',
         dict(cap=None, prepare_cap=_notset,
              version='1.0', prepare_version=_notset,
              can_send_version=_notset,
              can_send=True)),
        ('ctor_cap_version_none_fail',
         dict(cap='1.0', prepare_cap=_notset,
              version=None, prepare_version=_notset,
              can_send_version=_notset,
              can_send=False)),
        ('ctor_cap_version_can_send_none_fail',
         dict(cap='1.0', prepare_cap=_notset,
              version='1.0', prepare_version=_notset,
              can_send_version=None,
              can_send=False)),
    ]

    def test_version_cap(self):
        self.config(rpc_response_timeout=None)

        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')

        target = oslo_messaging.Target(version=self.version)
        client = oslo_messaging.get_rpc_client(transport, target,
                                               version_cap=self.cap)

        prep_kwargs = {}
        if self.prepare_cap is not _notset:
            prep_kwargs['version_cap'] = self.prepare_cap
        if self.prepare_version is not _notset:
            prep_kwargs['version'] = self.prepare_version
        if prep_kwargs:
            client = client.prepare(**prep_kwargs)

        if self.can_send_version is not _notset:
            can_send = client.can_send_version(version=self.can_send_version)
            call_context_can_send = client.prepare().can_send_version(
                version=self.can_send_version)
            self.assertEqual(can_send, call_context_can_send)
        else:
            can_send = client.can_send_version()

        self.assertEqual(self.can_send, can_send)

    def test_invalid_version_type(self):
        target = oslo_messaging.Target(topic='sometopic')
        transport = oslo_messaging.get_rpc_transport(self.conf, url='fake:')
        client = oslo_messaging.get_rpc_client(transport, target)
        self.assertRaises(exceptions.MessagingException,
                          client.prepare, version='5')
        self.assertRaises(exceptions.MessagingException,
                          client.prepare, version='5.a')
        self.assertRaises(exceptions.MessagingException,
                          client.prepare, version='5.5.a')


class TestTransportWarning(test_utils.BaseTestCase):

    @mock.patch('oslo_messaging.rpc.client.LOG')
    def test_warning_when_notifier_transport(self, log):
        transport = oslo_messaging.get_notification_transport(self.conf)
        oslo_messaging.get_rpc_client(transport, oslo_messaging.Target())
        log.warning.assert_called_once_with(
            "Using notification transport for RPC. Please use "
            "get_rpc_transport to obtain an RPC transport "
            "instance.")
