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

import testscenarios

import oslo_messaging
from oslo_messaging import rpc
from oslo_messaging import serializer as msg_serializer
from oslo_messaging.tests import utils as test_utils
from six.moves import mock

load_tests = testscenarios.load_tests_apply_scenarios


class _FakeEndpoint(object):
    def __init__(self, target=None):
        self.target = target

    def foo(self, ctxt, **kwargs):
        pass

    @rpc.expose
    def bar(self, ctxt, **kwargs):
        pass

    def _foobar(self, ctxt, **kwargs):
        pass


class TestDispatcher(test_utils.BaseTestCase):
    scenarios = [
        ('no_endpoints',
         dict(endpoints=[],
              access_policy=None,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo'),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('default_target',
         dict(endpoints=[{}],
              access_policy=None,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo'),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_ctxt_and_args',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.LegacyRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='bar'),
              ctxt=dict(user='bob'), msg=dict(method='bar',
                                              args=dict(blaa=True)),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_namespace',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.LegacyRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace=None),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_version',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', version='1.0'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('default_target_no_such_method',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foobar'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('namespace',
         dict(endpoints=[{}, dict(namespace='testns')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('namespace_mismatch',
         dict(endpoints=[{}, dict(namespace='testns')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', namespace='nstest'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('version',
         dict(endpoints=[dict(version='1.5'), dict(version='3.4')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', version='3.2'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('version_mismatch',
         dict(endpoints=[dict(version='1.5'), dict(version='3.0')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', version='3.2'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('message_in_null_namespace_with_multiple_namespaces',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=[None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace=None),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('message_in_wrong_namespace_with_multiple_namespaces',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', namespace='wrong'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('message_with_endpoint_no_private_and_public_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('message_with_endpoint_no_private_and_private_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='_foobar'),
              ctxt={}, msg=dict(method='_foobar', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('message_with_endpoint_explicitly_exposed_without_exposed_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.ExplicitRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('message_with_endpoint_explicitly_exposed_with_exposed_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.ExplicitRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='bar'),
              ctxt={}, msg=dict(method='bar', namespace='testns'),
              exposed_methods=['bar'],
              success=True, ex=None)),
    ]

    def test_dispatcher(self):

        def _set_endpoint_mock_properties(endpoint):
            endpoint.foo = mock.Mock(spec=dir(_FakeEndpoint.foo))
            # mock doesn't pick up the decorated method.
            endpoint.bar = mock.Mock(spec=dir(_FakeEndpoint.bar))
            endpoint.bar.exposed = mock.PropertyMock(return_value=True)
            endpoint._foobar = mock.Mock(spec=dir(_FakeEndpoint._foobar))

            return endpoint

        endpoints = [_set_endpoint_mock_properties(mock.Mock(
            spec=_FakeEndpoint, target=oslo_messaging.Target(**e)))
            for e in self.endpoints]

        serializer = None
        dispatcher = oslo_messaging.RPCDispatcher(endpoints, serializer,
                                                  self.access_policy)

        incoming = mock.Mock(ctxt=self.ctxt, message=self.msg)

        res = None

        try:
            res = dispatcher.dispatch(incoming)
        except Exception as ex:
            self.assertFalse(self.success, ex)
            self.assertIsNotNone(self.ex, ex)
            self.assertIsInstance(ex, self.ex, ex)
            if isinstance(ex, oslo_messaging.NoSuchMethod):
                self.assertEqual(self.msg.get('method'), ex.method)
            elif isinstance(ex, oslo_messaging.UnsupportedVersion):
                self.assertEqual(self.msg.get('version', '1.0'),
                                 ex.version)
                if ex.method:
                    self.assertEqual(self.msg.get('method'), ex.method)
        else:
            self.assertTrue(self.success,
                            "Unexpected success of operation during testing")
            self.assertIsNotNone(res)

        for n, endpoint in enumerate(endpoints):
            for method_name in self.exposed_methods:
                method = getattr(endpoint, method_name)
                if self.dispatch_to and n == self.dispatch_to['endpoint'] and \
                        method_name == self.dispatch_to['method'] and \
                        method_name in self.exposed_methods:
                    method.assert_called_once_with(
                        self.ctxt, **self.msg.get('args', {}))
                else:
                    self.assertEqual(0, method.call_count,
                                     'method: {}'.format(method))


class TestSerializer(test_utils.BaseTestCase):
    scenarios = [
        ('no_args_or_retval',
         dict(ctxt={}, dctxt={}, args={}, retval=None)),
        ('args_and_retval',
         dict(ctxt=dict(user='bob'),
              dctxt=dict(user='alice'),
              args=dict(a='a', b='b', c='c'),
              retval='d')),
    ]

    def test_serializer(self):
        endpoint = _FakeEndpoint()
        serializer = msg_serializer.NoOpSerializer()
        dispatcher = oslo_messaging.RPCDispatcher([endpoint], serializer)

        endpoint.foo = mock.Mock()

        args = dict([(k, 'd' + v) for k, v in self.args.items()])
        endpoint.foo.return_value = self.retval

        serializer.serialize_entity = mock.Mock()
        serializer.deserialize_entity = mock.Mock()
        serializer.deserialize_context = mock.Mock()

        serializer.deserialize_context.return_value = self.dctxt

        expected_side_effect = ['d' + arg for arg in self.args]
        serializer.deserialize_entity.side_effect = expected_side_effect

        serializer.serialize_entity.return_value = None
        if self.retval:
            serializer.serialize_entity.return_value = 's' + self.retval

        incoming = mock.Mock()
        incoming.ctxt = self.ctxt
        incoming.message = dict(method='foo', args=self.args)
        retval = dispatcher.dispatch(incoming)
        if self.retval is not None:
            self.assertEqual('s' + self.retval, retval)

        endpoint.foo.assert_called_once_with(self.dctxt, **args)
        serializer.deserialize_context.assert_called_once_with(self.ctxt)

        expected_calls = [mock.call(self.dctxt, arg) for arg in self.args]
        self.assertEqual(expected_calls,
                         serializer.deserialize_entity.mock_calls)

        serializer.serialize_entity.assert_called_once_with(self.dctxt,
                                                            self.retval)
