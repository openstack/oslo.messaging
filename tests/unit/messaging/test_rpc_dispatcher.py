
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

from openstack.common import messaging
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


class _FakeEndpoint(object):

    def __init__(self, target=None):
        self.target = target

    def foo(self, ctxt, **kwargs):
        pass

    def bar(self, ctxt, **kwargs):
        pass


class TestDispatcher(test_utils.BaseTestCase):

    scenarios = [
        ('no_endpoints',
         dict(endpoints=[],
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo'),
              success=False, ex=messaging.UnsupportedVersion)),
        ('default_target',
         dict(endpoints=[{}],
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo'),
              success=True, ex=None)),
        ('default_target_ctxt_and_args',
         dict(endpoints=[{}],
              dispatch_to=dict(endpoint=0, method='bar'),
              ctxt=dict(user='bob'), msg=dict(method='bar',
                                              args=dict(blaa=True)),
              success=True, ex=None)),
        ('default_target_namespace',
         dict(endpoints=[{}],
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace=None),
              success=True, ex=None)),
        ('default_target_version',
         dict(endpoints=[{}],
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', version='1.0'),
              success=True, ex=None)),
        ('default_target_no_such_method',
         dict(endpoints=[{}],
              dispatch_to=None,
              ctxt={}, msg=dict(method='foobar'),
              success=False, ex=messaging.NoSuchMethod)),
        ('namespace',
         dict(endpoints=[{}, dict(namespace='testns')],
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              success=True, ex=None)),
        ('namespace_mismatch',
         dict(endpoints=[{}, dict(namespace='testns')],
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', namespace='nstest'),
              success=False, ex=messaging.UnsupportedVersion)),
        ('version',
         dict(endpoints=[dict(version='1.5'), dict(version='3.4')],
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', version='3.2'),
              success=True, ex=None)),
        ('version_mismatch',
         dict(endpoints=[dict(version='1.5'), dict(version='3.0')],
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', version='3.2'),
              success=False, ex=messaging.UnsupportedVersion)),
    ]

    def _test_dispatcher(self):
        endpoints = []
        for e in self.endpoints:
            target = messaging.Target(**e) if e else None
            endpoints.append(_FakeEndpoint(target))

        serializer = None
        dispatcher = messaging.RPCDispatcher(endpoints, serializer)

        if self.dispatch_to is not None:
            endpoint = endpoints[self.dispatch_to['endpoint']]
            method = self.dispatch_to['method']

            self.mox.StubOutWithMock(endpoint, method)

            method = getattr(endpoint, method)
            method(self.ctxt, **self.msg.get('args', {}))

        self.mox.ReplayAll()

        try:
            dispatcher(self.ctxt, self.msg)
        except Exception as ex:
            self.assertFalse(self.success, ex)
            self.assertTrue(self.ex is not None, ex)
            self.assertTrue(isinstance(ex, self.ex), ex)
            if isinstance(ex, messaging.NoSuchMethod):
                self.assertEquals(ex.method, self.msg.get('method'))
            elif isinstance(ex, messaging.UnsupportedVersion):
                self.assertEquals(ex.version, self.msg.get('version', '1.0'))
        else:
            self.assertTrue(self.success)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_dispatcher)

    def test_dispatcher_no_endpoints(self):
        self._test_scenario('no_endpoints')

    def test_dispatcher_default_target(self):
        self._test_scenario('default_target')

    def test_dispatcher_default_target_ctxt_and_args(self):
        self._test_scenario('default_target_ctxt_and_args')

    def test_dispatcher_default_target_namespace(self):
        self._test_scenario('default_target')

    def test_dispatcher_default_target_version(self):
        self._test_scenario('default_target')

    def test_dispatcher_default_target_no_such_method(self):
        self._test_scenario('default_target_no_such_method')

    def test_dispatcher_namespace(self):
        self._test_scenario('namespace')

    def test_dispatcher_namespace_mismatch(self):
        self._test_scenario('namespace_mismatch')

    def test_dispatcher_version(self):
        self._test_scenario('version')

    def test_dispatcher_version_mismatch(self):
        self._test_scenario('version_mismatch')


class TestSerializer(test_utils.BaseTestCase):

    scenarios = [
        ('no_args_or_retval',
         dict(ctxt={}, args={}, retval=None)),
        ('args_and_retval',
         dict(ctxt=dict(user='bob'),
              args=dict(a='a', b='b', c='c'),
              retval='d')),
    ]

    def _test_serializer(self):
        endpoint = _FakeEndpoint()
        serializer = msg_serializer.NoOpSerializer
        dispatcher = messaging.RPCDispatcher([endpoint], serializer)

        self.mox.StubOutWithMock(endpoint, 'foo')
        args = dict([(k, 'd' + v) for k, v in self.args.items()])
        endpoint.foo(self.ctxt, **args).AndReturn(self.retval)

        self.mox.StubOutWithMock(serializer, 'serialize_entity')
        self.mox.StubOutWithMock(serializer, 'deserialize_entity')

        for arg in self.args:
            serializer.deserialize_entity(self.ctxt, arg).AndReturn('d' + arg)

        serializer.serialize_entity(self.ctxt, self.retval).\
            AndReturn('s' + self.retval if self.retval else None)

        self.mox.ReplayAll()

        retval = dispatcher(self.ctxt, dict(method='foo', args=self.args))
        if self.retval is not None:
            self.assertEquals(retval, 's' + self.retval)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_serializer)

    def test_serializer_no_args_or_retval(self):
        self._test_scenario('no_args_or_retval')

    def test_serializer_args_and_retval(self):
        self._test_scenario('args_and_retval')
