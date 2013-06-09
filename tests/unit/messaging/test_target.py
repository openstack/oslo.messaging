
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


class TargetConstructorTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('all_none', dict(kwargs=dict())),
        ('exchange', dict(kwargs=dict(exchange='testexchange'))),
        ('topic', dict(kwargs=dict(topic='testtopic'))),
        ('namespace', dict(kwargs=dict(namespace='testnamespace'))),
        ('version', dict(kwargs=dict(version='3.4'))),
        ('server', dict(kwargs=dict(server='testserver'))),
        ('fanout', dict(kwargs=dict(fanout=True))),
    ]

    def _test_constructor(self):
        target = messaging.Target(**self.kwargs)
        for k in self.kwargs:
            self.assertEquals(getattr(target, k), self.kwargs[k])
        for k in ['exchange', 'topic', 'namespace',
                  'version', 'server', 'fanout']:
            if k in self.kwargs:
                continue
            self.assertTrue(getattr(target, k) is None)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_constructor)

    def test_constructor_all_none(self):
        self._test_scenario('all_none')

    def test_constructor_exchange(self):
        self._test_scenario('exchange')

    def test_constructor_topic(self):
        self._test_scenario('topic')

    def test_constructor_namespace(self):
        self._test_scenario('namespace')

    def test_constructor_version(self):
        self._test_scenario('version')

    def test_constructor_server(self):
        self._test_scenario('server')

    def test_constructor_fanout(self):
        self._test_scenario('fanout')


class TargetCallableTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('all_none', dict(attrs=dict(), kwargs=dict(), vals=dict())),
        ('exchange_attr', dict(attrs=dict(exchange='testexchange'),
                               kwargs=dict(),
                               vals=dict(exchange='testexchange'))),
        ('exchange_arg', dict(attrs=dict(),
                              kwargs=dict(exchange='testexchange'),
                              vals=dict(exchange='testexchange'))),
        ('topic_attr', dict(attrs=dict(topic='testtopic'),
                            kwargs=dict(),
                            vals=dict(topic='testtopic'))),
        ('topic_arg', dict(attrs=dict(),
                           kwargs=dict(topic='testtopic'),
                           vals=dict(topic='testtopic'))),
        ('namespace_attr', dict(attrs=dict(namespace='testnamespace'),
                                kwargs=dict(),
                                vals=dict(namespace='testnamespace'))),
        ('namespace_arg', dict(attrs=dict(),
                               kwargs=dict(namespace='testnamespace'),
                               vals=dict(namespace='testnamespace'))),
        ('version_attr', dict(attrs=dict(version='3.4'),
                              kwargs=dict(),
                              vals=dict(version='3.4'))),
        ('version_arg', dict(attrs=dict(),
                             kwargs=dict(version='3.4'),
                             vals=dict(version='3.4'))),
        ('server_attr', dict(attrs=dict(server='testserver'),
                             kwargs=dict(),
                             vals=dict(server='testserver'))),
        ('server_arg', dict(attrs=dict(),
                            kwargs=dict(server='testserver'),
                            vals=dict(server='testserver'))),
        ('fanout_attr', dict(attrs=dict(fanout=True),
                             kwargs=dict(),
                             vals=dict(fanout=True))),
        ('fanout_arg', dict(attrs=dict(),
                            kwargs=dict(fanout=True),
                            vals=dict(fanout=True))),
    ]

    def _test_callable(self):
        target = messaging.Target(**self.attrs)
        target = target(**self.kwargs)
        for k in self.vals:
            self.assertEquals(getattr(target, k), self.vals[k])
        for k in ['exchange', 'topic', 'namespace',
                  'version', 'server', 'fanout']:
            if k in self.vals:
                continue
            self.assertTrue(getattr(target, k) is None)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_callable)

    def test_callable_all_none(self):
        self._test_scenario('all_none')

    def test_callable_exchange_attr(self):
        self._test_scenario('exchange_attr')

    def test_callable_exchange_arg(self):
        self._test_scenario('exchange_arg')

    def test_callable_topic_attr(self):
        self._test_scenario('topic_attr')

    def test_callable_topic_arg(self):
        self._test_scenario('topic_arg')

    def test_callable_namespace_attr(self):
        self._test_scenario('namespace_attr')

    def test_callable_namespace_arg(self):
        self._test_scenario('namespace_arg')

    def test_callable_version_attr(self):
        self._test_scenario('version_attr')

    def test_callable_version_arg(self):
        self._test_scenario('version_arg')

    def test_callable_server_attr(self):
        self._test_scenario('server_attr')

    def test_callable_server_arg(self):
        self._test_scenario('server_arg')

    def test_callable_fanout_attr(self):
        self._test_scenario('fanout_attr')

    def test_callable_fanout_arg(self):
        self._test_scenario('fanout_arg')


class TargetReprTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('all_none', dict(kwargs=dict(), repr='')),
        ('exchange', dict(kwargs=dict(exchange='testexchange'),
                          repr='exchange=testexchange')),
        ('topic', dict(kwargs=dict(topic='testtopic'),
                       repr='topic=testtopic')),
        ('namespace', dict(kwargs=dict(namespace='testnamespace'),
                           repr='namespace=testnamespace')),
        ('version', dict(kwargs=dict(version='3.4'),
                         repr='version=3.4')),
        ('server', dict(kwargs=dict(server='testserver'),
                        repr='server=testserver')),
        ('fanout', dict(kwargs=dict(fanout=True),
                        repr='fanout=True')),
        ('exchange_and_fanout', dict(kwargs=dict(exchange='testexchange',
                                                 fanout=True),
                                     repr='exchange=testexchange, '
                                          'fanout=True')),
    ]

    def _test_repr(self):
        target = messaging.Target(**self.kwargs)
        self.assertEquals(str(target), '<Target ' + self.repr + '>')

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_repr)

    def test_repr_all_none(self):
        self._test_scenario('all_none')

    def test_repr_exchange(self):
        self._test_scenario('exchange')

    def test_repr_topic(self):
        self._test_scenario('topic')

    def test_repr_namespace(self):
        self._test_scenario('namespace')

    def test_repr_version(self):
        self._test_scenario('version')

    def test_repr_server(self):
        self._test_scenario('server')

    def test_repr_fanout(self):
        self._test_scenario('fanout')

    def test_repr_exchange_and_fanout(self):
        self._test_scenario('exchange_and_fanout')


class EqualityTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('all_none',
         dict(a_kwargs=dict(), b_kwargs=dict(), equals=True)),
    ]

    @classmethod
    def _add_scenarios(cls, param):
        new_scenarios = [
            ('a_' + param,
             dict(a_kwargs={param: 'foo'},
                  b_kwargs={},
                  equals=False)),
            ('b_' + param,
             dict(a_kwargs={},
                  b_kwargs={param: 'bar'},
                  equals=False)),
            (param + '_equals',
             dict(a_kwargs={param: 'foo'},
                  b_kwargs={param: 'foo'},
                  equals=True)),
            (param + '_not_equals',
             dict(a_kwargs={param: 'foo'},
                  b_kwargs={param: 'bar'},
                  equals=False))
        ]
        cls.scenarios.extend(new_scenarios)

    def _test_equality(self):
        a = messaging.Target(**self.a_kwargs)
        b = messaging.Target(**self.b_kwargs)

        if self.equals:
            self.assertEquals(a, b)
            self.assertFalse(a != b)
        else:
            self.assertNotEquals(a, b)
            self.assertFalse(a == b)

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_equality)

    def test_equality_all_none(self):
        self._test_scenario('all_none')

    def test_equality_a_exchange(self):
        self._test_scenario('a_exchange')

    def test_equality_b_exchange(self):
        self._test_scenario('b_exchange')

    def test_equality_exchange_equals(self):
        self._test_scenario('exchange_equals')

    def test_equality_exchange_not_equals(self):
        self._test_scenario('exchange_not_equals')

    def test_equality_a_topic(self):
        self._test_scenario('a_topic')

    def test_equality_b_topic(self):
        self._test_scenario('b_topic')

    def test_equality_topic_equals(self):
        self._test_scenario('topic_equals')

    def test_equality_topic_not_equals(self):
        self._test_scenario('topic_not_equals')

    def test_equality_a_namespace(self):
        self._test_scenario('a_namespace')

    def test_equality_b_namespace(self):
        self._test_scenario('b_namespace')

    def test_equality_namespace_equals(self):
        self._test_scenario('namespace_equals')

    def test_equality_namespace_not_equals(self):
        self._test_scenario('namespace_not_equals')

    def test_equality_a_version(self):
        self._test_scenario('a_version')

    def test_equality_b_version(self):
        self._test_scenario('b_version')

    def test_equality_version_equals(self):
        self._test_scenario('version_equals')

    def test_equality_version_not_equals(self):
        self._test_scenario('version_not_equals')

    def test_equality_a_server(self):
        self._test_scenario('a_server')

    def test_equality_b_server(self):
        self._test_scenario('b_server')

    def test_equality_server_equals(self):
        self._test_scenario('server_equals')

    def test_equality_server_not_equals(self):
        self._test_scenario('server_not_equals')

    def test_equality_a_fanout(self):
        self._test_scenario('a_fanout')

    def test_equality_b_fanout(self):
        self._test_scenario('b_fanout')

    def test_equality_fanout_equals(self):
        self._test_scenario('fanout_equals')

    def test_equality_fanout_not_equals(self):
        self._test_scenario('fanout_not_equals')


EqualityTestCase._add_scenarios('exchange')
EqualityTestCase._add_scenarios('topic')
EqualityTestCase._add_scenarios('namespace')
EqualityTestCase._add_scenarios('version')
EqualityTestCase._add_scenarios('server')
EqualityTestCase._add_scenarios('fanout')
