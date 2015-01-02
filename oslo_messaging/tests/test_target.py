
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
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


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

    def test_constructor(self):
        target = oslo_messaging.Target(**self.kwargs)
        for k in self.kwargs:
            self.assertEqual(self.kwargs[k], getattr(target, k))
        for k in ['exchange', 'topic', 'namespace',
                  'version', 'server', 'fanout']:
            if k in self.kwargs:
                continue
            self.assertIsNone(getattr(target, k))


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

    def test_callable(self):
        target = oslo_messaging.Target(**self.attrs)
        target = target(**self.kwargs)
        for k in self.vals:
            self.assertEqual(self.vals[k], getattr(target, k))
        for k in ['exchange', 'topic', 'namespace',
                  'version', 'server', 'fanout']:
            if k in self.vals:
                continue
            self.assertIsNone(getattr(target, k))


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

    def test_repr(self):
        target = oslo_messaging.Target(**self.kwargs)
        self.assertEqual('<Target ' + self.repr + '>', str(target))


_notset = object()


class EqualityTestCase(test_utils.BaseTestCase):

    @classmethod
    def generate_scenarios(cls):
        attr = [
            ('exchange', dict(attr='exchange')),
            ('topic', dict(attr='topic')),
            ('namespace', dict(attr='namespace')),
            ('version', dict(attr='version')),
            ('server', dict(attr='server')),
            ('fanout', dict(attr='fanout')),
        ]
        a = [
            ('a_notset', dict(a_value=_notset)),
            ('a_none', dict(a_value=None)),
            ('a_empty', dict(a_value='')),
            ('a_foo', dict(a_value='foo')),
            ('a_bar', dict(a_value='bar')),
        ]
        b = [
            ('b_notset', dict(b_value=_notset)),
            ('b_none', dict(b_value=None)),
            ('b_empty', dict(b_value='')),
            ('b_foo', dict(b_value='foo')),
            ('b_bar', dict(b_value='bar')),
        ]

        cls.scenarios = testscenarios.multiply_scenarios(attr, a, b)
        for s in cls.scenarios:
            s[1]['equals'] = (s[1]['a_value'] == s[1]['b_value'])

    def test_equality(self):
        a_kwargs = {self.attr: self.a_value}
        b_kwargs = {self.attr: self.b_value}

        a = oslo_messaging.Target(**a_kwargs)
        b = oslo_messaging.Target(**b_kwargs)

        if self.equals:
            self.assertEqual(a, b)
            self.assertFalse(a != b)
        else:
            self.assertNotEqual(a, b)
            self.assertFalse(a == b)


EqualityTestCase.generate_scenarios()
