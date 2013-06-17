
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

from oslo.messaging import _urls as urls
from tests import utils as test_utils


load_tests = testscenarios.load_tests_apply_scenarios


class TestExchangeFromURL(test_utils.BaseTestCase):

    _notset = object()

    scenarios = [
        ('none_url_no_default',
         dict(url=None, default=_notset, expect=None)),
        ('empty_url_no_default',
         dict(url='', default=_notset, expect=None)),
        ('empty_url_none_default',
         dict(url='foo:///', default=None, expect=None)),
        ('empty_url_with_default',
         dict(url='foo:///', default='bar', expect='bar')),
        ('url_with_no_default',
         dict(url='foo:///bar', default=_notset, expect='bar')),
        ('url_with_none_default',
         dict(url='foo:///bar', default=None, expect='bar')),
        ('url_with_none_default',
         dict(url='foo:///bar', default='blaa', expect='bar')),
        ('multipart_url',
         dict(url='foo:///bar/blaa', default=None, expect='bar')),
        ('invalid_url',
         dict(url='hooha', default='blaa', expect='blaa')),
    ]

    def test_exchange_from_url(self):
        kwargs = {}
        if self.default is not self._notset:
            kwargs['default_exchange'] = self.default

        self.assertEqual(urls.exchange_from_url(self.url, **kwargs),
                         self.expect)


class TestParseURL(test_utils.BaseTestCase):

    scenarios = [
        ('transport',
         dict(url='foo:',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange=None,
                          hosts=[],
                          parameters={}))),
        ('default_exchange',
         dict(url='foo:///bar',
              default_exchange='bar',
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[],
                          parameters={}))),
        ('exchange',
         dict(url='foo:///bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[],
                          parameters={}))),
        ('host',
         dict(url='foo://host/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host',
                                   username='',
                                   password=''),
                          ],
                          parameters={}))),
        ('port',
         dict(url='foo://host:1234/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='',
                                   password=''),
                          ],
                          parameters={}))),
        ('username',
         dict(url='foo://u@host:1234/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='u',
                                   password=''),
                          ],
                          parameters={}))),
        ('password',
         dict(url='foo://u:p@host:1234/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='u',
                                   password='p'),
                          ],
                          parameters={}))),
        ('multi_host',
         dict(url='foo://u:p@host1:1234,host2:4321/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host1:1234',
                                   username='u',
                                   password='p'),
                              dict(host='host2:4321',
                                   username='u',
                                   password='p'),
                          ],
                          parameters={}))),
        ('multi_creds',
         dict(url='foo://u1:p1@host1:1234,u2:p2@host2:4321/bar',
              default_exchange=None,
              expect=dict(transport='foo',
                          exchange='bar',
                          hosts=[
                              dict(host='host1:1234',
                                   username='u1',
                                   password='p1'),
                              dict(host='host2:4321',
                                   username='u2',
                                   password='p2'),
                          ],
                          parameters={}))),
    ]

    def test_parse_url(self):
        self.assertEqual(urls.parse_url(self.url), self.expect)
