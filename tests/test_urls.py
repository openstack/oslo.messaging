
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


class TestParseURL(test_utils.BaseTestCase):

    scenarios = [
        ('transport',
         dict(url='foo:',
              expect=dict(transport='foo',
                          virtual_host=None,
                          hosts=[],
                          parameters={}))),
        ('virtual_host_slash',
         dict(url='foo:////',
              expect=dict(transport='foo',
                          virtual_host='/',
                          hosts=[],
                          parameters={}))),
        ('virtual_host',
         dict(url='foo:///bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[],
                          parameters={}))),
        ('host',
         dict(url='foo://host/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host',
                                   username='',
                                   password=''),
                          ],
                          parameters={}))),
        ('port',
         dict(url='foo://host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='',
                                   password=''),
                          ],
                          parameters={}))),
        ('username',
         dict(url='foo://u@host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='u',
                                   password=''),
                          ],
                          parameters={}))),
        ('password',
         dict(url='foo://u:p@host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host:1234',
                                   username='u',
                                   password='p'),
                          ],
                          parameters={}))),
        ('multi_host',
         dict(url='foo://u:p@host1:1234,host2:4321/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
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
              expect=dict(transport='foo',
                          virtual_host='bar',
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
