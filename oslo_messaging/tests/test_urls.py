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


class TestParseURL(test_utils.BaseTestCase):

    scenarios = [
        ('transport',
         dict(url='foo:',
              expect=dict(transport='foo'))),
        ('virtual_host_slash',
         dict(url='foo:////',
              expect=dict(transport='foo', virtual_host='/'))),
        ('virtual_host',
         dict(url='foo:///bar',
              expect=dict(transport='foo', virtual_host='bar'))),
        ('host',
         dict(url='foo://host/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host'),
                          ]))),
        ('ipv6_host',
         dict(url='foo://[ffff::1]/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='ffff::1'),
                          ]))),
        ('port',
         dict(url='foo://host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host', port=1234),
                          ]))),
        ('ipv6_port',
         dict(url='foo://[ffff::1]:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='ffff::1', port=1234),
                          ]))),
        ('username',
         dict(url='foo://u@host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host', port=1234, username='u'),
                          ]))),
        ('password',
         dict(url='foo://u:p@host:1234/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host', port=1234,
                                   username='u', password='p'),
                          ]))),
        ('creds_no_host',
         dict(url='foo://u:p@/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(username='u', password='p'),
                          ]))),
        ('multi_host',
         dict(url='foo://u:p@host1:1234,host2:4321/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host1', port=1234,
                                   username='u', password='p'),
                              dict(host='host2', port=4321),
                          ]))),
        ('multi_host_partial_creds',
         dict(url='foo://u:p@host1,host2/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host1', username='u', password='p'),
                              dict(host='host2'),
                          ]))),
        ('multi_creds',
         dict(url='foo://u1:p1@host1:1234,u2:p2@host2:4321/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='host1', port=1234,
                                   username='u1', password='p1'),
                              dict(host='host2', port=4321,
                                   username='u2', password='p2'),
                          ]))),
        ('multi_creds_ipv6',
         dict(url='foo://u1:p1@[ffff::1]:1234,u2:p2@[ffff::2]:4321/bar',
              expect=dict(transport='foo',
                          virtual_host='bar',
                          hosts=[
                              dict(host='ffff::1', port=1234,
                                   username='u1', password='p1'),
                              dict(host='ffff::2', port=4321,
                                   username='u2', password='p2'),
                          ]))),
        ('quoting',
         dict(url='foo://u%24:p%26@host:1234/%24',
              expect=dict(transport='foo',
                          virtual_host='$',
                          hosts=[
                              dict(host='host', port=1234,
                                   username='u$', password='p&'),
                          ]))),
    ]

    def test_parse_url(self):
        url = oslo_messaging.TransportURL.parse(self.conf, self.url)

        hosts = []
        for host in self.expect.get('hosts', []):
            hosts.append(oslo_messaging.TransportHost(host.get('host'),
                                                      host.get('port'),
                                                      host.get('username'),
                                                      host.get('password')))
        expected = oslo_messaging.TransportURL(self.conf,
                                               self.expect.get('transport'),
                                               self.expect.get('virtual_host'),
                                               hosts)

        self.assertEqual(expected, url)


class TestFormatURL(test_utils.BaseTestCase):

    scenarios = [
        ('transport',
         dict(transport='testtransport',
              virtual_host=None,
              hosts=[],
              expected='testtransport:///')),
        ('virtual_host',
         dict(transport='testtransport',
              virtual_host='/vhost',
              hosts=[],
              expected='testtransport:////vhost')),
        ('host',
         dict(transport='testtransport',
              virtual_host='/',
              hosts=[
                  dict(hostname='host',
                       port=10,
                       username='bob',
                       password='secret'),
              ],
              expected='testtransport://bob:secret@host:10//')),
        ('multi_host',
         dict(transport='testtransport',
              virtual_host='',
              hosts=[
                  dict(hostname='h1',
                       port=1000,
                       username='b1',
                       password='s1'),
                  dict(hostname='h2',
                       port=2000,
                       username='b2',
                       password='s2'),
              ],
              expected='testtransport://b1:s1@h1:1000,b2:s2@h2:2000/')),
        ('quoting',
         dict(transport='testtransport',
              virtual_host='/$',
              hosts=[
                  dict(hostname='host',
                       port=10,
                       username='b$',
                       password='s&'),
              ],
              expected='testtransport://b%24:s%26@host:10//%24')),
    ]

    def test_parse_url(self):
        hosts = []
        for host in self.hosts:
            hosts.append(oslo_messaging.TransportHost(host.get('hostname'),
                                                      host.get('port'),
                                                      host.get('username'),
                                                      host.get('password')))

        url = oslo_messaging.TransportURL(self.conf, self.transport,
                                          self.virtual_host, hosts)

        self.assertEqual(self.expected, str(url))
