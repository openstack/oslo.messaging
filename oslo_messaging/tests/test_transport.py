
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

import fixtures
from oslo_config import cfg
import six
from six.moves import mock
from stevedore import driver
import testscenarios

import oslo_messaging
from oslo_messaging.tests import utils as test_utils
from oslo_messaging import transport

load_tests = testscenarios.load_tests_apply_scenarios


class _FakeDriver(object):

    def __init__(self, conf):
        self.conf = conf

    def send(self, *args, **kwargs):
        pass

    def send_notification(self, *args, **kwargs):
        pass

    def listen(self, target, batch_size, batch_timeout):
        pass


class _FakeManager(object):

    def __init__(self, driver):
        self.driver = driver


class GetTransportTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('default',
         dict(url=None, transport_url=None,
              control_exchange=None, allowed=None,
              expect=dict(backend='rabbit',
                          exchange=None,
                          url='rabbit:',
                          allowed=[]))),
        ('transport_url',
         dict(url=None, transport_url='testtransport:',
              control_exchange=None, allowed=None,
              expect=dict(backend='testtransport',
                          exchange=None,
                          url='testtransport:',
                          allowed=[]))),
        ('url_param',
         dict(url='testtransport:', transport_url=None,
              control_exchange=None, allowed=None,
              expect=dict(backend='testtransport',
                          exchange=None,
                          url='testtransport:',
                          allowed=[]))),
        ('control_exchange',
         dict(url=None, transport_url='testbackend:',
              control_exchange='testexchange', allowed=None,
              expect=dict(backend='testbackend',
                          exchange='testexchange',
                          url='testbackend:',
                          allowed=[]))),
        ('allowed_remote_exmods',
         dict(url=None, transport_url='testbackend:',
              control_exchange=None, allowed=['foo', 'bar'],
              expect=dict(backend='testbackend',
                          exchange=None,
                          url='testbackend:',
                          allowed=['foo', 'bar']))),
    ]

    @mock.patch('oslo_messaging.transport.LOG')
    def test_get_transport(self, fake_logger):
        self.messaging_conf.reset()
        self.config(control_exchange=self.control_exchange)
        if self.transport_url:
            self.config(transport_url=self.transport_url)

        driver.DriverManager = mock.Mock()

        invoke_args = [self.conf,
                       oslo_messaging.TransportURL.parse(self.conf,
                                                         self.expect['url'])]
        invoke_kwds = dict(default_exchange=self.expect['exchange'],
                           allowed_remote_exmods=self.expect['allowed'])

        drvr = _FakeDriver(self.conf)

        driver.DriverManager.return_value = _FakeManager(drvr)

        kwargs = dict(url=self.url)
        if self.allowed is not None:
            kwargs['allowed_remote_exmods'] = self.allowed
        transport_ = oslo_messaging.get_transport(self.conf, **kwargs)

        self.assertIsNotNone(transport_)
        self.assertIs(transport_.conf, self.conf)
        self.assertIs(transport_._driver, drvr)
        self.assertTrue(isinstance(transport_, transport.RPCTransport))

        driver.DriverManager.assert_called_once_with('oslo.messaging.drivers',
                                                     self.expect['backend'],
                                                     invoke_on_load=True,
                                                     invoke_args=invoke_args,
                                                     invoke_kwds=invoke_kwds)


class GetTransportSadPathTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('invalid_transport_url',
         dict(url=None, transport_url='invalid',
              ex=dict(cls=oslo_messaging.InvalidTransportURL,
                      msg_contains='No scheme specified',
                      url='invalid'))),
        ('invalid_url_param',
         dict(url='invalid', transport_url=None,
              ex=dict(cls=oslo_messaging.InvalidTransportURL,
                      msg_contains='No scheme specified',
                      url='invalid'))),
        ('driver_load_failure',
         dict(url=None, transport_url='testbackend:/',
              ex=dict(cls=oslo_messaging.DriverLoadFailure,
                      msg_contains='Failed to load',
                      driver='testbackend'))),
    ]

    def test_get_transport_sad(self):
        self.config(transport_url=self.transport_url)
        ex_cls = self.ex.pop('cls')
        ex_msg_contains = self.ex.pop('msg_contains')
        ex = self.assertRaises(
            ex_cls, oslo_messaging.get_transport, self.conf, url=self.url)
        self.assertIn(ex_msg_contains, six.text_type(ex))
        for k, v in self.ex.items():
            self.assertTrue(hasattr(ex, k))
            self.assertEqual(v, str(getattr(ex, k)))


# FIXME(markmc): this could be used elsewhere
class _SetDefaultsFixture(fixtures.Fixture):

    def __init__(self, set_defaults, opts, *names):
        super(_SetDefaultsFixture, self).__init__()
        self.set_defaults = set_defaults
        self.opts = opts
        self.names = names

    def setUp(self):
        super(_SetDefaultsFixture, self).setUp()

        # FIXME(markmc): this comes from Id5c1f3ba
        def first(seq, default=None, key=None):
            if key is None:
                key = bool
            return next(six.moves.filter(key, seq), default)

        def default(opts, name):
            return first(opts, key=lambda o: o.name == name).default

        orig_defaults = {}
        for n in self.names:
            orig_defaults[n] = default(self.opts, n)

        def restore_defaults():
            self.set_defaults(**orig_defaults)

        self.addCleanup(restore_defaults)


class TestSetDefaults(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSetDefaults, self).setUp(conf=cfg.ConfigOpts())
        self.useFixture(_SetDefaultsFixture(
            oslo_messaging.set_transport_defaults,
            transport._transport_opts,
            'control_exchange'))

    def test_set_default_control_exchange(self):
        oslo_messaging.set_transport_defaults(control_exchange='foo')

        driver.DriverManager = mock.Mock()
        invoke_kwds = dict(default_exchange='foo', allowed_remote_exmods=[])

        driver.DriverManager.return_value = \
            _FakeManager(_FakeDriver(self.conf))

        oslo_messaging.get_transport(self.conf)

        driver.DriverManager.assert_called_once_with(mock.ANY,
                                                     mock.ANY,
                                                     invoke_on_load=mock.ANY,
                                                     invoke_args=mock.ANY,
                                                     invoke_kwds=invoke_kwds)


class TestTransportMethodArgs(test_utils.BaseTestCase):

    _target = oslo_messaging.Target(topic='topic', server='server')

    def test_send_defaults(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        t._driver.send = mock.Mock()

        t._send(self._target, 'ctxt', 'message')

        t._driver.send.assert_called_once_with(self._target,
                                               'ctxt',
                                               'message',
                                               wait_for_reply=None,
                                               timeout=None,
                                               call_monitor_timeout=None,
                                               retry=None)

    def test_send_all_args(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        t._driver.send = mock.Mock()

        t._send(self._target, 'ctxt', 'message',
                wait_for_reply='wait_for_reply',
                timeout='timeout', call_monitor_timeout='cm_timeout',
                retry='retry')

        t._driver.send.\
            assert_called_once_with(self._target,
                                    'ctxt',
                                    'message',
                                    wait_for_reply='wait_for_reply',
                                    timeout='timeout',
                                    call_monitor_timeout='cm_timeout',
                                    retry='retry')

    def test_send_notification(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        t._driver.send_notification = mock.Mock()

        t._send_notification(self._target, 'ctxt', 'message', version=1.0)

        t._driver.send_notification.assert_called_once_with(self._target,
                                                            'ctxt',
                                                            'message',
                                                            1.0,
                                                            retry=None)

    def test_send_notification_all_args(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        t._driver.send_notification = mock.Mock()

        t._send_notification(self._target, 'ctxt', 'message', version=1.0,
                             retry=5)

        t._driver.send_notification.assert_called_once_with(self._target,
                                                            'ctxt',
                                                            'message',
                                                            1.0,
                                                            retry=5)

    def test_listen(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        t._driver.listen = mock.Mock()

        t._listen(self._target, 1, None)

        t._driver.listen.assert_called_once_with(self._target, 1, None)


class TestTransportUrlCustomisation(test_utils.BaseTestCase):
    def setUp(self):
        super(TestTransportUrlCustomisation, self).setUp()

        def transport_url_parse(url):
            return transport.TransportURL.parse(self.conf, url)

        self.url1 = transport_url_parse(
            "fake:/vhost1/localhost:5672/?x=1&y=2&z=3")
        self.url2 = transport_url_parse("fake:/vhost2/localhost:5672/?foo=bar")
        self.url3 = transport_url_parse(
            "fake:/vhost1/localhost:5672/?l=1&l=2&l=3")
        self.url4 = transport_url_parse(
            "fake:/vhost2/localhost:5672/?d=x:1&d=y:2&d=z:3")
        self.url5 = transport_url_parse("fake://noport:/?")

    def test_hash(self):
        urls = {}
        urls[self.url1] = self.url1
        urls[self.url2] = self.url2
        urls[self.url3] = self.url3
        urls[self.url4] = self.url4
        urls[self.url5] = self.url5
        self.assertEqual(3, len(urls))

    def test_eq(self):
        self.assertEqual(self.url1, self.url3)
        self.assertEqual(self.url2, self.url4)
        self.assertNotEqual(self.url1, self.url4)

    def test_query(self):
        self.assertEqual({'x': '1', 'y': '2', 'z': '3'}, self.url1.query)
        self.assertEqual({'foo': 'bar'}, self.url2.query)
        self.assertEqual({'l': '1,2,3'}, self.url3.query)
        self.assertEqual({'d': 'x:1,y:2,z:3'}, self.url4.query)

    def test_noport(self):
        self.assertIsNone(self.url5.hosts[0].port)


class TestTransportHostCustomisation(test_utils.BaseTestCase):
    def setUp(self):
        super(TestTransportHostCustomisation, self).setUp()
        self.host1 = transport.TransportHost("host1", 5662, "user", "pass")
        self.host2 = transport.TransportHost("host1", 5662, "user", "pass")
        self.host3 = transport.TransportHost("host1", 5663, "user", "pass")
        self.host4 = transport.TransportHost("host1", 5662, "user2", "pass")
        self.host5 = transport.TransportHost("host1", 5662, "user", "pass2")
        self.host6 = transport.TransportHost("host2", 5662, "user", "pass")

    def test_hash(self):
        hosts = {}
        hosts[self.host1] = self.host1
        hosts[self.host2] = self.host2
        hosts[self.host3] = self.host3
        hosts[self.host4] = self.host4
        hosts[self.host5] = self.host5
        hosts[self.host6] = self.host6
        self.assertEqual(5, len(hosts))

    def test_eq(self):
        self.assertEqual(self.host1, self.host2)
        self.assertNotEqual(self.host1, self.host3)
        self.assertNotEqual(self.host1, self.host4)
        self.assertNotEqual(self.host1, self.host5)
        self.assertNotEqual(self.host1, self.host6)
