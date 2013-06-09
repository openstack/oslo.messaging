
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

import itertools

import fixtures
import mox
from oslo.config import cfg
from stevedore import driver

from openstack.common import messaging
from openstack.common.messaging import transport
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


class _FakeDriver(object):

    def __init__(self, conf):
        self.conf = conf

    def send(self, *args, **kwargs):
        pass

    def listen(self, target):
        pass


class _FakeManager(object):

    def __init__(self, driver):
        self.driver = driver


class GetTransportTestCase(test_utils.BaseTestCase):

    scenarios = [
        ('all_none',
         dict(url=None, transport_url=None, rpc_backend=None,
              control_exchange=None,
              expect=dict(backend=None,
                          exchange=None,
                          url=None),
              ex=None)),
        ('rpc_backend',
         dict(url=None, transport_url=None, rpc_backend='testbackend',
              control_exchange=None,
              expect=dict(backend='testbackend',
                          exchange=None,
                          url=None),
              ex=None)),
        ('control_exchange',
         dict(url=None, transport_url=None, rpc_backend=None,
              control_exchange='testexchange',
              expect=dict(backend=None,
                          exchange='testexchange',
                          url=None),
              ex=None)),
        ('transport_url',
         dict(url=None, transport_url='testtransport:', rpc_backend=None,
              control_exchange=None,
              expect=dict(backend='testtransport',
                          exchange=None,
                          url='testtransport:'),
              ex=None)),
        ('url_param',
         dict(url='testtransport:', transport_url=None, rpc_backend=None,
              control_exchange=None,
              expect=dict(backend='testtransport',
                          exchange=None,
                          url='testtransport:'),
              ex=None)),
        ('invalid_transport_url',
         dict(url=None, transport_url='invalid', rpc_backend=None,
              control_exchange=None,
              expect=None,
              ex=dict(cls=messaging.InvalidTransportURL,
                      msg_contains='No scheme specified',
                      url='invalid'))),
        ('invalid_url_param',
         dict(url='invalid', transport_url=None, rpc_backend=None,
              control_exchange=None,
              expect=None,
              ex=dict(cls=messaging.InvalidTransportURL,
                      msg_contains='No scheme specified',
                      url='invalid'))),
        ('driver_load_failure',
         dict(url=None, transport_url=None, rpc_backend='testbackend',
              control_exchange=None,
              expect=dict(backend='testbackend',
                          exchange=None,
                          url=None),
              ex=dict(cls=messaging.DriverLoadFailure,
                      msg_contains='Failed to load',
                      driver='testbackend'))),
    ]

    def setUp(self):
        super(GetTransportTestCase, self).setUp(conf=cfg.ConfigOpts())
        self.conf.register_opts(transport._transport_opts)

    def _test_get_transport_happy(self):
        self.config(rpc_backend=self.rpc_backend,
                    control_exchange=self.control_exchange,
                    transport_url=self.transport_url)

        self.mox.StubOutWithMock(driver, 'DriverManager')

        invoke_args = [self.conf]
        invoke_kwds = dict(default_exchange=self.expect['exchange'])
        if self.expect['url']:
            invoke_kwds['url'] = self.expect['url']

        drvr = _FakeDriver(self.conf)
        driver.DriverManager('openstack.common.messaging.drivers',
                             self.expect['backend'],
                             invoke_on_load=True,
                             invoke_args=invoke_args,
                             invoke_kwds=invoke_kwds).\
            AndReturn(_FakeManager(drvr))

        self.mox.ReplayAll()

        transport = messaging.get_transport(self.conf, url=self.url)

        self.assertTrue(transport is not None)
        self.assertTrue(transport.conf is self.conf)
        self.assertTrue(transport._driver is drvr)

    def _test_get_transport_sad(self):
        self.config(rpc_backend=self.rpc_backend,
                    control_exchange=self.control_exchange,
                    transport_url=self.transport_url)

        if self.expect:
            self.mox.StubOutWithMock(driver, 'DriverManager')

            invoke_args = [self.conf]
            invoke_kwds = dict(default_exchange=self.expect['exchange'])
            if self.expect['url']:
                invoke_kwds['url'] = self.expect['url']

            driver.DriverManager('openstack.common.messaging.drivers',
                                 self.expect['backend'],
                                 invoke_on_load=True,
                                 invoke_args=invoke_args,
                                 invoke_kwds=invoke_kwds).\
                AndRaise(RuntimeError())

            self.mox.ReplayAll()

        try:
            messaging.get_transport(self.conf, url=self.url)
            self.assertFalse(True)
        except Exception as ex:
            ex_cls = self.ex.pop('cls')
            ex_msg_contains = self.ex.pop('msg_contains')

            self.assertTrue(isinstance(ex, messaging.MessagingException))
            self.assertTrue(isinstance(ex, ex_cls))
            self.assertTrue(hasattr(ex, 'msg'))
            self.assertTrue(ex_msg_contains in ex.msg)

            for k, v in self.ex.items():
                self.assertTrue(hasattr(ex, k))
                self.assertEquals(getattr(ex, k), v)

    def _test_get_transport(self):
        if self.ex is None:
            self._test_get_transport_happy()
        else:
            self._test_get_transport_sad()

    def _test_scenario(self, name):
        _test_scenario(name, self.scenarios, self, self._test_get_transport)

    def test_get_transport_all_none(self):
        self._test_scenario('all_none')

    def test_get_transport_rpc_backend(self):
        self._test_scenario('rpc_backend')

    def test_get_transport_control_exchange(self):
        self._test_scenario('control_exchange')

    def test_get_transport_transport_url(self):
        self._test_scenario('transport_url')

    def test_get_transport_url_param(self):
        self._test_scenario('url_param')

    def test_get_transport_invalid_transport_url(self):
        self._test_scenario('invalid_transport_url')

    def test_get_transport_invalid_url_param(self):
        self._test_scenario('invalid_url_param')

    def test_get_transport_driver_load_failure(self):
        self._test_scenario('driver_load_failure')


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
            return next(itertools.ifilter(key, seq), default)

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
        self.useFixture(_SetDefaultsFixture(transport.set_defaults,
                                            transport._transport_opts,
                                            'control_exchange'))

    def test_set_default_control_exchange(self):
        transport.set_defaults(control_exchange='foo')

        self.mox.StubOutWithMock(driver, 'DriverManager')
        invoke_kwds = mox.ContainsKeyValue('default_exchange', 'foo')
        driver.DriverManager(mox.IgnoreArg(),
                             mox.IgnoreArg(),
                             invoke_on_load=mox.IgnoreArg(),
                             invoke_args=mox.IgnoreArg(),
                             invoke_kwds=invoke_kwds).\
            AndReturn(_FakeManager(_FakeDriver(self.conf)))
        self.mox.ReplayAll()

        messaging.get_transport(self.conf)


class TestTransportMethodArgs(test_utils.BaseTestCase):

    def test_send_defaults(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        self.mox.StubOutWithMock(t._driver, 'send')
        t._driver.send('target', 'ctxt', 'message',
                       wait_for_reply=None,
                       timeout=None,
                       envelope=False)
        self.mox.ReplayAll()

        t._send('target', 'ctxt', 'message')

    def test_send_all_args(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        self.mox.StubOutWithMock(t._driver, 'send')
        t._driver.send('target', 'ctxt', 'message',
                       wait_for_reply='wait_for_reply',
                       timeout='timeout',
                       envelope='envelope')
        self.mox.ReplayAll()

        t._send('target', 'ctxt', 'message',
                wait_for_reply='wait_for_reply',
                timeout='timeout',
                envelope='envelope')

    def test_listen(self):
        t = transport.Transport(_FakeDriver(cfg.CONF))

        self.mox.StubOutWithMock(t._driver, 'listen')
        t._driver.listen('target')
        self.mox.ReplayAll()

        t._listen('target')
