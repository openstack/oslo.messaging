# Copyright 2015 Mirantis Inc.
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

from oslo_messaging import conffixture
from oslo_messaging.tests import utils as test_utils


class TestConfFixture(test_utils.BaseTestCase):

    def test_fixture_wraps_set_override(self):
        conf = self.messaging_conf.conf
        self.assertIsNotNone(conf.set_override.wrapped)
        self.messaging_conf._teardown_decorator()
        self.assertFalse(hasattr(conf.set_override, 'wrapped'))

    def test_fixture_wraps_clear_override(self):
        conf = self.messaging_conf.conf
        self.assertIsNotNone(conf.clear_override.wrapped)
        self.messaging_conf._teardown_decorator()
        self.assertFalse(hasattr(conf.clear_override, 'wrapped'))

    def test_fixture_setup_teardown_decorator(self):
        conf = cfg.ConfigOpts()
        self.assertFalse(hasattr(conf.set_override, 'wrapped'))
        self.assertFalse(hasattr(conf.clear_override, 'wrapped'))
        fixture = conffixture.ConfFixture(conf)
        self.assertFalse(hasattr(conf.set_override, 'wrapped'))
        self.assertFalse(hasattr(conf.clear_override, 'wrapped'))
        self.useFixture(fixture)
        self.assertTrue(hasattr(conf.set_override, 'wrapped'))
        self.assertTrue(hasattr(conf.clear_override, 'wrapped'))
        fixture._teardown_decorator()
        self.assertFalse(hasattr(conf.set_override, 'wrapped'))
        self.assertFalse(hasattr(conf.clear_override, 'wrapped'))

    def test_fixture_properties(self):
        conf = self.messaging_conf.conf
        self.messaging_conf.transport_driver = 'fake'
        self.assertEqual('fake',
                         self.messaging_conf.transport_driver)
        self.assertEqual('fake',
                         conf.rpc_backend)

    def test_old_notifications_config_override(self):
        conf = self.messaging_conf.conf
        conf.set_override(
            "notification_driver", ["messaging"])
        conf.set_override(
            "notification_transport_url", "http://xyz")
        conf.set_override(
            "notification_topics", ['topic1'])

        self.assertEqual(["messaging"],
                         conf.oslo_messaging_notifications.driver)
        self.assertEqual("http://xyz",
                         conf.oslo_messaging_notifications.transport_url)
        self.assertEqual(['topic1'],
                         conf.oslo_messaging_notifications.topics)

        conf.clear_override("notification_driver")
        conf.clear_override("notification_transport_url")
        conf.clear_override("notification_topics")

        self.assertEqual([],
                         conf.oslo_messaging_notifications.driver)
        self.assertIsNone(conf.oslo_messaging_notifications.transport_url)
        self.assertEqual(['notifications'],
                         conf.oslo_messaging_notifications.topics)
