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

from oslo_messaging.tests import utils as test_utils


class TestConfFixture(test_utils.BaseTestCase):
    def test_old_notifications_config_override(self):
        conf = self.messaging_conf.conf
        conf.set_override(
            "notification_driver", "messaging")
        conf.set_override(
            "notification_transport_url", "http://xyz")
        conf.set_override(
            "notification_topics", ['topic1'])

        self.assertEqual("messaging",
                         conf.oslo_messaging_notifications.driver)
        self.assertEqual("http://xyz",
                         conf.oslo_messaging_notifications.transport_url)
        self.assertEqual(['topic1'],
                         conf.oslo_messaging_notifications.topics)
