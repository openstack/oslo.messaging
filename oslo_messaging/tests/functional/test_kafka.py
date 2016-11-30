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

import time

from oslo_config import cfg

import oslo_messaging
from oslo_messaging.tests.functional import utils


class TestWithRealKafkaBroker(utils.SkipIfNoTransportURL):
    def setUp(self):
        super(TestWithRealKafkaBroker, self).setUp(conf=cfg.ConfigOpts())
        if not self.url.startswith('kafka://'):
            self.skipTest("TRANSPORT_URL is not set to kafka driver")
        transport = oslo_messaging.get_transport(self.conf, self.url)
        self.driver = transport._driver

    def test_send_and_receive_message(self):
        target = oslo_messaging.Target(
            topic="fake_topic", exchange='fake_exchange')
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener
        fake_context = {"fake_context_key": "fake_context_value"}
        fake_message = {"fake_message_key": "fake_message_value"}
        self.driver.send_notification(
            target, fake_context, fake_message, None)

        received_message = listener.poll()[0]
        self.assertEqual(fake_context, received_message.ctxt)
        self.assertEqual(fake_message, received_message.message)

    def test_send_and_receive_message_without_exchange(self):
        target = oslo_messaging.Target(topic="fake_no_exchange_topic")
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener
        fake_context = {"fake_context_key": "fake_context_value"}
        fake_message = {"fake_message_key": "fake_message_value"}
        self.driver.send_notification(
            target, fake_context, fake_message, None)

        received_message = listener.poll()[0]
        self.assertEqual(fake_context, received_message.ctxt)
        self.assertEqual(fake_message, received_message.message)

    def test_receive_message_from_empty_topic_with_timeout(self):
        target = oslo_messaging.Target(
            topic="fake_empty_topic", exchange='fake_empty_exchange')
        targets_and_priorities = [(target, 'fake_info')]

        listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener

        deadline = time.time() + 3
        received_message = listener.poll(batch_timeout=3)
        self.assertEqual(0, int(deadline - time.time()))
        self.assertEqual([], received_message)
