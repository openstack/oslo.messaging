# Copyright 2015 NetEase Corp.
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

import logging

import testscenarios

import oslo_messaging
from oslo_messaging.tests.functional import utils

load_tests = testscenarios.load_tests_apply_scenarios


class LoggingNotificationHandlerTestCase(utils.SkipIfNoTransportURL):
    """Test case for `oslo_messaging.LoggingNotificationHandler`

    Build up a logger using this handler, then test logging under messaging and
    messagingv2 driver. Make sure receive expected logging notifications.
    """

    _priority = [
        ('debug', dict(priority='debug')),
        ('info', dict(priority='info')),
        ('warn', dict(priority='warn')),
        ('error', dict(priority='error')),
        ('critical', dict(priority='critical')),
    ]

    _driver = [
        ('messaging', dict(driver='messaging')),
        ('messagingv2', dict(driver='messagingv2')),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._priority,
                                                         cls._driver)

    def test_logging(self):
        # NOTE(gtt): Using different topic to make tests run in parallel
        topic = 'test_logging_%s_driver_%s' % (self.priority, self.driver)

        self.conf.notification_driver = [self.driver]
        self.conf.notification_topics = [topic]

        listener = self.useFixture(
            utils.NotificationFixture(self.conf, self.url, [topic]))

        log_notify = oslo_messaging.LoggingNotificationHandler(self.url)

        log = logging.getLogger(topic)
        log.setLevel(logging.DEBUG)
        log.addHandler(log_notify)

        log_method = getattr(log, self.priority)
        log_method('Test logging at priority: %s' % self.priority)

        events = listener.get_events(timeout=1)
        self.assertEqual(len(events), 1)

        info_event = events[0]

        self.assertEqual(info_event[0], self.priority)
        self.assertEqual(info_event[1], 'logrecord')

        for key in ['name', 'thread', 'extra', 'process', 'funcName',
                    'levelno', 'processName', 'pathname', 'lineno',
                    'msg', 'exc_info', 'levelname']:
            self.assertTrue(key in info_event[2])


LoggingNotificationHandlerTestCase.generate_scenarios()
