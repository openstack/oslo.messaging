# Copyright 2013 eNovance
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
import logging.config
import os

import mock
import testscenarios
import testtools

from oslo import messaging
from oslo.messaging.openstack.common import timeutils
from tests import test_notifier
from tests import utils as test_utils


load_tests = testscenarios.load_tests_apply_scenarios

# Stolen from openstack.common.logging
logging.AUDIT = logging.INFO + 1
logging.addLevelName(logging.AUDIT, 'AUDIT')


class TestLogNotifier(test_utils.BaseTestCase):

    scenarios = [
        ('debug', dict(priority='debug')),
        ('info', dict(priority='info')),
        ('warning', dict(priority='warning', queue='WARN')),
        ('warn', dict(priority='warn')),
        ('error', dict(priority='error')),
        ('critical', dict(priority='critical')),
        ('audit', dict(priority='audit')),
    ]

    def setUp(self):
        super(TestLogNotifier, self).setUp()
        self.addCleanup(timeutils.clear_time_override)
        self.addCleanup(messaging.notify._impl_test.reset)
        self.config(notification_driver=['test'])

    def test_logger(self):
        with mock.patch('oslo.messaging.transport.get_transport',
                        return_value=test_notifier._FakeTransport(self.conf)):
            self.logger = messaging.LoggingNotificationHandler('test://')

        timeutils.set_time_override()

        levelno = getattr(logging, self.priority.upper(), 42)

        record = logging.LogRecord('foo',
                                   levelno,
                                   '/foo/bar',
                                   42,
                                   'Something happened',
                                   None,
                                   None)

        self.logger.emit(record)

        n = messaging.notify._impl_test.NOTIFICATIONS[0][1]
        self.assertEqual(n['priority'],
                         getattr(self, 'queue', self.priority.upper()))
        self.assertEqual(n['event_type'], 'logrecord')
        self.assertEqual(n['timestamp'], str(timeutils.utcnow.override_time))
        self.assertEqual(n['publisher_id'], None)
        self.assertEqual(
            n['payload'],
            {'process': os.getpid(),
             'funcName': None,
             'name': 'foo',
             'thread': logging.thread.get_ident() if logging.thread else None,
             'levelno': levelno,
             'processName': 'MainProcess',
             'pathname': '/foo/bar',
             'lineno': 42,
             'msg': 'Something happened',
             'exc_info': None,
             'levelname': logging.getLevelName(levelno),
             'extra': None})

    @testtools.skipUnless(hasattr(logging.config, 'dictConfig'),
                          "Need logging.config.dictConfig (Python >= 2.7)")
    def test_logging_conf(self):
        with mock.patch('oslo.messaging.transport.get_transport',
                        return_value=test_notifier._FakeTransport(self.conf)):
            logging.config.dictConfig({
                'version': 1,
                'handlers': {
                    'notification': {
                        'class': 'oslo.messaging.LoggingNotificationHandler',
                        'level': self.priority.upper(),
                        'url': 'test://',
                    },
                },
                'loggers': {
                    'default': {
                        'handlers': ['notification'],
                        'level': self.priority.upper(),
                    },
                },
            })

        timeutils.set_time_override()

        levelno = getattr(logging, self.priority.upper())

        logger = logging.getLogger('default')
        logger.log(levelno, 'foobar')

        n = messaging.notify._impl_test.NOTIFICATIONS[0][1]
        self.assertEqual(n['priority'],
                         getattr(self, 'queue', self.priority.upper()))
        self.assertEqual(n['event_type'], 'logrecord')
        self.assertEqual(n['timestamp'], str(timeutils.utcnow.override_time))
        self.assertEqual(n['publisher_id'], None)
        self.assertDictEqual(
            n['payload'],
            {'process': os.getpid(),
             'funcName': 'test_logging_conf',
             'name': 'default',
             'thread': logging.thread.get_ident() if logging.thread else None,
             'levelno': levelno,
             'processName': 'MainProcess',
             'pathname': __file__[:-1],  # Remove the 'c' of .pyc
             'lineno': 121,
             'msg': 'foobar',
             'exc_info': None,
             'levelname': logging.getLevelName(levelno),
             'extra': None})
