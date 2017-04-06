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

import datetime
import logging
import logging.config
import os
import sys

from oslo_utils import timeutils
import testscenarios

import oslo_messaging
from oslo_messaging.tests.notify import test_notifier
from oslo_messaging.tests import utils as test_utils
from six.moves import mock


load_tests = testscenarios.load_tests_apply_scenarios

# Stolen from oslo.log
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
        self.addCleanup(oslo_messaging.notify._impl_test.reset)
        self.config(driver=['test'],
                    group='oslo_messaging_notifications')
        # NOTE(jamespage) disable thread information logging for testing
        # as this causes test failures when zmq tests monkey_patch via
        # eventlet
        logging.logThreads = 0

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_logger(self, mock_utcnow):
        with mock.patch('oslo_messaging.transport._get_transport',
                        return_value=test_notifier._FakeTransport(self.conf)):
            self.logger = oslo_messaging.LoggingNotificationHandler('test://')

        mock_utcnow.return_value = datetime.datetime.utcnow()

        levelno = getattr(logging, self.priority.upper(), 42)

        record = logging.LogRecord('foo',
                                   levelno,
                                   '/foo/bar',
                                   42,
                                   'Something happened',
                                   None,
                                   None)

        self.logger.emit(record)

        context = oslo_messaging.notify._impl_test.NOTIFICATIONS[0][0]
        self.assertEqual({}, context)

        n = oslo_messaging.notify._impl_test.NOTIFICATIONS[0][1]
        self.assertEqual(getattr(self, 'queue', self.priority.upper()),
                         n['priority'])
        self.assertEqual('logrecord', n['event_type'])
        self.assertEqual(str(timeutils.utcnow()), n['timestamp'])
        self.assertIsNone(n['publisher_id'])
        self.assertEqual(
            {'process': os.getpid(),
             'funcName': None,
             'name': 'foo',
             'thread': None,
             'levelno': levelno,
             'processName': 'MainProcess',
             'pathname': '/foo/bar',
             'lineno': 42,
             'msg': 'Something happened',
             'exc_info': None,
             'levelname': logging.getLevelName(levelno),
             'extra': None},
            n['payload'])

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_logging_conf(self, mock_utcnow):
        with mock.patch('oslo_messaging.transport._get_transport',
                        return_value=test_notifier._FakeTransport(self.conf)):
            logging.config.dictConfig({
                'version': 1,
                'handlers': {
                    'notification': {
                        'class': 'oslo_messaging.LoggingNotificationHandler',
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

        mock_utcnow.return_value = datetime.datetime.utcnow()

        levelno = getattr(logging, self.priority.upper())

        logger = logging.getLogger('default')
        lineno = sys._getframe().f_lineno + 1
        logger.log(levelno, 'foobar')

        n = oslo_messaging.notify._impl_test.NOTIFICATIONS[0][1]
        self.assertEqual(getattr(self, 'queue', self.priority.upper()),
                         n['priority'])
        self.assertEqual('logrecord', n['event_type'])
        self.assertEqual(str(timeutils.utcnow()), n['timestamp'])
        self.assertIsNone(n['publisher_id'])
        pathname = __file__
        if pathname.endswith(('.pyc', '.pyo')):
            pathname = pathname[:-1]
        self.assertDictEqual(
            n['payload'],
            {'process': os.getpid(),
             'funcName': 'test_logging_conf',
             'name': 'default',
             'thread': None,
             'levelno': levelno,
             'processName': 'MainProcess',
             'pathname': pathname,
             'lineno': lineno,
             'msg': 'foobar',
             'exc_info': None,
             'levelname': logging.getLevelName(levelno),
             'extra': None})
