# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging

import fixtures

import oslo_messaging
from oslo_messaging.notify import log_handler
from oslo_messaging.tests import utils as test_utils
from six.moves import mock


class PublishErrorsHandlerTestCase(test_utils.BaseTestCase):
    """Tests for log.PublishErrorsHandler"""
    def setUp(self):
        super(PublishErrorsHandlerTestCase, self).setUp()
        self.publisherrorshandler = (log_handler.
                                     PublishErrorsHandler(logging.ERROR))

    def test_emit_cfg_log_notifier_in_notifier_drivers(self):
        drivers = ['messaging', 'log']
        self.config(driver=drivers,
                    group='oslo_messaging_notifications')
        self.stub_flg = True

        transport = oslo_messaging.get_notification_transport(self.conf)
        notifier = oslo_messaging.Notifier(transport)

        def fake_notifier(*args, **kwargs):
            self.stub_flg = False

        self.useFixture(fixtures.MockPatchObject(
            notifier, 'error', fake_notifier))

        logrecord = logging.LogRecord(name='name', level='WARN',
                                      pathname='/tmp', lineno=1, msg='Message',
                                      args=None, exc_info=None)
        self.publisherrorshandler.emit(logrecord)
        self.assertTrue(self.stub_flg)

    @mock.patch('oslo_messaging.notify.notifier.Notifier._notify')
    def test_emit_notification(self, mock_notify):
        logrecord = logging.LogRecord(name='name', level='ERROR',
                                      pathname='/tmp', lineno=1, msg='Message',
                                      args=None, exc_info=None)
        self.publisherrorshandler.emit(logrecord)
        self.assertEqual('error.publisher',
                         self.publisherrorshandler._notifier.publisher_id)
        mock_notify.assert_called_with({},
                                       'error_notification',
                                       {'error': 'Message'}, 'ERROR')
