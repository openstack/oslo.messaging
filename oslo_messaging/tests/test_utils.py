
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

from oslo_messaging._drivers import common
from oslo_messaging import _utils as utils
from oslo_messaging.tests import utils as test_utils
from six.moves import mock


class VersionIsCompatibleTestCase(test_utils.BaseTestCase):
    def test_version_is_compatible_same(self):
        self.assertTrue(utils.version_is_compatible('1.23', '1.23'))

    def test_version_is_compatible_newer_minor(self):
        self.assertTrue(utils.version_is_compatible('1.24', '1.23'))

    def test_version_is_compatible_older_minor(self):
        self.assertFalse(utils.version_is_compatible('1.22', '1.23'))

    def test_version_is_compatible_major_difference1(self):
        self.assertFalse(utils.version_is_compatible('2.23', '1.23'))

    def test_version_is_compatible_major_difference2(self):
        self.assertFalse(utils.version_is_compatible('1.23', '2.23'))

    def test_version_is_compatible_newer_rev(self):
        self.assertFalse(utils.version_is_compatible('1.23', '1.23.1'))

    def test_version_is_compatible_newer_rev_both(self):
        self.assertFalse(utils.version_is_compatible('1.23.1', '1.23.2'))

    def test_version_is_compatible_older_rev_both(self):
        self.assertTrue(utils.version_is_compatible('1.23.2', '1.23.1'))

    def test_version_is_compatible_older_rev(self):
        self.assertTrue(utils.version_is_compatible('1.24', '1.23.1'))

    def test_version_is_compatible_no_rev_is_zero(self):
        self.assertTrue(utils.version_is_compatible('1.23.0', '1.23'))


class TimerTestCase(test_utils.BaseTestCase):
    def test_no_duration_no_callback(self):
        t = common.DecayingTimer()
        t.start()
        remaining = t.check_return()
        self.assertIsNone(remaining)

    def test_no_duration_but_maximum(self):
        t = common.DecayingTimer()
        t.start()
        remaining = t.check_return(maximum=2)
        self.assertEqual(2, remaining)

    @mock.patch('oslo_utils.timeutils.now')
    def test_duration_expired_no_callback(self, now):
        now.return_value = 0
        t = common.DecayingTimer(2)
        t.start()

        now.return_value = 3
        remaining = t.check_return()
        self.assertEqual(0, remaining)

    @mock.patch('oslo_utils.timeutils.now')
    def test_duration_callback(self, now):
        now.return_value = 0
        t = common.DecayingTimer(2)
        t.start()

        now.return_value = 3
        callback = mock.Mock()
        remaining = t.check_return(callback)
        self.assertEqual(0, remaining)
        callback.assert_called_once_with()

    @mock.patch('oslo_utils.timeutils.now')
    def test_duration_callback_with_args(self, now):
        now.return_value = 0
        t = common.DecayingTimer(2)
        t.start()

        now.return_value = 3
        callback = mock.Mock()
        remaining = t.check_return(callback, 1, a='b')
        self.assertEqual(0, remaining)
        callback.assert_called_once_with(1, a='b')
