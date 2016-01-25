# Copyright 2015 IBM Corp.
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

import mock

from oslo_messaging.tests import utils as test_utils


class TestDeprecationWarning(test_utils.BaseTestCase):

    @mock.patch('warnings.warn')
    def test_impl_messaging_deprecation_warning(self, mock_warn):
        # Tests that we get a deprecation warning when loading a messaging
        # driver out of oslo_messaging.notify._impl_messaging.
        from oslo_messaging.notify import _impl_messaging as messaging
        driver = messaging.MessagingV2Driver(  # noqa
            conf={}, topics=['notifications'], transport='rpc')
        # Make sure we got a deprecation warning by loading from the alias
        self.assertEqual(1, mock_warn.call_count)
