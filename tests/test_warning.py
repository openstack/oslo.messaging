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

import imp
import os
import warnings

import mock
from oslotest import base as test_base
import six


class DeprecationWarningTest(test_base.BaseTestCase):

    @mock.patch('warnings.warn')
    def test_warning(self, mock_warn):
        import oslo.messaging
        imp.reload(oslo.messaging)
        self.assertTrue(mock_warn.called)
        args = mock_warn.call_args
        self.assertIn('oslo_messaging', args[0][0])
        self.assertIn('deprecated', args[0][0])
        self.assertTrue(issubclass(args[0][1], DeprecationWarning))

    def test_real_warning(self):
        with warnings.catch_warnings(record=True) as warning_msgs:
            warnings.resetwarnings()
            warnings.simplefilter('always', DeprecationWarning)
            import oslo.messaging

            # Use a separate function to get the stack level correct
            # so we know the message points back to this file. This
            # corresponds to an import or reload, which isn't working
            # inside the test under Python 3.3. That may be due to a
            # difference in the import implementation not triggering
            # warnings properly when the module is reloaded, or
            # because the warnings module is mostly implemented in C
            # and something isn't cleanly resetting the global state
            # used to track whether a warning needs to be
            # emitted. Whatever the cause, we definitely see the
            # warnings.warn() being invoked on a reload (see the test
            # above) and warnings are reported on the console when we
            # run the tests. A simpler test script run outside of
            # testr does correctly report the warnings.
            def foo():
                oslo.messaging.deprecated()

            foo()
            self.assertEqual(1, len(warning_msgs))
            msg = warning_msgs[0]
            self.assertIn('oslo_messaging', six.text_type(msg.message))
            self.assertEqual('test_warning.py', os.path.basename(msg.filename))
