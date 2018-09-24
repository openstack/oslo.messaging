
# Copyright 2014 Red Hat, Inc.
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
from six.moves import mock
import stevedore
import testtools

from oslo_messaging import server
try:
    from oslo_messaging import opts
except ImportError:
    opts = None
from oslo_messaging.tests import utils as test_utils


@testtools.skipIf(opts is None, "Options not importable")
class OptsTestCase(test_utils.BaseTestCase):

    def _test_list_opts(self, result):
        self.assertEqual(5, len(result))

        groups = [g for (g, l) in result]
        self.assertIn(None, groups)
        self.assertIn('oslo_messaging_amqp', groups)
        self.assertIn('oslo_messaging_notifications', groups)
        self.assertIn('oslo_messaging_rabbit', groups)
        self.assertIn('oslo_messaging_kafka', groups)

    def test_list_opts(self):
        self._test_list_opts(opts.list_opts())

    def test_entry_point(self):
        result = None
        for ext in stevedore.ExtensionManager('oslo.config.opts',
                                              invoke_on_load=True):
            if ext.name == "oslo.messaging":
                result = ext.obj
                break

        self.assertIsNotNone(result)
        self._test_list_opts(result)

    def test_defaults(self):
        transport = mock.Mock()
        transport.conf = self.conf

        class MessageHandlingServerImpl(server.MessageHandlingServer):
            def _create_listener(self):
                pass

            def _process_incoming(self, incoming):
                pass

        MessageHandlingServerImpl(transport, mock.Mock())
        opts.set_defaults(self.conf, executor_thread_pool_size=100)
        self.assertEqual(100, self.conf.executor_thread_pool_size)
