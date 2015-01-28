
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
import pkg_resources
import testtools

try:
    from oslo_messaging import opts
except ImportError:
    opts = None
from oslo_messaging.tests import utils as test_utils


class OptsTestCase(test_utils.BaseTestCase):

    @testtools.skipIf(opts is None, "Options not importable")
    def setUp(self):
        super(OptsTestCase, self).setUp()

    def _test_list_opts(self, result):
        self.assertEqual(6, len(result))

        groups = [g for (g, l) in result]
        self.assertIn(None, groups)
        self.assertIn('matchmaker_ring', groups)
        self.assertIn('matchmaker_redis', groups)
        self.assertIn('oslo_messaging_amqp', groups)
        self.assertIn('oslo_messaging_rabbit', groups)
        self.assertIn('oslo_messaging_qpid', groups)

        opt_names = [o.name for (g, l) in result for o in l]
        self.assertIn('rpc_backend', opt_names)

    def test_list_opts(self):
        self._test_list_opts(opts.list_opts())

    def test_entry_point(self):
        result = None
        for ep in pkg_resources.iter_entry_points('oslo.config.opts'):
            if ep.name == "oslo.messaging":
                list_fn = ep.load()
                result = list_fn()
                break

        self.assertIsNotNone(result)
        self._test_list_opts(result)
