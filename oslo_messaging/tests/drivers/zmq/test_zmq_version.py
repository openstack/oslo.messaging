#    Copyright 2016 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging.tests import utils as test_utils


class Doer(object):

    def __init__(self):
        self.x = 1
        self.y = 2
        self.z = 3

    def _sudo(self):
        pass

    def do(self):
        pass

    def _do_v_1_1(self):
        pass

    def _do_v_2_2(self):
        pass

    def _do_v_3_3(self):
        pass


class TestZmqVersion(test_utils.BaseTestCase):

    def setUp(self):
        super(TestZmqVersion, self).setUp()
        self.doer = Doer()

    def test_get_unknown_attr_versions(self):
        self.assertRaises(AssertionError, zmq_version.get_method_versions,
                          self.doer, 'qwerty')

    def test_get_non_method_attr_versions(self):
        for attr_name in vars(self.doer):
            self.assertRaises(AssertionError, zmq_version.get_method_versions,
                              self.doer, attr_name)

    def test_get_private_method_versions(self):
        self.assertRaises(AssertionError, zmq_version.get_method_versions,
                          self.doer, '_sudo')

    def test_get_public_method_versions(self):
        do_versions = zmq_version.get_method_versions(self.doer, 'do')
        self.assertEqual(['1.1', '2.2', '3.3'], sorted(do_versions.keys()))
