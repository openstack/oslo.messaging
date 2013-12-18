# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010-2011 OpenStack Foundation
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2013 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
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

"""Common utilities used in testing"""

import os

import fixtures
from oslo.config import cfg
import six
import testtools

from oslo.messaging.openstack.common.fixture import moxstubout

TRUE_VALUES = ('true', '1', 'yes')


class BaseTestCase(testtools.TestCase):

    def setUp(self, conf=cfg.CONF):
        super(BaseTestCase, self).setUp()
        self.useFixture(fixtures.FakeLogger('oslo.config'))
        test_timeout = os.environ.get('OS_TEST_TIMEOUT', 30)
        try:
            test_timeout = int(test_timeout)
        except ValueError:
            # If timeout value is invalid, fail hard.
            print("OS_TEST_TIMEOUT set to invalid value"
                  " defaulting to no timeout")
            test_timeout = 0
        if test_timeout > 0:
            self.useFixture(fixtures.Timeout(test_timeout, gentle=True))

        if os.environ.get('OS_STDOUT_CAPTURE') in TRUE_VALUES:
            stdout = self.useFixture(fixtures.StringStream('stdout')).stream
            self.useFixture(fixtures.MonkeyPatch('sys.stdout', stdout))
        if os.environ.get('OS_STDERR_CAPTURE') in TRUE_VALUES:
            stderr = self.useFixture(fixtures.StringStream('stderr')).stream
            self.useFixture(fixtures.MonkeyPatch('sys.stderr', stderr))

        self.conf = conf
        self.addCleanup(self.conf.reset)

        from oslo.messaging import conffixture
        self.messaging_conf = self.useFixture(
            conffixture.ConfFixture(self.conf))

        moxfixture = self.useFixture(moxstubout.MoxStubout())
        self.mox = moxfixture.mox
        self.stubs = moxfixture.stubs

    def config(self, **kw):
        """Override some configuration values.

        The keyword arguments are the names of configuration options to
        override and their values.

        If a group argument is supplied, the overrides are applied to
        the specified configuration option group.

        All overrides are automatically cleared at the end of the current
        test by the tearDown() method.
        """
        group = kw.pop('group', None)
        for k, v in six.iteritems(kw):
            self.conf.set_override(k, v, group)
