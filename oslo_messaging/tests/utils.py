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

import threading

from oslo_config import cfg
from oslotest import base


TRUE_VALUES = ('true', '1', 'yes')


class BaseTestCase(base.BaseTestCase):

    def setUp(self, conf=cfg.CONF):
        super(BaseTestCase, self).setUp()

        from oslo_messaging import conffixture
        self.messaging_conf = self.useFixture(conffixture.ConfFixture(conf))
        self.messaging_conf.transport_driver = 'fake'
        self.conf = self.messaging_conf.conf

        self.conf.project = 'project'
        self.conf.prog = 'prog'

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
        for k, v in kw.items():
            self.conf.set_override(k, v, group)


class ServerThreadHelper(threading.Thread):
    def __init__(self, server):
        super(ServerThreadHelper, self).__init__()
        self.daemon = True
        self._server = server
        self._stop_event = threading.Event()
        self._start_event = threading.Event()

    def start(self):
        super(ServerThreadHelper, self).start()
        self._start_event.wait()

    def run(self):
        self._server.start()
        self._start_event.set()
        self._stop_event.wait()
        # Check start() does nothing with a running listener
        self._server.start()
        self._server.stop()
        self._server.wait()

    def stop(self):
        self._stop_event.set()
