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

import logging
import os
import sys

from oslo_messaging.tests.functional.zmq import multiproc_utils


LOG = logging.getLogger(__name__)


class StartupOrderTestCase(multiproc_utils.MutliprocTestCase):

    def setUp(self):
        super(StartupOrderTestCase, self).setUp()

        self.conf.prog = "test_prog"
        self.conf.project = "test_project"

        kwargs = {'rpc_response_timeout': 30,
                  'use_pub_sub': False}
        self.config(**kwargs)

        log_path = self.conf.rpc_zmq_ipc_dir + "/" + str(os.getpid()) + ".log"
        sys.stdout = open(log_path, "w", buffering=0)

    def test_call_server_before_client(self):
        self.spawn_servers(3, wait_for_server=True, random_topic=False)
        servers = self.spawned
        client = self.get_client(servers[0].topic)
        for i in range(3):
            reply = client.call_a()
            self.assertIsNotNone(reply)
        self.assertEqual(3, len(client.replies))

    def test_call_client_dont_wait_for_server(self):
        self.spawn_servers(3, wait_for_server=False, random_topic=False)
        servers = self.spawned
        client = self.get_client(servers[0].topic)
        for i in range(3):
            reply = client.call_a()
            self.assertIsNotNone(reply)
        self.assertEqual(3, len(client.replies))
