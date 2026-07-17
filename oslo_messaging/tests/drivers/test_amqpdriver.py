# Copyright 2026 OVHcloud
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

import os
from unittest import mock

import fixtures
from oslo_concurrency import lockutils

from oslo_messaging._drivers import amqpdriver
from oslo_messaging.tests import utils as test_utils


class QManagerTestCase(test_utils.BaseTestCase):
    """Tests for the reply/fanout queue name manager (use_queue_manager).

    Regression coverage for
    https://bugs.launchpad.net/oslo.messaging/+bug/2110957 : the per-process
    identity used to key the shared /dev/shm counter must be evaluated when the
    queue name is built (get()), not cached at __init__ time, otherwise a
    QManager created in the cotyledon master before os.setsid() and a QManager
    created later in a worker disagree on the identity, keep resetting the
    shared counter, and hand out duplicate reply queue names.
    """

    def setUp(self):
        super().setUp()
        # get() takes an external (file) lock via lockutils; give it a path.
        lock_path = self.useFixture(fixtures.TempDir()).path
        lockutils.set_defaults(lock_path=lock_path)

    def _make_qmanager(self):
        tmp = self.useFixture(fixtures.TempDir()).path
        qm = amqpdriver.QManager(hostname='host', processname='conductor')
        # Do not touch the real /dev/shm.
        qm.file_name = os.path.join(tmp, 'host_conductor_qmanager')
        return qm

    def test_get_increments_for_a_stable_identity(self):
        qm = self._make_qmanager()
        with mock.patch.object(qm, '_service_identity',
                               return_value=(7, 111)):
            self.assertEqual('host:conductor:1', qm.get())
            self.assertEqual('host:conductor:2', qm.get())
            self.assertEqual('host:conductor:3', qm.get())

    def test_get_resets_when_identity_changes(self):
        # A different (pg, start_time) means the service was restarted: the
        # counter must start over so queue names get reused.
        qm = self._make_qmanager()
        with mock.patch.object(qm, '_service_identity',
                               return_value=(7, 111)):
            self.assertEqual('host:conductor:1', qm.get())
            self.assertEqual('host:conductor:2', qm.get())
        with mock.patch.object(qm, '_service_identity',
                               return_value=(9, 222)):
            self.assertEqual('host:conductor:1', qm.get())

    def test_get_resets_on_restart_with_constant_pid(self):
        # Containerised service running as a constant pid/pgid across restarts
        # (bug 2078935): the process group is unchanged but the start_time (in
        # jiffies since boot) differs, so the restart must still be detected
        # and the counter reset. This is why start_time is part of the key.
        qm = self._make_qmanager()
        with mock.patch.object(qm, '_service_identity',
                               return_value=(1, 111)):
            self.assertEqual('host:conductor:1', qm.get())
            self.assertEqual('host:conductor:2', qm.get())
        # Same pgid (1), different start_time -> restarted.
        with mock.patch.object(qm, '_service_identity',
                               return_value=(1, 999)):
            self.assertEqual('host:conductor:1', qm.get())

    def test_identity_evaluated_at_get_time_not_at_init(self):
        # The bug: object built in the master (pre-setsid pgid) but used by a
        # worker (post-setsid pgid). With the identity resolved at get() time,
        # what matters is the *calling* process, not the constructing one.
        qm = self._make_qmanager()  # "constructed in the master"

        # Now the worker (pgid 7) uses it: the stored identity must be the
        # worker's, so the reply queue is stable and no longer keyed on the
        # pre-setsid process group of the master.
        with mock.patch.object(qm, '_service_identity',
                               return_value=(7, 111)):
            self.assertEqual('host:conductor:1', qm.get())
            self.assertEqual('host:conductor:2', qm.get())

    def test_no_collision_between_master_born_and_worker_born_managers(self):
        # Two QManagers sharing the same shm file (e.g. the reply-queue
        # manager inherited from the master and a fanout manager created in
        # the worker).
        # Before the fix their cached self.pg differed (1 vs 7) and they reset
        # the counter for one another, both landing on ":1". Now both resolve
        # the identity at get() time -> same worker identity -> the counter
        # keeps growing and names stay unique.
        reply_qm = self._make_qmanager()          # master-born
        fanout_qm = self._make_qmanager()         # worker-born
        fanout_qm.file_name = reply_qm.file_name  # same shared counter

        worker_identity = (7, 111)
        with mock.patch.object(reply_qm, '_service_identity',
                               return_value=worker_identity), \
                mock.patch.object(fanout_qm, '_service_identity',
                                  return_value=worker_identity):
            first = reply_qm.get()
            second = fanout_qm.get()

        self.assertNotEqual(first, second)
        self.assertEqual(['host:conductor:1', 'host:conductor:2'],
                         [first, second])

    def test_service_identity_uses_process_group(self):
        stat = ('7 (nova-conductor) S 1 7 7 0 -1 0 0 0 0 0 0 0 0 0 20 0 2 0 '
                '424242 0 0')
        with mock.patch.object(amqpdriver.os, 'getpgrp', return_value=7), \
                mock.patch('builtins.open', mock.mock_open(read_data=stat)):
            pg, start_time = amqpdriver.QManager._service_identity()
        self.assertEqual(7, pg)
        self.assertEqual(424242, start_time)

    def test_service_identity_falls_back_to_pid_when_pgrp_zero(self):
        # crun edge case (https://github.com/containers/crun/issues/1642):
        # os.getpgrp() returns 0 and /proc/0 does not exist, so the start_time
        # is read from our own pid instead. This is evaluated here at call
        # time, no longer relying on the identity cached at construction. The
        # returned pgid stays 0, which is shared across the exec'd processes.
        stat = ('123 (nova-conductor) S 1 123 123 0 -1 0 0 0 0 0 0 0 0 0 20 0 '
                '2 0 424242 0 0')
        with mock.patch.object(amqpdriver.os, 'getpgrp', return_value=0), \
                mock.patch.object(amqpdriver.os, 'getpid', return_value=123), \
                mock.patch('builtins.open', mock.mock_open(read_data=stat)):
            pg, start_time = amqpdriver.QManager._service_identity()
        self.assertEqual(0, pg)
        self.assertEqual(424242, start_time)
