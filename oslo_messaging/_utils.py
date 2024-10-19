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

import logging
import queue
import threading

from oslo_utils import eventletutils
from oslo_utils import importutils

LOG = logging.getLogger(__name__)

eventlet = importutils.try_import('eventlet')
if eventlet and eventletutils.is_monkey_patched("thread"):
    # Here we initialize module with the native python threading module
    # if it was already monkey patched by eventlet/greenlet.
    stdlib_threading = eventlet.patcher.original('threading')
    stdlib_queue = eventlet.patcher.original('queue')
else:
    # Manage the case where we run this driver in a non patched environment
    # and where user even so configure the driver to run heartbeat through
    # a python thread, if we don't do that when the heartbeat will start
    # we will facing an issue by trying to override the threading module.
    stdlib_threading = threading
    stdlib_queue = queue


def version_is_compatible(imp_version, version):
    """Determine whether versions are compatible.

    :param imp_version: The version implemented
    :param version: The version requested by an incoming message.
    """
    if imp_version is None:
        return True

    if version is None:
        return False

    version_parts = version.split('.')
    imp_version_parts = imp_version.split('.')
    try:
        rev = version_parts[2]
    except IndexError:
        rev = 0
    try:
        imp_rev = imp_version_parts[2]
    except IndexError:
        imp_rev = 0

    if int(version_parts[0]) != int(imp_version_parts[0]):  # Major
        return False
    if int(version_parts[1]) > int(imp_version_parts[1]):  # Minor
        return False
    if (int(version_parts[1]) == int(imp_version_parts[1]) and
            int(rev) > int(imp_rev)):  # Revision
        return False
    return True


class DummyLock:
    def acquire(self):
        pass

    def release(self):
        pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


def get_executor_with_context():
    if eventletutils.is_monkey_patched('thread'):
        LOG.debug("Threading is patched, using an eventlet executor")
        return 'eventlet'
    LOG.debug("Using a threading executor")
    return 'threading'
