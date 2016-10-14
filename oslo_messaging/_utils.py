
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

import threading

from oslo_utils import eventletutils
from oslo_utils import importutils

_eventlet = importutils.try_import('eventlet')


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


def fetch_current_thread_functor():
    # Until https://github.com/eventlet/eventlet/issues/172 is resolved
    # or addressed we have to use complicated workaround to get a object
    # that will not be recycled; the usage of threading.current_thread()
    # doesn't appear to currently be monkey patched and therefore isn't
    # reliable to use (and breaks badly when used as all threads share
    # the same current_thread() object)...
    try:
        import eventlet
        from eventlet import patcher
        green_threaded = patcher.is_monkey_patched('thread')
    except ImportError:
        green_threaded = False
    if green_threaded:
        return lambda: eventlet.getcurrent()
    else:
        return lambda: threading.current_thread()


class DummyLock(object):
    def acquire(self):
        pass

    def release(self):
        pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


class _Event(object):
    """A class that provides consistent eventlet/threading Event API.

    This wraps the eventlet.event.Event class to have the same API as
    the standard threading.Event object.
    """
    def __init__(self, *args, **kwargs):
        self.clear()

    def clear(self):
        self._set = False
        self._event = _eventlet.event.Event()

    def is_set(self):
        return self._set

    isSet = is_set

    def set(self):
        self._set = True
        self._event.send(True)

    def wait(self, timeout=None):
        with _eventlet.timeout.Timeout(timeout, False):
            self._event.wait()
        return self.is_set()


def Event():
    if eventletutils.is_monkey_patched("thread"):
        return _Event()
    else:
        return threading.Event()
