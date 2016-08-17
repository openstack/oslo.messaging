#    Copyright 2015 Mirantis, Inc.
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

from oslo_utils import eventletutils
from oslo_utils import importutils


def import_zmq():
    imported_zmq = importutils.try_import(
        'eventlet.green.zmq' if eventletutils.is_monkey_patched('thread') else
        'zmq', default=None
    )
    return imported_zmq


def get_poller():
    if eventletutils.is_monkey_patched('thread'):
        from oslo_messaging._drivers.zmq_driver.poller import green_poller
        return green_poller.GreenPoller()

    from oslo_messaging._drivers.zmq_driver.poller import threading_poller
    return threading_poller.ThreadingPoller()


def get_executor(method):
    if eventletutils.is_monkey_patched('thread'):
        from oslo_messaging._drivers.zmq_driver.poller import green_poller
        return green_poller.GreenExecutor(method)

    from oslo_messaging._drivers.zmq_driver.poller import threading_poller
    return threading_poller.ThreadingExecutor(method)


def get_pool(size):
    import futurist

    if eventletutils.is_monkey_patched('thread'):
        return futurist.GreenThreadPoolExecutor(size)

    return futurist.ThreadPoolExecutor(size)


def get_queue():
    if eventletutils.is_monkey_patched('thread'):
        import eventlet
        return eventlet.queue.Queue(), eventlet.queue.Empty

    import six
    return six.moves.queue.Queue(), six.moves.queue.Empty
