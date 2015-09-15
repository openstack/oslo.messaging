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

import logging

from oslo_messaging._drivers.zmq_driver.poller import green_poller
from oslo_messaging._drivers.zmq_driver.poller import threading_poller
from oslo_messaging._i18n import _, _LE
from oslo_utils import importutils

LOG = logging.getLogger(__name__)

# Map zmq_concurrency config option names to the actual module name.
ZMQ_MODULES = {
    'native': 'zmq',
    'eventlet': 'eventlet.green.zmq',
}


def import_zmq(zmq_concurrency='eventlet'):
    _raise_error_if_invalid_config_value(zmq_concurrency)

    imported_zmq = importutils.try_import(ZMQ_MODULES[zmq_concurrency],
                                          default='zmq')

    if imported_zmq is None:
        errmsg = _LE("ZeroMQ not found!")
        LOG.error(_LE("ZeroMQ not found!"))
        raise ImportError(errmsg)
    return imported_zmq


def get_poller(zmq_concurrency='eventlet'):
    _raise_error_if_invalid_config_value(zmq_concurrency)

    if zmq_concurrency == 'eventlet' and _is_eventlet_zmq_available():
        return green_poller.GreenPoller()
    return threading_poller.ThreadingPoller()


def get_reply_poller(zmq_concurrency='eventlet'):
    _raise_error_if_invalid_config_value(zmq_concurrency)

    if zmq_concurrency == 'eventlet' and _is_eventlet_zmq_available():
        return green_poller.HoldReplyPoller()
    return threading_poller.ThreadingPoller()


def get_executor(method, zmq_concurrency='eventlet'):
    _raise_error_if_invalid_config_value(zmq_concurrency)

    if zmq_concurrency == 'eventlet' and _is_eventlet_zmq_available():
        return green_poller.GreenExecutor(method)
    return threading_poller.ThreadingExecutor(method)


def _is_eventlet_zmq_available():
    return importutils.try_import('eventlet.green.zmq')


def _raise_error_if_invalid_config_value(zmq_concurrency):
    if zmq_concurrency not in ZMQ_MODULES:
        errmsg = _('Invalid zmq_concurrency value: %s')
        raise ValueError(errmsg % zmq_concurrency)
