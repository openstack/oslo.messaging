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

import select
import socket
import sys

from oslo_utils import timeutils
from pika import exceptions as pika_exceptions
import six


PIKA_CONNECTIVITY_ERRORS = (
    pika_exceptions.AMQPConnectionError,
    pika_exceptions.ConnectionClosed,
    pika_exceptions.ChannelClosed,
    socket.timeout,
    select.error
)

EXCEPTIONS_MODULE = 'exceptions' if six.PY2 else 'builtins'

INFINITE_STOP_WATCH = timeutils.StopWatch(duration=None).start()


def is_eventlet_monkey_patched(module):
    """Determines safely is eventlet patching for module enabled or not

    :param module: String, module name
    :return Bool, True if module is patched, False otherwise
    """

    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched(module)
