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

from oslo_serialization.serializer import json_serializer
from oslo_serialization.serializer import msgpack_serializer
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

MESSAGE_SERIALIZERS = {
    'application/json': json_serializer.JSONSerializer(),
    'application/msgpack': msgpack_serializer.MessagePackSerializer()
}
