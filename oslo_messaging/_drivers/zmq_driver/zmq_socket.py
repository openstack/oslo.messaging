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
import uuid

import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI
from oslo_messaging import exceptions
from oslo_serialization.serializer import json_serializer
from oslo_serialization.serializer import msgpack_serializer

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqSocket(object):

    SERIALIZERS = {
        'json': json_serializer.JSONSerializer(),
        'msgpack': msgpack_serializer.MessagePackSerializer()
    }

    def __init__(self, conf, context, socket_type, immediate=True,
                 high_watermark=0):
        self.conf = conf
        self.context = context
        self.socket_type = socket_type
        self.handle = context.socket(socket_type)
        self.handle.set_hwm(high_watermark)

        self.close_linger = -1
        if self.conf.oslo_messaging_zmq.rpc_cast_timeout > 0:
            self.close_linger = \
                self.conf.oslo_messaging_zmq.rpc_cast_timeout * 1000
        self.handle.setsockopt(zmq.LINGER, self.close_linger)
        # Put messages to only connected queues
        self.handle.setsockopt(zmq.IMMEDIATE, 1 if immediate else 0)
        self.handle.identity = six.b(str(uuid.uuid4()))
        self.connections = set()

    def _get_serializer(self, serialization):
        serializer = self.SERIALIZERS.get(serialization, None)
        if serializer is None:
            raise NotImplementedError(
                "Serialization '{}' is not supported".format(serialization)
            )
        return serializer

    def type_name(self):
        return zmq_names.socket_type_str(self.socket_type)

    def connections_count(self):
        return len(self.connections)

    def connect(self, address):
        if address not in self.connections:
            self.handle.connect(address)
            self.connections.add(address)

    def setsockopt(self, *args, **kwargs):
        self.handle.setsockopt(*args, **kwargs)

    def setsockopt_string(self, *args, **kwargs):
        self.handle.setsockopt_string(*args, **kwargs)

    def send(self, *args, **kwargs):
        self.handle.send(*args, **kwargs)

    def send_string(self, *args, **kwargs):
        self.handle.send_string(*args, **kwargs)

    def send_json(self, *args, **kwargs):
        self.handle.send_json(*args, **kwargs)

    def send_pyobj(self, *args, **kwargs):
        self.handle.send_pyobj(*args, **kwargs)

    def send_multipart(self, *args, **kwargs):
        self.handle.send_multipart(*args, **kwargs)

    def send_dumped(self, obj, *args, **kwargs):
        serialization = kwargs.pop(
            'serialization',
            self.conf.oslo_messaging_zmq.rpc_zmq_serialization)
        serializer = self._get_serializer(serialization)
        s = serializer.dump_as_bytes(obj)
        self.handle.send(s, *args, **kwargs)

    def recv(self, *args, **kwargs):
        return self.handle.recv(*args, **kwargs)

    def recv_string(self, *args, **kwargs):
        return self.handle.recv_string(*args, **kwargs)

    def recv_json(self, *args, **kwargs):
        return self.handle.recv_json(*args, **kwargs)

    def recv_pyobj(self, *args, **kwargs):
        return self.handle.recv_pyobj(*args, **kwargs)

    def recv_multipart(self, *args, **kwargs):
        return self.handle.recv_multipart(*args, **kwargs)

    def recv_loaded(self, *args, **kwargs):
        serialization = kwargs.pop(
            'serialization',
            self.conf.oslo_messaging_zmq.rpc_zmq_serialization)
        serializer = self._get_serializer(serialization)
        s = self.handle.recv(*args, **kwargs)
        obj = serializer.load_from_bytes(s)
        return obj

    def close(self, *args, **kwargs):
        self.handle.close(*args, **kwargs)

    def connect_to_address(self, address):
        if address in self.connections:
            return
        stype = zmq_names.socket_type_str(self.socket_type)
        try:
            LOG.info(_LI("Connecting %(stype)s id %(id)s to %(address)s"),
                     {"stype": stype,
                      "id": self.handle.identity,
                      "address": address})
            self.connect(address)
        except zmq.ZMQError as e:
            errmsg = _LE("Failed connecting %(stype)s to %(address)s: %(e)s") \
                % {"stype": stype, "address": address, "e": e}
            LOG.error(_LE("Failed connecting %(stype)s to %(address)s: %(e)s"),
                      {"stype": stype, "address": address, "e": e})
            raise rpc_common.RPCException(errmsg)

    def connect_to_host(self, host):
        address = zmq_address.get_tcp_direct_address(
            host.decode('utf-8') if six.PY3 and
            isinstance(host, six.binary_type) else host
        )
        self.connect_to_address(address)


class ZmqPortBusy(exceptions.MessagingException):
    """Raised when binding to a port failure"""

    def __init__(self, port_number):
        super(ZmqPortBusy, self).__init__()
        self.port_number = port_number


class ZmqRandomPortSocket(ZmqSocket):

    def __init__(self, conf, context, socket_type, host=None,
                 high_watermark=0):
        super(ZmqRandomPortSocket, self).__init__(
            conf, context, socket_type, immediate=False,
            high_watermark=high_watermark)
        self.bind_address = zmq_address.get_tcp_random_address(self.conf)
        if host is None:
            host = conf.oslo_messaging_zmq.rpc_zmq_host
        try:
            self.port = self.handle.bind_to_random_port(
                self.bind_address,
                min_port=conf.oslo_messaging_zmq.rpc_zmq_min_port,
                max_port=conf.oslo_messaging_zmq.rpc_zmq_max_port,
                max_tries=conf.oslo_messaging_zmq.rpc_zmq_bind_port_retries)
            self.connect_address = zmq_address.combine_address(host, self.port)
        except zmq.ZMQBindError:
            LOG.error(_LE("Random ports range exceeded!"))
            raise ZmqPortBusy(port_number=0)


class ZmqFixedPortSocket(ZmqSocket):

    def __init__(self, conf, context, socket_type, host, port,
                 high_watermark=0):
        super(ZmqFixedPortSocket, self).__init__(
            conf, context, socket_type, immediate=False,
            high_watermark=high_watermark)
        self.connect_address = zmq_address.combine_address(host, port)
        self.bind_address = zmq_address.get_tcp_direct_address(
            zmq_address.combine_address(
                conf.oslo_messaging_zmq.rpc_zmq_bind_address, port))
        self.host = host
        self.port = port

        try:
            self.handle.bind(self.bind_address)
        except zmq.ZMQError as e:
            LOG.exception(e)
            LOG.error(_LE("Chosen port %d is being busy.") % self.port)
            raise ZmqPortBusy(port_number=port)
