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

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.rpc.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ZmqServer(base.Listener):

    def __init__(self, conf, matchmaker=None):
        self.conf = conf
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.ROUTER)
            self.address = zmq_target.get_tcp_random_address(conf)
            self.port = self.socket.bind_to_random_port(self.address)
            LOG.info("Run server on tcp://%s:%d" %
                     (self.address, self.port))
        except zmq.ZMQError as e:
            errmsg = _LE("Failed binding to port %(port)d: %(e)s")\
                % (self.port, e)
            LOG.error(errmsg)
            raise rpc_common.RPCException(errmsg)

        self.poller = zmq_async.get_poller()
        self.poller.register(self.socket, self._receive_message)
        self.matchmaker = matchmaker

    def poll(self, timeout=None):
        incoming = self.poller.poll(timeout or self.conf.rpc_poll_timeout)
        return incoming[0]

    def stop(self):
        LOG.info("Stop server tcp://%s:%d" % (self.address, self.port))

    def cleanup(self):
        self.poller.close()
        if not self.socket.closed:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()

    def listen(self, target):
        LOG.info("Listen to Target %s on tcp://%s:%d" %
                 (target, self.address, self.port))
        host = zmq_target.combine_address(self.conf.rpc_zmq_host, self.port)
        self.matchmaker.register(target=target,
                                 hostname=host)

    def _receive_message(self, socket):
        try:
            reply_id = socket.recv()
            empty = socket.recv()
            assert empty == b'', 'Bad format: empty delimiter expected'
            msg_type = socket.recv_string()
            assert msg_type is not None, 'Bad format: msg type expected'
            target_dict = socket.recv_json()
            assert target_dict is not None, 'Bad format: target expected'
            context = socket.recv_json()
            message = socket.recv_json()
            LOG.debug("Received CALL message %s" % str(message))

            direct_type = (zmq_serializer.CALL_TYPE, zmq_serializer.CAST_TYPE)
            if msg_type in direct_type:
                return zmq_incoming_message.ZmqIncomingRequest(
                    self, context, message, socket, reply_id, self.poller)
            elif msg_type == zmq_serializer.FANOUT_TYPE:
                return zmq_incoming_message.ZmqFanoutMessage(
                    self, context, message, socket, self.poller)

        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed: %s") % str(e))
