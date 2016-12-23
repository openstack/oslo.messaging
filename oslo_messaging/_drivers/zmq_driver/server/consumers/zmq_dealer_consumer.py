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
import uuid

import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver.client import zmq_senders
from oslo_messaging._drivers.zmq_driver.client import zmq_sockets_manager
from oslo_messaging._drivers.zmq_driver.server.consumers \
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver.server import zmq_ttl_cache
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging._i18n import _LE, _LI, _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        self.reply_sender = zmq_senders.ReplySenderProxy(conf)
        self.sockets_manager = zmq_sockets_manager.SocketsManager(
            conf, server.matchmaker, zmq.DEALER)
        self.host = None
        super(DealerConsumer, self).__init__(conf, poller, server, zmq.DEALER)
        self._receive_request_versions = \
            zmq_version.get_method_versions(self, 'receive_request')
        self.connection_updater = ConsumerConnectionUpdater(
            conf, self.matchmaker, self.socket)
        LOG.info(_LI("[%s] Run DEALER consumer"), self.host)

    def _generate_identity(self):
        return six.b(self.conf.oslo_messaging_zmq.rpc_zmq_host + "/" +
                     zmq_address.target_to_key(self.target) + "/" +
                     str(uuid.uuid4()))

    def subscribe_socket(self, socket_type):
        try:
            socket = self.sockets_manager.get_socket_to_routers(
                self._generate_identity())
            self.host = socket.handle.identity
            self.poller.register(socket, self.receive_request)
            return socket
        except zmq.ZMQError as e:
            LOG.error(_LE("Failed connecting to ROUTER socket %(e)s") % e)
            raise rpc_common.RPCException(str(e))

    def _reply(self, rpc_message, reply, failure):
        if failure is not None:
            failure = rpc_common.serialize_remote_exception(failure)
        reply = zmq_response.Reply(message_id=rpc_message.message_id,
                                   reply_id=rpc_message.reply_id,
                                   message_version=rpc_message.message_version,
                                   reply_body=reply,
                                   failure=failure)
        self.reply_sender.send(rpc_message.socket, reply)
        return reply

    def _create_message(self, context, message, message_version, reply_id,
                        message_id, socket, message_type):
        if message_type == zmq_names.CALL_TYPE:
            message = zmq_incoming_message.ZmqIncomingMessage(
                context, message, message_version=message_version,
                reply_id=reply_id, message_id=message_id,
                socket=socket, reply_method=self._reply
            )
        else:
            message = zmq_incoming_message.ZmqIncomingMessage(context, message)

        LOG.debug("[%(host)s] Received %(msg_type)s message %(msg_id)s "
                  "(v%(msg_version)s)",
                  {"host": self.host,
                   "msg_type": zmq_names.message_type_str(message_type),
                   "msg_id": message_id,
                   "msg_version": message_version})
        return message

    def _get_receive_request_version(self, version):
        receive_request_version = self._receive_request_versions.get(version)
        if receive_request_version is None:
            raise zmq_version.UnsupportedMessageVersionError(version)
        return receive_request_version

    def receive_request(self, socket):
        try:
            empty = socket.recv()
            assert empty == b'', "Empty delimiter expected!"
            message_version = socket.recv_string()
            assert message_version != b'', "Valid message version expected!"

            receive_request_version = \
                self._get_receive_request_version(message_version)
            return receive_request_version(socket)
        except (zmq.ZMQError, AssertionError, ValueError,
                zmq_version.UnsupportedMessageVersionError) as e:
            LOG.error(_LE("Receiving message failure: %s"), str(e))
            # NOTE(gdavoian): drop the left parts of a broken message
            if socket.getsockopt(zmq.RCVMORE):
                socket.recv_multipart()

    def _receive_request_v_1_0(self, socket):
        reply_id = socket.recv()
        assert reply_id != b'', "Valid reply id expected!"
        message_type = int(socket.recv())
        assert message_type in zmq_names.REQUEST_TYPES, "Request expected!"
        message_id = socket.recv_string()
        assert message_id != '', "Valid message id expected!"
        context, message = socket.recv_loaded()

        return self._create_message(context, message, '1.0', reply_id,
                                    message_id, socket, message_type)

    def cleanup(self):
        LOG.info(_LI("[%s] Destroy DEALER consumer"), self.host)
        self.connection_updater.cleanup()
        super(DealerConsumer, self).cleanup()


class DealerConsumerWithAcks(DealerConsumer):

    def __init__(self, conf, poller, server):
        super(DealerConsumerWithAcks, self).__init__(conf, poller, server)
        self.ack_sender = zmq_senders.AckSenderProxy(conf)
        self.messages_cache = zmq_ttl_cache.TTLCache(
            ttl=conf.oslo_messaging_zmq.rpc_message_ttl
        )

    def _acknowledge(self, message_version, reply_id, message_id, socket):
        ack = zmq_response.Ack(message_id=message_id,
                               reply_id=reply_id,
                               message_version=message_version)
        self.ack_sender.send(socket, ack)

    def _reply(self, rpc_message, reply, failure):
        reply = super(DealerConsumerWithAcks, self)._reply(rpc_message,
                                                           reply, failure)
        self.messages_cache.add(rpc_message.message_id, reply)
        return reply

    def _reply_from_cache(self, message_id, socket):
        reply = self.messages_cache.get(message_id)
        if reply is not None:
            self.reply_sender.send(socket, reply)

    def _create_message(self, context, message, message_version, reply_id,
                        message_id, socket, message_type):
        # drop a duplicate message
        if message_id in self.messages_cache:
            LOG.warning(
                _LW("[%(host)s] Dropping duplicate %(msg_type)s "
                    "message %(msg_id)s"),
                {"host": self.host,
                 "msg_type": zmq_names.message_type_str(message_type),
                 "msg_id": message_id}
            )
            # NOTE(gdavoian): send yet another ack for the direct
            # message, since the old one might be lost;
            # for the CALL message also try to resend its reply
            # (of course, if it was already obtained and cached).
            if message_type in zmq_names.DIRECT_TYPES:
                self._acknowledge(message_version, reply_id, message_id,
                                  socket)
            if message_type == zmq_names.CALL_TYPE:
                self._reply_from_cache(message_id, socket)
            return None

        self.messages_cache.add(message_id)

        # NOTE(gdavoian): send an immediate ack, since it may
        # be too late to wait until the message will be
        # dispatched and processed by a RPC server
        if message_type in zmq_names.DIRECT_TYPES:
            self._acknowledge(message_version, reply_id, message_id, socket)

        return super(DealerConsumerWithAcks, self)._create_message(
            context, message, message_version, reply_id,
            message_id, socket, message_type
        )

    def cleanup(self):
        self.messages_cache.cleanup()
        super(DealerConsumerWithAcks, self).cleanup()


class ConsumerConnectionUpdater(zmq_updater.ConnectionUpdater):

    def _update_connection(self):
        routers = self.matchmaker.get_routers()
        for router_address in routers:
            self.socket.connect_to_host(router_address)
