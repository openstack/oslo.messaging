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
import threading
import time

from oslo_messaging._drivers import base
from oslo_messaging._drivers.zmq_driver.server.consumers\
    import zmq_consumer_base
from oslo_messaging._drivers.zmq_driver.server import zmq_incoming_message
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class RouterIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, context, message, socket, reply_id, msg_id,
                 poller):
        super(RouterIncomingMessage, self).__init__(listener, context, message)
        self.socket = socket
        self.reply_id = reply_id
        self.msg_id = msg_id
        self.message = message
        poller.resume_polling(socket)

    def reply(self, reply=None, failure=None, log_failure=True):
        """Reply is not needed for non-call messages"""

    def acknowledge(self):
        LOG.debug("Not sending acknowledge for %s", self.msg_id)

    def requeue(self):
        """Requeue is not supported"""


class RouterConsumer(zmq_consumer_base.SingleSocketConsumer):

    def __init__(self, conf, poller, server):
        super(RouterConsumer, self).__init__(conf, poller, server, zmq.ROUTER)
        self.matchmaker = server.matchmaker
        self.host = zmq_address.combine_address(self.conf.rpc_zmq_host,
                                                self.port)
        self.targets = TargetsManager(conf, self.matchmaker, self.host)
        LOG.info(_LI("[%s] Run ROUTER consumer"), self.host)

    def listen(self, target):
        LOG.info(_LI("[%(host)s] Listen to target %(target)s"),
                 {'host': self.host, 'target': target})
        self.targets.listen(target)

    def cleanup(self):
        super(RouterConsumer, self).cleanup()
        self.targets.cleanup()

    def _receive_request(self, socket):
        reply_id = socket.recv()
        empty = socket.recv()
        assert empty == b'', 'Bad format: empty delimiter expected'
        request = socket.recv_pyobj()
        return request, reply_id

    def receive_message(self, socket):
        try:
            request, reply_id = self._receive_request(socket)
            LOG.debug("[%(host)s] Received %(type)s, %(id)s, %(target)s",
                      {"host": self.host,
                       "type": request.msg_type,
                       "id": request.message_id,
                       "target": request.target})

            if request.msg_type == zmq_names.CALL_TYPE:
                return zmq_incoming_message.ZmqIncomingRequest(
                    self.server, socket, reply_id, request, self.poller)
            elif request.msg_type in zmq_names.NON_BLOCKING_TYPES:
                return RouterIncomingMessage(
                    self.server, request.context, request.message, socket,
                    reply_id, request.message_id, self.poller)
            else:
                LOG.error(_LE("Unknown message type: %s"), request.msg_type)

        except zmq.ZMQError as e:
            LOG.error(_LE("Receiving message failed: %s"), str(e))


class TargetsManager(object):

    def __init__(self, conf, matchmaker, host):
        self.targets = []
        self.conf = conf
        self.matchmaker = matchmaker
        self.host = host
        self.targets_lock = threading.Lock()
        self.updater = zmq_async.get_executor(method=self._update_targets) \
            if conf.zmq_target_expire > 0 else None
        if self.updater:
            self.updater.execute()

    def _update_targets(self):
        with self.targets_lock:
            for target in self.targets:
                self.matchmaker.register(
                    target, self.host, zmq_names.socket_type_str(zmq.ROUTER))

        # Update target-records once per half expiration time
        time.sleep(self.conf.zmq_target_expire / 2)

    def listen(self, target):
        with self.targets_lock:
            self.targets.append(target)
            self.matchmaker.register(target, self.host,
                                     zmq_names.socket_type_str(zmq.ROUTER))

    def cleanup(self):
        if self.updater:
            self.updater.stop()
        for target in self.targets:
            self.matchmaker.unregister(target, self.host,
                                       zmq_names.socket_type_str(zmq.ROUTER))
