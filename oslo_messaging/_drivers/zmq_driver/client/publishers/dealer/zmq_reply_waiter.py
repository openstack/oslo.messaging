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
import threading

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class ReplyWaiter(object):

    def __init__(self, conf):
        self.conf = conf
        self.replies = {}
        self.poller = zmq_async.get_poller()
        self.executor = zmq_async.get_executor(self.run_loop)
        self.executor.execute()
        self._lock = threading.Lock()

    def track_reply(self, reply_future, message_id):
        with self._lock:
            self.replies[message_id] = reply_future

    def untrack_id(self, message_id):
        with self._lock:
            self.replies.pop(message_id)

    def poll_socket(self, socket):

        def _receive_method(socket):
            empty = socket.recv()
            assert empty == b'', "Empty expected!"
            envelope = socket.recv_pyobj()
            assert envelope is not None, "Invalid envelope!"
            reply = socket.recv_pyobj()
            LOG.debug("Received reply %s", envelope)
            return reply

        self.poller.register(socket, recv_method=_receive_method)

    def run_loop(self):
        reply, socket = self.poller.poll(
            timeout=self.conf.rpc_poll_timeout)
        if reply is not None:
            call_future = self.replies.get(reply.message_id)
            if call_future:
                call_future.set_result(reply)
            else:
                LOG.warning(_LW("Received timed out reply: %s"),
                            reply.message_id)

    def cleanup(self):
        self.poller.close()
