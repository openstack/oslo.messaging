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

import threading
import uuid

from concurrent import futures
from oslo_log import log as logging

from oslo_messaging._drivers.pika_driver import pika_poller as pika_drv_poller

LOG = logging.getLogger(__name__)


class RpcReplyPikaListener(object):
    """Provide functionality for listening RPC replies. Create and handle
    reply poller and coroutine for performing polling job
    """

    def __init__(self, pika_engine):
        super(RpcReplyPikaListener, self).__init__()
        self._pika_engine = pika_engine

        # preparing poller for listening replies
        self._reply_queue = None

        self._reply_poller = None
        self._reply_waiting_futures = {}

        self._reply_consumer_initialized = False
        self._reply_consumer_initialization_lock = threading.Lock()
        self._shutdown = False

    def get_reply_qname(self):
        """As result return reply queue name, shared for whole process,
        but before this check is RPC listener initialized or not and perform
        initialization if needed

        :return: String, queue name which hould be used for reply sending
        """
        if self._reply_consumer_initialized:
            return self._reply_queue

        with self._reply_consumer_initialization_lock:
            if self._reply_consumer_initialized:
                return self._reply_queue

            # generate reply queue name if needed
            if self._reply_queue is None:
                self._reply_queue = "reply.{}.{}.{}".format(
                    self._pika_engine.conf.project,
                    self._pika_engine.conf.prog, uuid.uuid4().hex
                )

            # initialize reply poller if needed
            if self._reply_poller is None:
                self._reply_poller = pika_drv_poller.RpcReplyPikaPoller(
                    self._pika_engine, self._pika_engine.rpc_reply_exchange,
                    self._reply_queue, 1, None,
                    self._pika_engine.rpc_reply_listener_prefetch_count
                )

            self._reply_poller.start(self._on_incoming)
            self._reply_consumer_initialized = True

        return self._reply_queue

    def _on_incoming(self, incoming):
        """Reply polling job. Poll replies in infinite loop and notify
        registered features
        """
        for message in incoming:
            try:
                message.acknowledge()
                future = self._reply_waiting_futures.pop(
                    message.msg_id, None
                )
                if future is not None:
                    future.set_result(message)
            except Exception:
                LOG.exception("Unexpected exception during processing"
                              "reply message")

    def register_reply_waiter(self, msg_id):
        """Register reply waiter. Should be called before message sending to
        the server
        :param msg_id: String, message_id of expected reply
        :return future: Future, container for expected reply to be returned
            over
        """
        future = futures.Future()
        self._reply_waiting_futures[msg_id] = future
        return future

    def unregister_reply_waiter(self, msg_id):
        """Unregister reply waiter. Should be called if client has not got
        reply and doesn't want to continue waiting (if timeout_expired for
        example)
        :param msg_id:
        """
        self._reply_waiting_futures.pop(msg_id, None)

    def cleanup(self):
        """Stop replies consuming and cleanup resources"""
        self._shutdown = True

        if self._reply_poller:
            self._reply_poller.stop()
            self._reply_poller.cleanup()
            self._reply_poller = None

        self._reply_queue = None
