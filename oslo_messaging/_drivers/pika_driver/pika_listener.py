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
import time
import uuid

from concurrent import futures
from oslo_log import log as logging

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_poller as pika_drv_poller

LOG = logging.getLogger(__name__)


class RpcReplyPikaListener(object):
    """Provide functionality for listening RPC replies. Create and handle
    reply poller and coroutine for performing polling job
    """

    def __init__(self, pika_engine):
        self._pika_engine = pika_engine

        # preparing poller for listening replies
        self._reply_queue = None

        self._reply_poller = None
        self._reply_waiting_futures = {}

        self._reply_consumer_initialized = False
        self._reply_consumer_initialization_lock = threading.Lock()
        self._poller_thread = None

    def get_reply_qname(self, expiration_time=None):
        """As result return reply queue name, shared for whole process,
        but before this check is RPC listener initialized or not and perform
        initialization if needed

        :param expiration_time: Float, expiration time in seconds
            (like time.time()),
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
                    pika_engine=self._pika_engine,
                    exchange=self._pika_engine.rpc_reply_exchange,
                    queue=self._reply_queue,
                    prefetch_count=(
                        self._pika_engine.rpc_reply_listener_prefetch_count
                    )
                )

                self._reply_poller.start()

            # start reply poller job thread if needed
            if self._poller_thread is None:
                self._poller_thread = threading.Thread(target=self._poller)
                self._poller_thread.daemon = True

            if not self._poller_thread.is_alive():
                self._poller_thread.start()

            self._reply_consumer_initialized = True

        return self._reply_queue

    def _poller(self):
        """Reply polling job. Poll replies in infinite loop and notify
        registered features
        """
        while self._reply_poller:
            try:
                try:
                    messages = self._reply_poller.poll()
                except pika_drv_exc.EstablishConnectionException:
                    LOG.exception("Problem during establishing connection for "
                                  "reply polling")
                    time.sleep(
                        self._pika_engine.host_connection_reconnect_delay
                    )
                    continue

                for message in messages:
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
            except BaseException:
                LOG.exception("Unexpected exception during reply polling")

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
        if self._reply_poller:
            self._reply_poller.stop()
            self._reply_poller.cleanup()
            self._reply_poller = None

        if self._poller_thread:
            if self._poller_thread.is_alive():
                self._poller_thread.join()
            self._poller_thread = None

        self._reply_queue = None
