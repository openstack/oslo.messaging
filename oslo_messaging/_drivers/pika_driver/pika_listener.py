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

from oslo_log import log as logging

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc
from oslo_messaging._drivers.pika_driver import pika_poller as pika_drv_poller

import threading
import time
import uuid


LOG = logging.getLogger(__name__)


class RpcReplyPikaListener(object):
    def __init__(self, pika_engine):
        self._pika_engine = pika_engine

        # preparing poller for listening replies
        self._reply_queue = None

        self._reply_poller = None
        self._reply_waiting_futures = {}

        self._reply_consumer_enabled = False
        self._reply_consumer_thread_run_flag = True
        self._reply_consumer_lock = threading.Lock()
        self._puller_thread = None

    def get_reply_qname(self, expiration_time=None):
        if self._reply_consumer_enabled:
            return self._reply_queue

        with self._reply_consumer_lock:
            if self._reply_consumer_enabled:
                return self._reply_queue

            if self._reply_queue is None:
                self._reply_queue = "reply.{}.{}.{}".format(
                    self._pika_engine.conf.project,
                    self._pika_engine.conf.prog, uuid.uuid4().hex
                )

            if self._reply_poller is None:
                self._reply_poller = pika_drv_poller.RpcReplyPikaPoller(
                    pika_engine=self._pika_engine,
                    exchange=self._pika_engine.rpc_reply_exchange,
                    queue=self._reply_queue,
                    prefetch_count=(
                        self._pika_engine.rpc_reply_listener_prefetch_count
                    )
                )

                self._reply_poller.start(timeout=expiration_time - time.time())

            if self._puller_thread is None:
                self._puller_thread = threading.Thread(target=self._poller)
                self._puller_thread.daemon = True

            if not self._puller_thread.is_alive():
                self._puller_thread.start()

            self._reply_consumer_enabled = True

        return self._reply_queue

    def _poller(self):
        while self._reply_consumer_thread_run_flag:
            try:
                message = self._reply_poller.poll(timeout=1)
                if message is None:
                    continue
                message.acknowledge()
                future = self._reply_waiting_futures.pop(message.msg_id, None)
                if future is not None:
                    future.set_result(message)
            except pika_drv_exc.EstablishConnectionException:
                LOG.exception("Problem during establishing connection for "
                              "reply polling")
                time.sleep(self._pika_engine.host_connection_reconnect_delay)
            except BaseException:
                LOG.exception("Unexpected exception during reply polling")

    def register_reply_waiter(self, msg_id, future):
        self._reply_waiting_futures[msg_id] = future

    def unregister_reply_waiter(self, msg_id):
        self._reply_waiting_futures.pop(msg_id, None)

    def cleanup(self):
        with self._reply_consumer_lock:
            self._reply_consumer_enabled = False

            if self._puller_thread:
                if self._puller_thread.is_alive():
                    self._reply_consumer_thread_run_flag = False
                    self._puller_thread.join()
                self._puller_thread = None

            if self._reply_poller:
                self._reply_poller.stop()
                self._reply_poller.cleanup()
                self._reply_poller = None

                self._reply_queue = None
