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

from concurrent import futures
import logging

from oslo_messaging._drivers.zmq_driver.client import zmq_publisher_manager
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class AckManager(zmq_publisher_manager.PublisherManagerBase):

    def __init__(self, publisher):
        super(AckManager, self).__init__(publisher)
        self._pool = zmq_async.get_pool(
            size=self.conf.oslo_messaging_zmq.rpc_thread_pool_size
        )

    def _wait_for_ack(self, ack_future):
        request = ack_future.request
        retries = \
            request.retry or self.conf.oslo_messaging_zmq.rpc_retry_attempts
        if retries is None:
            retries = -1
        timeout = self.conf.oslo_messaging_zmq.rpc_ack_timeout_base

        done = False
        while not done:
            try:
                reply_id, response = ack_future.result(timeout=timeout)
                done = True
                assert response is None, "Ack expected!"
                if reply_id is not None:
                    assert reply_id == request.routing_key, \
                        "Ack from recipient expected!"
            except AssertionError:
                LOG.error(_LE("Message format error in ack for %s"),
                          request.message_id)
            except futures.TimeoutError:
                LOG.warning(_LW("No ack received within %(tout)s seconds "
                                "for %(msg_id)s"),
                            {"tout": timeout,
                             "msg_id": request.message_id})
                if retries != 0:
                    if retries > 0:
                        retries -= 1
                    self.sender.send(ack_future.socket, request)
                    timeout *= \
                        self.conf.oslo_messaging_zmq.rpc_ack_timeout_multiplier
                else:
                    LOG.warning(_LW("Exhausted number of retries for %s"),
                                request.message_id)
                    done = True

        if request.msg_type != zmq_names.CALL_TYPE:
            self.receiver.untrack_request(request)

    def _schedule_request_for_ack(self, request):
        socket = self.publisher.acquire_connection(request)
        self.publisher.send_request(socket, request)
        self.receiver.register_socket(socket)
        futures_by_type = self.receiver.track_request(request)
        ack_future = futures_by_type[zmq_names.ACK_TYPE]
        ack_future.request = request
        ack_future.socket = socket
        return ack_future

    @zmq_publisher_manager.target_not_found_timeout
    def send_call(self, request):
        try:
            ack_future = self._schedule_request_for_ack(request)
            self._pool.submit(self._wait_for_ack, ack_future)
            return self.publisher.receive_reply(ack_future.socket, request)
        finally:
            if not ack_future.done():
                ack_future.set_result((None, None))

    @zmq_publisher_manager.target_not_found_warn
    def send_cast(self, request):
        ack_future = self._schedule_request_for_ack(request)
        self._pool.submit(self._wait_for_ack, ack_future)

    @zmq_publisher_manager.target_not_found_warn
    def _send_request(self, request):
        socket = self.publisher.acquire_connection(request)
        self.publisher.send_request(socket, request)

    def cleanup(self):
        self._pool.shutdown(wait=True)
        super(AckManager, self).cleanup()

    send_fanout = _send_request
    send_notify = _send_request
