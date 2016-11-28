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
from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class AckManager(zmq_publisher_manager.PublisherManagerBase):

    def __init__(self, publisher):
        super(AckManager, self).__init__(publisher, with_pool=True)

    @staticmethod
    def _check_ack(ack, request):
        if ack is not None:
            assert isinstance(ack, zmq_response.Ack), "Ack expected!"
            assert ack.reply_id == request.routing_key, \
                "Ack from recipient expected!"

    def _wait_for_ack(self, request, ack_future=None):
        if ack_future is None:
            ack_future = self._schedule_request_for_ack(request)

        retries = \
            request.retry or self.conf.oslo_messaging_zmq.rpc_retry_attempts
        if retries is None:
            retries = -1
        timeout = self.conf.oslo_messaging_zmq.rpc_ack_timeout_base

        done = ack_future is None
        while not done:
            try:
                ack = ack_future.result(timeout=timeout)
                done = True
                self._check_ack(ack, request)
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

    @zmq_publisher_manager.target_not_found_warn
    def _send_request(self, request):
        socket = self.publisher.acquire_connection(request)
        self.publisher.send_request(socket, request)
        return socket

    def _schedule_request_for_ack(self, request):
        socket = self._send_request(request)
        if socket is None:
            return None
        self.receiver.register_socket(socket)
        ack_future, _ = self.receiver.track_request(request)
        ack_future.socket = socket
        return ack_future

    def send_call(self, request):
        ack_future = self._schedule_request_for_ack(request)
        if ack_future is None:
            self.publisher._raise_timeout(request)
        self.pool.submit(self._wait_for_ack, request, ack_future)
        try:
            return self.publisher.receive_reply(ack_future.socket, request)
        finally:
            if not ack_future.done():
                ack_future.set_result(None)

    def send_cast(self, request):
        self.pool.submit(self._wait_for_ack, request)

    send_fanout = _send_request
    send_notify = _send_request


class AckManagerAsyncMultisend(AckManager):

    def _send_request_async(self, request):
        self.pool.submit(self._send_request, request)

    send_fanout = _send_request_async
    send_notify = _send_request_async
