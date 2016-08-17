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

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE, _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class AckManagerBase(object):

    def __init__(self, publisher):
        self.publisher = publisher
        self.conf = publisher.conf
        self.sender = publisher.sender
        self.receiver = publisher.receiver

    def send_call(self, request):
        return self.publisher.send_call(request)

    def send_cast(self, request):
        self.publisher.send_cast(request)

    def send_fanout(self, request):
        self.publisher.send_fanout(request)

    def send_notify(self, request):
        self.publisher.send_notify(request)

    def cleanup(self):
        self.publisher.cleanup()


class AckManagerDirect(AckManagerBase):
    pass


class AckManagerProxy(AckManagerBase):

    def __init__(self, publisher):
        super(AckManagerProxy, self).__init__(publisher)
        self._pool = zmq_async.get_pool(
            size=self.conf.oslo_messaging_zmq.rpc_thread_pool_size
        )

    def _wait_for_ack(self, ack_future):
        request, socket = ack_future.args
        retries = \
            request.retry or self.conf.oslo_messaging_zmq.rpc_retry_attempts
        timeout = self.conf.oslo_messaging_zmq.rpc_ack_timeout_base

        done = False
        while not done:
            try:
                reply_id, response = ack_future.result(timeout=timeout)
                done = True
                assert response is None, "Ack expected!"
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
                if retries is None or retries != 0:
                    if retries is not None and retries > 0:
                        retries -= 1
                    self.sender.send(socket, request)
                    timeout *= \
                        self.conf.oslo_messaging_zmq.rpc_ack_timeout_multiplier
                else:
                    LOG.warning(_LW("Exhausted number of retries for %s"),
                                request.message_id)
                    done = True

        self.receiver.untrack_request(request)

    def _get_ack_future(self, request):
        socket = self.publisher.connect_socket(request)
        self.receiver.register_socket(socket)
        ack_future = self.receiver.track_request(request)[zmq_names.ACK_TYPE]
        ack_future.args = request, socket
        return ack_future

    def send_cast(self, request):
        self.publisher.send_cast(request)
        self._pool.submit(self._wait_for_ack, self._get_ack_future(request))

    def cleanup(self):
        self._pool.shutdown(wait=True)
        super(AckManagerProxy, self).cleanup()
