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

from concurrent import futures
import futurist

import oslo_messaging
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerCallPublisher(object):
    """Thread-safe CALL publisher

        Used as faster and thread-safe publisher for CALL
        instead of ReqPublisher.
    """

    def __init__(self, conf, matchmaker):
        super(DealerCallPublisher, self).__init__()
        self.conf = conf
        self.matchmaker = matchmaker
        self.reply_waiter = ReplyWaiter(conf)
        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)

        def _do_send_request(socket, request):
            target_hosts = self.sockets_manager.get_hosts(request.target)
            envelope = request.create_envelope(target_hosts)
            # DEALER socket specific envelope empty delimiter
            socket.send(b'', zmq.SNDMORE)
            socket.send_pyobj(envelope, zmq.SNDMORE)
            socket.send_pyobj(request)

            LOG.debug("Sent message_id %(message)s to a target %(target)s",
                      {"message": request.message_id,
                       "target": request.target})

        self.sender = CallSender(self.sockets_manager, _do_send_request,
                                 self.reply_waiter) \
            if not conf.use_router_proxy else \
            CallSenderLight(self.sockets_manager, _do_send_request,
                            self.reply_waiter)

    def send_request(self, request):
        reply_future = self.sender.send_request(request)
        try:
            reply = reply_future.result(timeout=request.timeout)
        except futures.TimeoutError:
            raise oslo_messaging.MessagingTimeout(
                "Timeout %s seconds was reached" % request.timeout)
        finally:
            self.reply_waiter.untrack_id(request.message_id)

        LOG.debug("Received reply %s", reply)
        if reply.failure:
            raise rpc_common.deserialize_remote_exception(
                reply.failure,
                request.allowed_remote_exmods)
        else:
            return reply.reply_body

    def cleanup(self):
        self.reply_waiter.cleanup()
        self.sender.cleanup()


class CallSender(zmq_publisher_base.QueuedSender):

    def __init__(self, sockets_manager, _do_send_request, reply_waiter):
        super(CallSender, self).__init__(sockets_manager, _do_send_request)
        assert reply_waiter, "Valid ReplyWaiter expected!"
        self.reply_waiter = reply_waiter

    def send_request(self, request):
        reply_future = futurist.Future()
        self.reply_waiter.track_reply(reply_future, request.message_id)
        self.queue.put(request)
        return reply_future

    def _connect_socket(self, target):
        socket = self.outbound_sockets.get_socket(target)
        self.reply_waiter.poll_socket(socket)
        return socket


class CallSenderLight(CallSender):

    def __init__(self, sockets_manager, _do_send_request, reply_waiter):
        super(CallSenderLight, self).__init__(
            sockets_manager, _do_send_request, reply_waiter)
        self.socket = self.outbound_sockets.get_socket_to_routers()
        self.reply_waiter.poll_socket(self.socket)

    def _connect_socket(self, target):
        return self.socket


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
            LOG.debug("Received reply %s", reply)
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
