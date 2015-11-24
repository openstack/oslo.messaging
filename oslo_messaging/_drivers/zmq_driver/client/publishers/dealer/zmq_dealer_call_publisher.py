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
from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerCallPublisher(zmq_publisher_base.PublisherBase):
    """Thread-safe CALL publisher

        Used as faster and thread-safe publisher for CALL
        instead of ReqPublisher.
    """

    def __init__(self, conf, matchmaker):
        super(DealerCallPublisher, self).__init__(conf)
        self.matchmaker = matchmaker
        self.reply_waiter = ReplyWaiter(conf)
        self.sender = RequestSender(conf, matchmaker, self.reply_waiter) \
            if not conf.direct_over_proxy else \
            RequestSenderLight(conf, matchmaker, self.reply_waiter)

    def send_request(self, request):
        reply_future = self.sender.send_request(request)
        try:
            reply = reply_future.result(timeout=request.timeout)
        except futures.TimeoutError:
            raise oslo_messaging.MessagingTimeout(
                "Timeout %s seconds was reached" % request.timeout)
        finally:
            self.reply_waiter.untrack_id(request.message_id)

        LOG.debug("Received reply %s" % reply)
        if reply[zmq_names.FIELD_FAILURE]:
            raise rpc_common.deserialize_remote_exception(
                reply[zmq_names.FIELD_FAILURE],
                request.allowed_remote_exmods)
        else:
            return reply[zmq_names.FIELD_REPLY]


class RequestSender(zmq_publisher_base.PublisherMultisend):

    def __init__(self, conf, matchmaker, reply_waiter):
        super(RequestSender, self).__init__(conf, matchmaker, zmq.DEALER)
        self.reply_waiter = reply_waiter
        self.queue, self.empty_except = zmq_async.get_queue()
        self.executor = zmq_async.get_executor(self.run_loop)
        self.executor.execute()

    def send_request(self, request):
        reply_future = futurist.Future()
        self.reply_waiter.track_reply(reply_future, request.message_id)
        self.queue.put(request)
        return reply_future

    def _do_send_request(self, socket, request):
        socket.send(b'', zmq.SNDMORE)
        socket.send_pyobj(request)

        LOG.debug("Sending message_id %(message)s to a target %(target)s"
                  % {"message": request.message_id,
                     "target": request.target})

    def _check_hosts_connections(self, target, listener_type):
        if str(target) in self.outbound_sockets:
            socket = self.outbound_sockets[str(target)]
        else:
            hosts = self.matchmaker.get_hosts(
                target, listener_type)
            socket = zmq_socket.ZmqSocket(self.zmq_context, self.socket_type)
            self.outbound_sockets[str(target)] = socket

            for host in hosts:
                self._connect_to_host(socket, host, target)

        return socket

    def run_loop(self):
        try:
            request = self.queue.get(timeout=self.conf.rpc_poll_timeout)
        except self.empty_except:
            return

        socket = self._check_hosts_connections(
            request.target, zmq_names.socket_type_str(zmq.ROUTER))

        self._do_send_request(socket, request)
        self.reply_waiter.poll_socket(socket)


class RequestSenderLight(RequestSender):
    """This class used with proxy.

        Simplified address matching because there is only
        one proxy IPC address.
    """

    def __init__(self, conf, matchmaker, reply_waiter):
        if not conf.direct_over_proxy:
            raise rpc_common.RPCException("RequestSenderLight needs a proxy!")

        super(RequestSenderLight, self).__init__(
            conf, matchmaker, reply_waiter)

        self.socket = None

    def _check_hosts_connections(self, target, listener_type):
        if self.socket is None:
            self.socket = zmq_socket.ZmqSocket(self.zmq_context,
                                               self.socket_type)
            self.outbound_sockets[str(target)] = self.socket
            address = zmq_address.get_broker_address(self.conf)
            self._connect_to_address(self.socket, address, target)
        return self.socket

    def _do_send_request(self, socket, request):
        LOG.debug("Sending %(type)s message_id %(message)s"
                  " to a target %(target)s"
                  % {"type": request.msg_type,
                     "message": request.message_id,
                     "target": request.target})

        envelope = request.create_envelope()

        socket.send(b'', zmq.SNDMORE)
        socket.send_pyobj(envelope, zmq.SNDMORE)
        socket.send_pyobj(request)


class ReplyWaiter(object):

    def __init__(self, conf):
        self.conf = conf
        self.replies = {}
        self.poller = zmq_async.get_poller()
        self.executor = zmq_async.get_executor(self.run_loop)
        self.executor.execute()
        self._lock = threading.Lock()

    def track_reply(self, reply_future, message_id):
        self._lock.acquire()
        self.replies[message_id] = reply_future
        self._lock.release()

    def untrack_id(self, message_id):
        self._lock.acquire()
        self.replies.pop(message_id)
        self._lock.release()

    def poll_socket(self, socket):

        def _receive_method(socket):
            empty = socket.recv()
            assert empty == b'', "Empty expected!"
            reply = socket.recv_pyobj()
            LOG.debug("Received reply %s" % reply)
            return reply

        self.poller.register(socket, recv_method=_receive_method)

    def run_loop(self):
        reply, socket = self.poller.poll(
            timeout=self.conf.rpc_poll_timeout)
        if reply is not None:
            reply_id = reply[zmq_names.FIELD_MSG_ID]
            call_future = self.replies.get(reply_id)
            if call_future:
                call_future.set_result(reply)
            else:
                LOG.warning(_LW("Received timed out reply: %s") % reply_id)
