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

import abc
import logging
import threading

import futurist
import six

from oslo_messaging._drivers.zmq_driver.client import zmq_response
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class ReceiverBase(object):
    """Base response receiving interface."""

    def __init__(self, conf):
        self.conf = conf
        self._lock = threading.Lock()
        self._requests = {}
        self._poller = zmq_async.get_poller()
        self._executor = zmq_async.get_executor(method=self._run_loop)
        self._executor.execute()

    @abc.abstractproperty
    def message_types(self):
        """A set of supported incoming response types."""

    def register_socket(self, socket):
        """Register a socket for receiving data."""
        self._poller.register(socket, recv_method=self.recv_response)

    def unregister_socket(self, socket):
        """Unregister a socket from receiving data."""
        self._poller.unregister(socket)

    @abc.abstractmethod
    def recv_response(self, socket):
        """Receive a response and return a tuple of the form
        (reply_id, message_type, message_id, response).
        """

    def track_request(self, request):
        """Track a request via already registered sockets and return
        a dict of futures for monitoring all types of responses.
        """
        futures = {}
        message_id = request.message_id
        for message_type in self.message_types:
            future = self._get_future(message_id, message_type)
            if future is None:
                future = futurist.Future()
                self._set_future(message_id, message_type, future)
            futures[message_type] = future
        return futures

    def untrack_request(self, request):
        """Untrack a request and stop monitoring any responses."""
        for message_type in self.message_types:
            self._pop_future(request.message_id, message_type)

    def stop(self):
        self._poller.close()
        self._executor.stop()

    def _get_future(self, message_id, message_type):
        with self._lock:
            return self._requests.get((message_id, message_type))

    def _set_future(self, message_id, message_type, future):
        with self._lock:
            self._requests[(message_id, message_type)] = future

    def _pop_future(self, message_id, message_type):
        with self._lock:
            return self._requests.pop((message_id, message_type), None)

    def _run_loop(self):
        data, socket = self._poller.poll(
            timeout=self.conf.oslo_messaging_zmq.rpc_poll_timeout)
        if data is None:
            return
        reply_id, message_type, message_id, response = data
        assert message_type in self.message_types, \
            "%s is not supported!" % zmq_names.message_type_str(message_type)
        future = self._get_future(message_id, message_type)
        if future is not None:
            LOG.debug("Received %(msg_type)s for %(msg_id)s",
                      {"msg_type": zmq_names.message_type_str(message_type),
                       "msg_id": message_id})
            future.set_result((reply_id, response))


class ReplyReceiver(ReceiverBase):

    message_types = {zmq_names.REPLY_TYPE}


class ReplyReceiverProxy(ReplyReceiver):

    def recv_response(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty expected!"
        reply_id = socket.recv()
        assert reply_id is not None, "Reply ID expected!"
        message_type = int(socket.recv())
        assert message_type == zmq_names.REPLY_TYPE, "Reply expected!"
        message_id = socket.recv_string()
        reply_body, failure = socket.recv_loaded()
        reply = zmq_response.Reply(
            message_id=message_id, reply_id=reply_id,
            reply_body=reply_body, failure=failure
        )
        return reply_id, message_type, message_id, reply


class ReplyReceiverDirect(ReplyReceiver):

    def recv_response(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty expected!"
        raw_reply = socket.recv_loaded()
        assert isinstance(raw_reply, dict), "Dict expected!"
        reply = zmq_response.Reply(**raw_reply)
        return reply.reply_id, reply.msg_type, reply.message_id, reply


class AckAndReplyReceiver(ReceiverBase):

    message_types = {zmq_names.ACK_TYPE, zmq_names.REPLY_TYPE}


class AckAndReplyReceiverProxy(AckAndReplyReceiver):

    def recv_response(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty expected!"
        reply_id = socket.recv()
        assert reply_id is not None, "Reply ID expected!"
        message_type = int(socket.recv())
        assert message_type in (zmq_names.ACK_TYPE, zmq_names.REPLY_TYPE), \
            "Ack or reply expected!"
        message_id = socket.recv_string()
        if message_type == zmq_names.REPLY_TYPE:
            reply_body, failure = socket.recv_loaded()
            reply = zmq_response.Reply(
                message_id=message_id, reply_id=reply_id,
                reply_body=reply_body, failure=failure
            )
            response = reply
        else:
            response = None
        return reply_id, message_type, message_id, response


class AckAndReplyReceiverDirect(AckAndReplyReceiver):

    def recv_response(self, socket):
        # acks are not supported yet
        empty = socket.recv()
        assert empty == b'', "Empty expected!"
        raw_reply = socket.recv_loaded()
        assert isinstance(raw_reply, dict), "Dict expected!"
        reply = zmq_response.Reply(**raw_reply)
        return reply.reply_id, reply.msg_type, reply.message_id, reply
