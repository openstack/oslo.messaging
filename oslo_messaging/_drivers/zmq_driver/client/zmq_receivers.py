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
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


def suppress_errors(func):
    @six.wraps(func)
    def silent_func(self, socket):
        try:
            return func(self, socket)
        except Exception as e:
            LOG.error(_LE("Receiving message failed: %r"), e)
            # NOTE(gdavoian): drop the left parts of a broken message, since
            # they most likely will lead to additional exceptions
            if socket.getsockopt(zmq.RCVMORE):
                socket.recv_multipart()
    return silent_func


@six.add_metaclass(abc.ABCMeta)
class ReceiverBase(object):
    """Base response receiving interface."""

    def __init__(self, conf):
        self.conf = conf
        self._lock = threading.Lock()
        self._requests = {}
        self._poller = zmq_async.get_poller()
        self._receive_response_versions = \
            zmq_version.get_method_versions(self, 'receive_response')
        self._executor = zmq_async.get_executor(self._run_loop)
        self._executor.execute()

    def register_socket(self, socket):
        """Register a socket for receiving data."""
        self._poller.register(socket, self.receive_response)

    def unregister_socket(self, socket):
        """Unregister a socket from receiving data."""
        self._poller.unregister(socket)

    @abc.abstractmethod
    def receive_response(self, socket):
        """Receive a response (ack or reply) and return it."""

    def track_request(self, request):
        """Track a request via already registered sockets and return
        a pair of ack and reply futures for monitoring all possible
        types of responses for the given request.
        """
        message_id = request.message_id
        futures = self._get_futures(message_id)
        if futures is None:
            ack_future = reply_future = None
            if self.conf.oslo_messaging_zmq.rpc_use_acks:
                ack_future = futurist.Future()
            if request.msg_type == zmq_names.CALL_TYPE:
                reply_future = futurist.Future()
            futures = (ack_future, reply_future)
            self._set_futures(message_id, futures)
        return futures

    def untrack_request(self, request):
        """Untrack a request and stop monitoring any responses."""
        self._pop_futures(request.message_id)

    def stop(self):
        self._poller.close()
        self._executor.stop()

    def _get_futures(self, message_id):
        with self._lock:
            return self._requests.get(message_id)

    def _set_futures(self, message_id, futures):
        with self._lock:
            self._requests[message_id] = futures

    def _pop_futures(self, message_id):
        with self._lock:
            return self._requests.pop(message_id, None)

    def _run_loop(self):
        response, socket = \
            self._poller.poll(self.conf.oslo_messaging_zmq.rpc_poll_timeout)
        if response is None:
            return
        message_type, message_id = response.msg_type, response.message_id
        futures = self._get_futures(message_id)
        if futures is not None:
            ack_future, reply_future = futures
            if message_type == zmq_names.REPLY_TYPE:
                reply_future.set_result(response)
            else:
                ack_future.set_result(response)
            LOG.debug("Received %(msg_type)s for %(msg_id)s",
                      {"msg_type": zmq_names.message_type_str(message_type),
                       "msg_id": message_id})

    def _get_receive_response_version(self, version):
        receive_response_version = self._receive_response_versions.get(version)
        if receive_response_version is None:
            raise zmq_version.UnsupportedMessageVersionError(version)
        return receive_response_version


class ReceiverProxy(ReceiverBase):

    @suppress_errors
    def receive_response(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty delimiter expected!"
        message_version = socket.recv_string()
        assert message_version != b'', "Valid message version expected!"

        receive_response_version = \
            self._get_receive_response_version(message_version)
        return receive_response_version(socket)

    def _receive_response_v_1_0(self, socket):
        reply_id = socket.recv()
        assert reply_id != b'', "Valid reply id expected!"
        message_type = int(socket.recv())
        assert message_type in zmq_names.RESPONSE_TYPES, "Response expected!"
        message_id = socket.recv_string()
        assert message_id != '', "Valid message id expected!"
        if message_type == zmq_names.REPLY_TYPE:
            reply_body, failure = socket.recv_loaded()
            reply = zmq_response.Reply(message_id=message_id,
                                       reply_id=reply_id,
                                       reply_body=reply_body,
                                       failure=failure)
            return reply
        else:
            ack = zmq_response.Ack(message_id=message_id,
                                   reply_id=reply_id)
            return ack


class ReceiverDirect(ReceiverBase):

    @suppress_errors
    def receive_response(self, socket):
        empty = socket.recv()
        assert empty == b'', "Empty delimiter expected!"
        message_version = socket.recv_string()
        assert message_version != b'', "Valid message version expected!"

        receive_response_version = \
            self._get_receive_response_version(message_version)
        return receive_response_version(socket)

    def _receive_response_v_1_0(self, socket):
        message_type = int(socket.recv())
        assert message_type in zmq_names.RESPONSE_TYPES, "Response expected!"
        message_id = socket.recv_string()
        assert message_id != '', "Valid message id expected!"
        if message_type == zmq_names.REPLY_TYPE:
            reply_body, failure = socket.recv_loaded()
            reply = zmq_response.Reply(message_id=message_id,
                                       reply_body=reply_body,
                                       failure=failure)
            return reply
        else:
            ack = zmq_response.Ack(message_id=message_id)
            return ack
