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

from concurrent import futures
import futurist

import oslo_messaging
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_reply_waiter
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class DealerCallPublisher(object):
    """Thread-safe CALL publisher

        Used as faster and thread-safe publisher for CALL
        instead of ReqPublisher.
    """

    def __init__(self, conf, matchmaker, sockets_manager, sender=None,
                 reply_waiter=None):
        super(DealerCallPublisher, self).__init__()
        self.conf = conf
        self.matchmaker = matchmaker
        self.reply_waiter = reply_waiter or zmq_reply_waiter.ReplyWaiter(conf)
        self.sockets_manager = sockets_manager
        self.sender = sender or CallSender(self.sockets_manager,
                                           self.reply_waiter)

    def send_request(self, request):
        reply_future = self.sender.send_request(request)
        try:
            reply = reply_future.result(timeout=request.timeout)
            LOG.debug("Received reply %s", request.message_id)
        except AssertionError:
            LOG.error(_LE("Message format error in reply %s"),
                      request.message_id)
            return None
        except futures.TimeoutError:
            raise oslo_messaging.MessagingTimeout(
                "Timeout %(tout)s seconds was reached for message %(id)s" %
                {"tout": request.timeout,
                 "id": request.message_id})
        finally:
            self.reply_waiter.untrack_id(request.message_id)

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

    def __init__(self, sockets_manager, reply_waiter):
        super(CallSender, self).__init__(sockets_manager,
                                         self._do_send_request)
        assert reply_waiter, "Valid ReplyWaiter expected!"
        self.reply_waiter = reply_waiter

    def _do_send_request(self, socket, request):
        envelope = request.create_envelope()
        # DEALER socket specific envelope empty delimiter
        socket.send(b'', zmq.SNDMORE)
        socket.send_pyobj(envelope, zmq.SNDMORE)
        socket.send_pyobj(request)

        LOG.debug("Sent message_id %(message)s to a target %(target)s",
                  {"message": request.message_id,
                   "target": request.target})

    def send_request(self, request):
        reply_future = futurist.Future()
        self.reply_waiter.track_reply(reply_future, request.message_id)
        self.queue.put(request)
        return reply_future

    def _connect_socket(self, target):
        socket = self.outbound_sockets.get_socket(target)
        self.reply_waiter.poll_socket(socket)
        return socket
