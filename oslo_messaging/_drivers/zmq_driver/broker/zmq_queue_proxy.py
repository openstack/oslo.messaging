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

import six

from oslo_messaging._drivers.zmq_driver.broker import zmq_base_proxy
from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LI

zmq = zmq_async.import_zmq(zmq_concurrency='native')
LOG = logging.getLogger(__name__)


class OutgoingQueueProxy(zmq_base_proxy.BaseProxy):

    def __init__(self, conf, context, queue, matchmaker):
        super(OutgoingQueueProxy, self).__init__(conf, context)
        self.queue = queue
        self.matchmaker = matchmaker
        self.publisher = zmq_dealer_publisher.DealerPublisher(
            conf, matchmaker)
        LOG.info(_LI("Polling at outgoing proxy ..."))

    def run(self):
        try:
            request = self.queue.get(timeout=self.conf.rpc_poll_timeout)
            LOG.info(_LI("Redirecting request %s to TCP publisher ...")
                     % request)
            self.publisher.send_request(request)
        except six.moves.queue.Empty:
            return


class IncomingQueueProxy(zmq_base_proxy.BaseProxy):

    def __init__(self, conf, context, queue):
        super(IncomingQueueProxy, self).__init__(conf, context)
        self.poller = zmq_async.get_poller(
            zmq_concurrency='native')

        self.queue = queue

        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind(zmq_address.get_broker_address(conf))
        self.poller.register(self.socket, self.receive_request)
        LOG.info(_LI("Polling at incoming proxy ..."))

    def run(self):
        request, socket = self.poller.poll(self.conf.rpc_poll_timeout)
        if request is None:
            return

        LOG.info(_LI("Received request and queue it: %s") % str(request))

        self.queue.put(request)

    def receive_request(self, socket):
        reply_id = socket.recv()
        assert reply_id is not None, "Valid id expected"
        empty = socket.recv()
        assert empty == b'', "Empty delimiter expected"
        return socket.recv_pyobj()
