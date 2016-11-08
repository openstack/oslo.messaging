#    Copyright 2015-2016 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver.proxy import zmq_sender
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_socket


zmq = zmq_async.import_zmq()


class PublisherProxy(object):
    """PUB/SUB based request publisher

        The publisher intended to be used for Fanout and Notify
        multi-sending patterns.

        It differs from direct publishers like DEALER or PUSH based
        in a way it treats matchmaker. Here all publishers register
        in the matchmaker. Subscribers (server-side) take the list
        of publishers and connect to all of them but subscribe
        only to a specific topic-filtering tag generated from the
        Target object.
    """

    def __init__(self, conf, matchmaker, sender=None):
        super(PublisherProxy, self).__init__()
        self.conf = conf
        self.zmq_context = zmq.Context()
        self.matchmaker = matchmaker

        port = conf.zmq_proxy_opts.publisher_port
        self.socket = zmq_socket.ZmqFixedPortSocket(
            self.conf, self.zmq_context, zmq.PUB, conf.zmq_proxy_opts.host,
            port) if port != 0 else \
            zmq_socket.ZmqRandomPortSocket(
                self.conf, self.zmq_context, zmq.PUB, conf.zmq_proxy_opts.host)

        self.host = self.socket.connect_address
        self.sender = sender or zmq_sender.CentralPublisherSender()

    def send_request(self, multipart_message):
        self.sender.send_message(self.socket, multipart_message)

    def cleanup(self):
        self.socket.close()
