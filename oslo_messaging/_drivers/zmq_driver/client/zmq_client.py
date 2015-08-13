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

import contextlib

from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver.client.publishers\
    import zmq_req_publisher
from oslo_messaging._drivers.zmq_driver.client import zmq_request
from oslo_messaging._drivers.zmq_driver import zmq_async

zmq = zmq_async.import_zmq()


class ZmqClient(object):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):
        self.conf = conf
        self.context = zmq.Context()
        self.matchmaker = matchmaker
        self.allowed_remote_exmods = allowed_remote_exmods or []
        self.dealer_publisher = zmq_dealer_publisher.DealerPublisher(
            conf, matchmaker)

    def send_call(self, target, context, message, timeout=None, retry=None):
        with contextlib.closing(zmq_request.CallRequest(
                target, context=context, message=message,
                timeout=timeout, retry=retry,
                allowed_remote_exmods=self.allowed_remote_exmods)) as request:
            with contextlib.closing(zmq_req_publisher.ReqPublisher(
                    self.conf, self.matchmaker)) as req_publisher:
                return req_publisher.send_request(request)

    def send_cast(self, target, context, message, timeout=None, retry=None):
        with contextlib.closing(zmq_request.CastRequest(
                target, context=context, message=message,
                timeout=timeout, retry=retry)) as request:
            self.dealer_publisher.send_request(request)

    def send_fanout(self, target, context, message, timeout=None, retry=None):
        with contextlib.closing(zmq_request.FanoutRequest(
                target, context=context, message=message,
                timeout=timeout, retry=retry)) as request:
            self.dealer_publisher.send_request(request)

    def send_notify(self, target, context, message, version, retry=None):
        with contextlib.closing(zmq_request.NotificationRequest(
                target, context, message, version=version,
                retry=retry)) as request:
            self.dealer_publisher.send_request(request)

    def send_notify_fanout(self, target, context, message, version,
                           retry=None):
        with contextlib.closing(zmq_request.NotificationFanoutRequest(
                target, context, message, version=version,
                retry=retry)) as request:
            self.dealer_publisher.send_request(request)

    def cleanup(self):
        self.dealer_publisher.cleanup()
