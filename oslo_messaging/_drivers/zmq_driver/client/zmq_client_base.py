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

from oslo_messaging._drivers.zmq_driver.client import zmq_request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()


class ZmqClientBase(object):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None,
                 publishers=None):
        self.conf = conf
        self.context = zmq.Context()
        self.matchmaker = matchmaker
        self.allowed_remote_exmods = allowed_remote_exmods or []

        self.publishers = publishers
        self.call_publisher = publishers.get(zmq_names.CALL_TYPE) \
            or publishers["default"]
        self.cast_publisher = publishers.get(zmq_names.CAST_TYPE) \
            or publishers["default"]
        self.fanout_publisher = publishers.get(zmq_names.CAST_FANOUT_TYPE) \
            or publishers["default"]
        self.notify_publisher = publishers.get(zmq_names.NOTIFY_TYPE) \
            or publishers["default"]

    def send_call(self, target, context, message, timeout=None, retry=None):
        with contextlib.closing(zmq_request.CallRequest(
                target, context=context, message=message,
                timeout=timeout, retry=retry,
                allowed_remote_exmods=self.allowed_remote_exmods)) as request:
            return self.call_publisher.send_request(request)

    def send_cast(self, target, context, message, retry=None):
        with contextlib.closing(zmq_request.CastRequest(
                target, context=context, message=message,
                retry=retry)) as request:
            self.cast_publisher.send_request(request)

    def send_fanout(self, target, context, message, retry=None):
        with contextlib.closing(zmq_request.FanoutRequest(
                target, context=context, message=message,
                retry=retry)) as request:
            self.fanout_publisher.send_request(request)

    def send_notify(self, target, context, message, version, retry=None):
        with contextlib.closing(zmq_request.NotificationRequest(
                target, context, message, version=version,
                retry=retry)) as request:
            self.notify_publisher.send_request(request)

    def cleanup(self):
        cleaned = set()
        for publisher in self.publishers.values():
            if publisher not in cleaned:
                publisher.cleanup()
                cleaned.add(publisher)
