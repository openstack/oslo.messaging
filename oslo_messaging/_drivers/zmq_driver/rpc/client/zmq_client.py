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

from oslo_messaging._drivers.zmq_driver.rpc.client import zmq_call_request
from oslo_messaging._drivers.zmq_driver.rpc.client import zmq_cast_dealer


class ZmqClient(object):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):
        self.conf = conf
        self.matchmaker = matchmaker
        self.allowed_remote_exmods = allowed_remote_exmods or []
        self.cast_publisher = zmq_cast_dealer.DealerCastPublisher(conf,
                                                                  matchmaker)

    def call(self, target, context, message, timeout=None, retry=None):
        with contextlib.closing(zmq_call_request.CallRequest(
                self.conf, target, context, message, timeout, retry,
                self.allowed_remote_exmods,
                self.matchmaker)) as request:
            return request()

    def cast(self, target, context, message, timeout=None, retry=None):
        self.cast_publisher.cast(target, context, message, timeout, retry)

    def cleanup(self):
        self.cast_publisher.cleanup()
