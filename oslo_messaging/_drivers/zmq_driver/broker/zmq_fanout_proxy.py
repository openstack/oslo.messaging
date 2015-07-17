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


import oslo_messaging._drivers.zmq_driver.broker.zmq_base_proxy as base_proxy
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging._drivers.zmq_driver import zmq_target

zmq = zmq_async.import_zmq()


class PublisherBackend(base_proxy.BaseBackendMatcher):

    def __init__(self, conf, context):
        poller = zmq_async.get_poller(native_zmq=conf.rpc_zmq_native)
        super(PublisherBackend, self).__init__(conf, poller, context)
        self.backend = self.context.socket(zmq.PUB)
        self.backend.bind(zmq_target.get_ipc_address_fanout(conf))

    def redirect_to_backend(self, message):
        target_pos = zmq_serializer.MESSAGE_CALL_TARGET_POSITION + 1
        msg = message[target_pos:]
        self.backend.send_multipart(msg)
