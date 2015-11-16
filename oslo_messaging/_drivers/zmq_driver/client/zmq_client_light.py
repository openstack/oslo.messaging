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


from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_call_publisher
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver.client import zmq_client_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()


class ZmqClientLight(zmq_client_base.ZmqClientBase):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):
        if not conf.zmq_use_broker:
            raise rpc_common.RPCException(
                "This client needs proxy to be configured!")

        super(ZmqClientLight, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CALL_TYPE:
                    zmq_dealer_call_publisher.DealerCallPublisher(
                        conf, matchmaker),

                "default": zmq_dealer_publisher.DealerPublisherLight(
                        conf, zmq_address.get_broker_address(self.conf))
            }
        )
