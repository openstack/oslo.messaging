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


from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_call_publisher
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver.client import zmq_client_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()


class ZmqClient(zmq_client_base.ZmqClientBase):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):

        default_publisher = zmq_dealer_publisher.DealerPublisher(
            conf, matchmaker)

        cast_publisher = zmq_dealer_publisher.DealerPublisherAsync(
            conf, matchmaker) \
            if zmq_async.is_eventlet_concurrency(conf) \
            else default_publisher

        fanout_publisher = zmq_dealer_publisher.DealerPublisherLight(
            conf, matchmaker) if conf.use_pub_sub else default_publisher

        super(ZmqClient, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CALL_TYPE:
                    zmq_dealer_call_publisher.DealerCallPublisher(
                        conf, matchmaker),

                zmq_names.CAST_TYPE: cast_publisher,

                # Here use DealerPublisherLight for sending request to proxy
                # which finally uses PubPublisher to send fanout in case of
                # 'use_pub_sub' option configured.
                zmq_names.CAST_FANOUT_TYPE: fanout_publisher,

                zmq_names.NOTIFY_TYPE: fanout_publisher,

                "default": default_publisher
            }
        )
