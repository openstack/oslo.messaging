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


from oslo_messaging._drivers import common
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_call_publisher
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_proxy
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_publisher_base
from oslo_messaging._drivers.zmq_driver.client import zmq_client_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()


class WrongClientException(common.RPCException):
    """Raised if client type doesn't match configuration"""


class ZmqClientMixDirectPubSub(zmq_client_base.ZmqClientBase):
    """Client for using with direct connections and fanout over proxy:

        use_pub_sub = true
        use_router_proxy = false

    """

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):

        if conf.use_router_proxy or not conf.use_pub_sub:
            raise WrongClientException()

        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)

        fanout_publisher = zmq_dealer_publisher_proxy.DealerPublisherProxy(
            conf, matchmaker, self.sockets_manager.get_socket_to_publishers())

        super(ZmqClientMixDirectPubSub, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CALL_TYPE:
                    zmq_dealer_call_publisher.DealerCallPublisher(
                        conf, matchmaker, self.sockets_manager),

                zmq_names.CAST_FANOUT_TYPE: fanout_publisher,

                zmq_names.NOTIFY_TYPE: fanout_publisher,

                "default": zmq_dealer_publisher.DealerPublisherAsync(
                    conf, matchmaker)
            }
        )


class ZmqClientDirect(zmq_client_base.ZmqClientBase):
    """This kind of client (publishers combination) is to be used for
    direct connections only:

        use_pub_sub = false
        use_router_proxy = false
    """

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):

        if conf.use_pub_sub or conf.use_router_proxy:
            raise WrongClientException()

        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)

        super(ZmqClientDirect, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CALL_TYPE:
                    zmq_dealer_call_publisher.DealerCallPublisher(
                        conf, matchmaker, self.sockets_manager),

                "default": zmq_dealer_publisher.DealerPublisher(
                    conf, matchmaker)
            }
        )


class ZmqClientProxy(zmq_client_base.ZmqClientBase):
    """Client for using with proxy:

        use_pub_sub = true
        use_router_proxy = true
    or
        use_pub_sub = false
        use_router_proxy = true
    """

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):

        if not conf.use_router_proxy:
            raise WrongClientException()

        self.sockets_manager = zmq_publisher_base.SocketsManager(
            conf, matchmaker, zmq.ROUTER, zmq.DEALER)

        super(ZmqClientProxy, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CALL_TYPE:
                    zmq_dealer_publisher_proxy.DealerCallPublisherProxy(
                        conf, matchmaker, self.sockets_manager),

                "default": zmq_dealer_publisher_proxy.DealerPublisherProxy(
                        conf, matchmaker,
                        self.sockets_manager.get_socket_to_publishers())
            }
        )
