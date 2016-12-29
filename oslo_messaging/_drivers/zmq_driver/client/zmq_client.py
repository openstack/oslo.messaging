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

from oslo_messaging._drivers import common
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

        if conf.oslo_messaging_zmq.use_router_proxy or not \
                conf.oslo_messaging_zmq.use_pub_sub:
            raise WrongClientException()

        publisher_direct = self._create_publisher_direct(conf, matchmaker)
        publisher_proxy = self._create_publisher_proxy_dynamic(conf,
                                                               matchmaker)

        super(ZmqClientMixDirectPubSub, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                zmq_names.CAST_FANOUT_TYPE: publisher_proxy,
                zmq_names.NOTIFY_TYPE: publisher_proxy,
                "default": publisher_direct
            }
        )


class ZmqClientDirect(zmq_client_base.ZmqClientBase):
    """This kind of client (publishers combination) is to be used for
    direct connections only:

        use_pub_sub = false
        use_router_proxy = false
    """

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None):

        if conf.oslo_messaging_zmq.use_pub_sub or \
                conf.oslo_messaging_zmq.use_router_proxy:
            raise WrongClientException()

        publisher = self._create_publisher_direct_dynamic(conf, matchmaker) \
            if conf.oslo_messaging_zmq.use_dynamic_connections else \
            self._create_publisher_direct(conf, matchmaker)

        super(ZmqClientDirect, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                "default": publisher
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

        if not conf.oslo_messaging_zmq.use_router_proxy:
            raise WrongClientException()

        super(ZmqClientProxy, self).__init__(
            conf, matchmaker, allowed_remote_exmods,
            publishers={
                "default": self._create_publisher_proxy(conf, matchmaker)
            }
        )
