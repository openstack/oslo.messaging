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


from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_direct
from oslo_messaging._drivers.zmq_driver.client.publishers.dealer \
    import zmq_dealer_publisher_proxy
from oslo_messaging._drivers.zmq_driver.client import zmq_ack_manager
from oslo_messaging._drivers.zmq_driver.client import zmq_publisher_manager
from oslo_messaging._drivers.zmq_driver.client import zmq_request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names

zmq = zmq_async.import_zmq()


class ZmqClientBase(object):

    def __init__(self, conf, matchmaker=None, allowed_remote_exmods=None,
                 publishers=None):
        self.conf = conf
        self.matchmaker = matchmaker
        self.allowed_remote_exmods = allowed_remote_exmods or []

        self.publishers = publishers
        self.call_publisher = publishers.get(zmq_names.CALL_TYPE,
                                             publishers["default"])
        self.cast_publisher = publishers.get(zmq_names.CAST_TYPE,
                                             publishers["default"])
        self.fanout_publisher = publishers.get(zmq_names.CAST_FANOUT_TYPE,
                                               publishers["default"])
        self.notify_publisher = publishers.get(zmq_names.NOTIFY_TYPE,
                                               publishers["default"])

    def send_call(self, target, context, message, timeout=None, retry=None):
        request = zmq_request.CallRequest(
            target, context=context, message=message, retry=retry,
            timeout=timeout, allowed_remote_exmods=self.allowed_remote_exmods
        )
        return self.call_publisher.send_call(request)

    def send_cast(self, target, context, message, retry=None):
        request = zmq_request.CastRequest(
            target, context=context, message=message, retry=retry
        )
        self.cast_publisher.send_cast(request)

    def send_fanout(self, target, context, message, retry=None):
        request = zmq_request.FanoutRequest(
            target, context=context, message=message, retry=retry
        )
        self.fanout_publisher.send_fanout(request)

    def send_notify(self, target, context, message, version, retry=None):
        request = zmq_request.NotificationRequest(
            target, context=context, message=message, retry=retry,
            version=version
        )
        self.notify_publisher.send_notify(request)

    @staticmethod
    def _create_publisher_direct(conf, matchmaker):
        publisher_cls = zmq_dealer_publisher_direct.DealerPublisherDirectStatic
        publisher_direct = publisher_cls(conf, matchmaker)
        publisher_manager_cls = zmq_publisher_manager.PublisherManagerStatic
        return publisher_manager_cls(publisher_direct)

    @staticmethod
    def _create_publisher_direct_dynamic(conf, matchmaker):
        publisher_cls = zmq_dealer_publisher_direct.DealerPublisherDirect
        publisher_direct = publisher_cls(conf, matchmaker)
        publisher_manager_cls = zmq_publisher_manager.PublisherManagerDynamic \
            if conf.oslo_messaging_zmq.use_pub_sub else \
            zmq_publisher_manager.PublisherManagerDynamicAsyncMultisend
        return publisher_manager_cls(publisher_direct)

    @staticmethod
    def _create_publisher_proxy(conf, matchmaker):
        publisher_proxy = \
            zmq_dealer_publisher_proxy.DealerPublisherProxy(conf, matchmaker)
        if conf.oslo_messaging_zmq.rpc_use_acks:
            ack_manager_cls = zmq_ack_manager.AckManager \
                if conf.oslo_messaging_zmq.use_pub_sub else \
                zmq_ack_manager.AckManagerAsyncMultisend
            return ack_manager_cls(publisher_proxy)
        else:
            publisher_manager_cls = \
                zmq_publisher_manager.PublisherManagerStatic \
                if conf.oslo_messaging_zmq.use_pub_sub else \
                zmq_publisher_manager.PublisherManagerStaticAsyncMultisend
            return publisher_manager_cls(publisher_proxy)

    @staticmethod
    def _create_publisher_proxy_dynamic(conf, matchmaker):
        publisher_proxy = \
            zmq_dealer_publisher_proxy.DealerPublisherProxyDynamic(conf,
                                                                   matchmaker)
        return zmq_publisher_manager.PublisherManagerDynamic(publisher_proxy)

    def cleanup(self):
        cleaned = set()
        for publisher in self.publishers.values():
            if publisher not in cleaned:
                publisher.cleanup()
                cleaned.add(publisher)
