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

import json
import time

import msgpack
import six
import testscenarios

from oslo_config import cfg

import oslo_messaging
from oslo_messaging._drivers.zmq_driver.proxy.central \
    import zmq_publisher_proxy
from oslo_messaging._drivers.zmq_driver.proxy import zmq_proxy
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging.tests.drivers.zmq import zmq_common

load_tests = testscenarios.load_tests_apply_scenarios

zmq = zmq_async.import_zmq()

opt_group = cfg.OptGroup(name='zmq_proxy_opts',
                         title='ZeroMQ proxy options')
cfg.CONF.register_opts(zmq_proxy.zmq_proxy_opts, group=opt_group)


class TestPubSub(zmq_common.ZmqBaseTestCase):

    LISTENERS_COUNT = 3

    scenarios = [
        ('json', {'serialization': 'json',
                  'dumps': lambda obj: six.b(json.dumps(obj))}),
        ('msgpack', {'serialization': 'msgpack',
                     'dumps': msgpack.dumps})
    ]

    def setUp(self):
        super(TestPubSub, self).setUp()

        kwargs = {'use_pub_sub': True,
                  'rpc_zmq_serialization': self.serialization}
        self.config(group='oslo_messaging_zmq', **kwargs)

        self.config(host="127.0.0.1", group="zmq_proxy_opts")
        self.config(publisher_port=0, group="zmq_proxy_opts")

        self.publisher = zmq_publisher_proxy.PublisherProxy(
            self.conf, self.driver.matchmaker)
        self.driver.matchmaker.register_publisher(
            (self.publisher.host, ''),
            expire=self.conf.oslo_messaging_zmq.zmq_target_expire)

        self.listeners = []
        for _ in range(self.LISTENERS_COUNT):
            self.listeners.append(zmq_common.TestServerListener(self.driver))

    def tearDown(self):
        super(TestPubSub, self).tearDown()
        self.publisher.cleanup()
        for listener in self.listeners:
            listener.stop()

    def _send_request(self, target):
        #  Needed only in test env to give listener a chance to connect
        #  before request fires
        time.sleep(1)
        context = {}
        message = {'method': 'hello-world'}

        self.publisher.send_request(
            [b"reply_id",
             b'',
             six.b(zmq_version.MESSAGE_VERSION),
             six.b(str(zmq_names.CAST_FANOUT_TYPE)),
             zmq_address.target_to_subscribe_filter(target),
             b"message_id",
             self.dumps([context, message])]
        )

    def _check_listener(self, listener):
        listener._received.wait(timeout=5)
        self.assertTrue(listener._received.isSet())
        method = listener.message.message[u'method']
        self.assertEqual(u'hello-world', method)

    def _check_listener_negative(self, listener):
        listener._received.wait(timeout=1)
        self.assertFalse(listener._received.isSet())

    def test_single_listener(self):
        target = oslo_messaging.Target(topic='testtopic', fanout=True)
        self.listener.listen(target)

        self._send_request(target)

        self._check_listener(self.listener)

    def test_all_listeners(self):
        target = oslo_messaging.Target(topic='testtopic', fanout=True)

        for listener in self.listeners:
            listener.listen(target)

        self._send_request(target)

        for listener in self.listeners:
            self._check_listener(listener)

    def test_filtered(self):
        target = oslo_messaging.Target(topic='testtopic', fanout=True)
        target_wrong = oslo_messaging.Target(topic='wrong', fanout=True)

        self.listeners[0].listen(target)
        self.listeners[1].listen(target)
        self.listeners[2].listen(target_wrong)

        self._send_request(target)

        self._check_listener(self.listeners[0])
        self._check_listener(self.listeners[1])
        self._check_listener_negative(self.listeners[2])

    def test_topic_part_matching(self):
        target = oslo_messaging.Target(topic='testtopic', server='server')
        target_part = oslo_messaging.Target(topic='testtopic', fanout=True)

        self.listeners[0].listen(target)
        self.listeners[1].listen(target)

        self._send_request(target_part)

        self._check_listener(self.listeners[0])
        self._check_listener(self.listeners[1])
