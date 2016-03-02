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

import pickle
import time

import contextlib

import oslo_messaging
from oslo_messaging._drivers.zmq_driver.client.publishers \
    import zmq_pub_publisher
from oslo_messaging._drivers.zmq_driver.client import zmq_request
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests.drivers.zmq import zmq_common


zmq = zmq_async.import_zmq()


class TestPubSub(zmq_common.ZmqBaseTestCase):

    LISTENERS_COUNT = 3

    def setUp(self):
        super(TestPubSub, self).setUp()

        kwargs = {'use_pub_sub': True}
        self.config(**kwargs)

        self.publisher = zmq_pub_publisher.PubPublisherProxy(
            self.conf, self.driver.matchmaker)
        self.driver.matchmaker.register_publisher(
            (self.publisher.host, ""))

        self.listeners = []
        for i in range(self.LISTENERS_COUNT):
            self.listeners.append(zmq_common.TestServerListener(self.driver))

    def _send_request(self, target):
        #  Needed only in test env to give listener a chance to connect
        #  before request fires
        time.sleep(1)
        with contextlib.closing(zmq_request.FanoutRequest(
                target, context={}, message={'method': 'hello-world'},
                retry=None)) as request:
            self.publisher.send_request([request.create_envelope(),
                                         pickle.dumps(request)])

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
