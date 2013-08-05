# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Red Hat, Inc.
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

from oslo import messaging
from oslo.messaging._drivers import impl_rabbit as rabbit_driver
from oslo.messaging import transport as msg_transport
from tests import utils as test_utils


class TestRabbitDriver(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRabbitDriver, self).setUp()
        self.conf.register_opts(msg_transport._transport_opts)
        self.conf.register_opts(rabbit_driver.rabbit_opts)
        self.config(rpc_backend='rabbit')
        self.config(fake_rabbit=True)

    def test_driver_load(self):
        transport = messaging.get_transport(self.conf)
        self.assertTrue(isinstance(transport._driver,
                                   rabbit_driver.RabbitDriver))

    def test_send_receive(self):
        transport = messaging.get_transport(self.conf)
        self.addCleanup(transport.cleanup)

        driver = transport._driver

        target = messaging.Target(topic='testtopic')

        listener = driver.listen(target)

        ctxt = {}
        message = {'foo': 'bar'}

        driver.send(target, ctxt, message)

        received = listener.poll()
        self.assertTrue(received is not None)
        self.assertEquals(received.ctxt, {})
        self.assertEquals(received.message, {'foo': 'bar'})
