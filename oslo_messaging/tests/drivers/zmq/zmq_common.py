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

import logging
import threading

import fixtures
from six.moves import mock
import testtools

import oslo_messaging
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_options
from oslo_messaging._i18n import _LE
from oslo_messaging.tests import utils as test_utils

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class TestServerListener(object):

    def __init__(self, driver):
        self.driver = driver
        self.listener = None
        self.executor = zmq_async.get_executor(self._run)
        self._stop = threading.Event()
        self._received = threading.Event()
        self.message = None

    def listen(self, target):
        self.listener = self.driver.listen(target, None,
                                           None)._poll_style_listener
        self.executor.execute()

    def listen_notifications(self, targets_and_priorities):
        self.listener = self.driver.listen_for_notifications(
            targets_and_priorities, None, None, None)._poll_style_listener
        self.executor.execute()

    def _run(self):
        try:
            messages = self.listener.poll()
            if messages:
                message = messages[0]
                message.acknowledge()
                self._received.set()
                self.message = message
                message.reply(reply=True)
        except Exception:
            LOG.exception(_LE("Unexpected exception occurred."))

    def stop(self):
        self.executor.stop()


class ZmqBaseTestCase(test_utils.BaseTestCase):
    """Base test case for all ZMQ tests """

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(ZmqBaseTestCase, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'
        zmq_options.register_opts(self.conf, mock.MagicMock())

        # Set config values
        self.internal_ipc_dir = self.useFixture(fixtures.TempDir()).path
        kwargs = {'rpc_zmq_bind_address': '127.0.0.1',
                  'rpc_zmq_host': '127.0.0.1',
                  'rpc_zmq_ipc_dir': self.internal_ipc_dir,
                  'use_pub_sub': False,
                  'use_router_proxy': False,
                  'rpc_zmq_matchmaker': 'dummy'}
        self.config(group='oslo_messaging_zmq', **kwargs)
        self.config(rpc_response_timeout=5)

        # Get driver
        transport = oslo_messaging.get_transport(self.conf)
        self.driver = transport._driver

        self.listener = TestServerListener(self.driver)

        self.addCleanup(
            StopRpc(self, [('listener', 'stop'), ('driver', 'cleanup')])
        )


class StopRpc(object):
    def __init__(self, obj, attrs_and_stops):
        self.obj = obj
        self.attrs_and_stops = attrs_and_stops

    def __call__(self):
        for attr, stop in self.attrs_and_stops:
            if hasattr(self.obj, attr):
                obj_attr = getattr(self.obj, attr)
                if hasattr(obj_attr, stop):
                    obj_attr_stop = getattr(obj_attr, stop)
                    obj_attr_stop()
