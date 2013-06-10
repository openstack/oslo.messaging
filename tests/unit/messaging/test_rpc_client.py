
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

from oslo.config import cfg

from openstack.common import messaging
from tests import utils as test_utils


class _FakeTransport(object):

    def __init__(self, conf):
        self.conf = conf

    def _send(self, *args, **kwargs):
        pass


class TestRPCClient(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRPCClient, self).setUp(conf=cfg.ConfigOpts())

    def test_rpc_client(self):
        transport = _FakeTransport(self.conf)
        client = messaging.RPCClient(transport, messaging.Target())

        self.mox.StubOutWithMock(transport, '_send')
        transport._send(messaging.Target(), {},
                        dict(method='foo', args={}))
        self.mox.ReplayAll()

        client.cast({}, 'foo')
