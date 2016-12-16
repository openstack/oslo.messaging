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


import os
import signal
import time

import fixtures
from pifpaf.drivers import rabbitmq

from oslo_messaging.tests.functional import utils
from oslo_messaging.tests import utils as test_utils


class ConnectedPortMatcher(object):
    def __init__(self, port):
        self.port = port

    def __eq__(self, data):
        return data.get("port") == self.port

    def __repr__(self):
        return "<ConnectedPortMatcher port=%d>" % self.port


class RabbitMQFailoverTests(test_utils.BaseTestCase):
    DRIVERS = [
        "rabbit",
    ]

    def test_failover_scenario(self):
        # NOTE(sileht): run this test only if functional suite run of a driver
        # that use rabbitmq as backend
        self.driver = os.environ.get('TRANSPORT_DRIVER')
        if self.driver not in self.DRIVERS:
            self.skipTest("TRANSPORT_DRIVER is not set to a rabbit driver")

        # NOTE(sileht): Allow only one response at a time, to
        # have only one tcp connection for reply and ensure it will failover
        # correctly
        self.config(heartbeat_timeout_threshold=1,
                    rpc_conn_pool_size=1,
                    kombu_reconnect_delay=0,
                    rabbit_retry_interval=0,
                    rabbit_retry_backoff=0,
                    group='oslo_messaging_rabbit')

        self.pifpaf = self.useFixture(rabbitmq.RabbitMQDriver(cluster=True,
                                                              port=5692))

        self.url = self.pifpaf.env["PIFPAF_URL"]
        self.n1 = self.pifpaf.env["PIFPAF_RABBITMQ_NODENAME1"]
        self.n2 = self.pifpaf.env["PIFPAF_RABBITMQ_NODENAME2"]
        self.n3 = self.pifpaf.env["PIFPAF_RABBITMQ_NODENAME3"]

        # NOTE(gdavoian): additional tweak for pika driver
        if self.driver == "pika":
            self.url = self.url.replace("rabbit", "pika")

        # ensure connections will be establish to the first node
        self.pifpaf.stop_node(self.n2)
        self.pifpaf.stop_node(self.n3)

        self.servers = self.useFixture(utils.RpcServerGroupFixture(
            self.conf, self.url, endpoint=self, names=["server"]))

        # Don't randomize rabbit hosts
        self.useFixture(fixtures.MockPatch(
            'oslo_messaging._drivers.impl_rabbit.random',
            side_effect=lambda x: x))

        # NOTE(sileht): this connects server connections and reply
        # connection to nodename n1
        self.client = self.servers.client(0)
        self.client.ping()
        self._check_ports(self.pifpaf.port)

        # Switch to node n2
        self.pifpaf.start_node(self.n2)
        self.assertEqual("callback done", self.client.kill_and_process())
        self.assertEqual("callback done", self.client.just_process())
        self._check_ports(self.pifpaf.get_port(self.n2))

        # Switch to node n3
        self.pifpaf.start_node(self.n3)
        time.sleep(0.1)
        self.pifpaf.kill_node(self.n2, signal=signal.SIGKILL)
        time.sleep(0.1)
        self.assertEqual("callback done", self.client.just_process())
        self._check_ports(self.pifpaf.get_port(self.n3))

        self.pifpaf.start_node(self.n1)
        time.sleep(0.1)
        self.pifpaf.kill_node(self.n3, signal=signal.SIGKILL)
        time.sleep(0.1)
        self.assertEqual("callback done", self.client.just_process())
        self._check_ports(self.pifpaf.get_port(self.n1))

    def kill_and_process(self, *args, **kargs):
        self.pifpaf.kill_node(self.n1, signal=signal.SIGKILL)
        time.sleep(0.1)
        return "callback done"

    def just_process(self, *args, **kargs):
        return "callback done"

    def _check_ports(self, port):
        getattr(self, '_check_ports_%s_driver' % self.driver)(port)

    def _check_ports_pika_driver(self, port):
        rpc_server = self.servers.servers[0].server
        # FIXME(sileht): Check other connections
        connections = [
            rpc_server.listener._connection
        ]
        for conn in connections:
            self.assertEqual(
                port, conn._impl.socket.getpeername()[1])

    def _check_ports_rabbit_driver(self, port):
        rpc_server = self.servers.servers[0].server
        connection_contexts = [
            # rpc server
            rpc_server.listener._poll_style_listener.conn,
            # rpc client
            self.client.client.transport._driver._get_connection(),
            # rpc client replies waiter
            self.client.client.transport._driver._reply_q_conn,
        ]

        ports = [cctxt.connection.channel.connection.sock.getpeername()[1]
                 for cctxt in connection_contexts]

        self.assertEqual([port] * len(ports), ports,
                         "expected: %s, rpc-server: %s, rpc-client: %s, "
                         "rpc-replies: %s" % tuple([port] + ports))
