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


def get_ipc_address_call(conf, topic):
    return "ipc://%s/%s" % (conf.rpc_zmq_ipc_dir, str(topic))


def get_tcp_bind_address(port):
    return "tcp://*:%s" % port


def get_tcp_address_call(conf, topic):
    return "tcp://%s:%s" % (topic.server, conf.rpc_zmq_port)


def get_ipc_address_cast(conf, topic):
    return "ipc://%s/fanout/%s" % (conf.rpc_zmq_ipc_dir, str(topic))


def get_ipc_address_fanout(conf):
    return "ipc://%s/fanout_general" % conf.rpc_zmq_ipc_dir


class Topic(object):

    def __init__(self, conf, topic, server=None, fanout=False):

        if server is None:
            self.server = conf.rpc_zmq_host
        else:
            self.server = server

        self._topic = topic
        self.fanout = fanout

    @staticmethod
    def _extract_cinder_server(server):
        return server.split('@', 1)[0]

    @staticmethod
    def from_target(conf, target):
        if target.server is not None:
            return Topic(conf, target.topic, target.server,
                         fanout=target.fanout)
        else:
            return Topic(conf, target.topic, fanout=target.fanout)

    @property
    def topic(self):
        return self._topic if self._topic else ""

    def __str__(self, *args, **kwargs):
        return u"%s.%s" % (self.topic, self.server)
