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

from oslo_messaging import target


def get_ipc_address_call(conf, target):
    target_addr = target_to_str(target)
    return "ipc://%s/%s" % (conf.rpc_zmq_ipc_dir, target_addr)


def get_tcp_bind_address(port):
    return "tcp://*:%s" % port


def get_tcp_address_call(conf, host):
    return "tcp://%s:%s" % (host, conf.rpc_zmq_port)


def get_ipc_address_cast(conf, target):
    target_addr = target_to_str(target)
    return "ipc://%s/fanout/%s" % (conf.rpc_zmq_ipc_dir, target_addr)


def get_ipc_address_fanout(conf):
    return "ipc://%s/fanout_general" % conf.rpc_zmq_ipc_dir


def target_to_str(target):
    if target.server is None:
        return target.topic
    return "%s.%s" % (target.server, target.topic)


def target_from_dict(target_dict):
    return target.Target(exchange=target_dict['exchange'],
                         topic=target_dict['topic'],
                         namespace=target_dict['namespace'],
                         version=target_dict['version'],
                         server=target_dict['server'],
                         fanout=target_dict['fanout'])
