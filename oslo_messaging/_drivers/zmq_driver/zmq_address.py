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

import six


def combine_address(host, port):
    return "%s:%s" % (host, port)


def get_tcp_direct_address(host):
    return "tcp://%s" % str(host)


def get_tcp_random_address(conf):
    return "tcp://%s" % conf.rpc_zmq_bind_address


def get_broker_address(conf):
    return "ipc://%s/zmq-broker" % conf.rpc_zmq_ipc_dir


def prefix_str(key, listener_type):
    return listener_type + "_" + key


def target_to_key(target, listener_type):

    def prefix(key):
        return prefix_str(key, listener_type)

    if target.topic and target.server:
        attributes = ['topic', 'server']
        key = ".".join(getattr(target, attr) for attr in attributes)
        return prefix(key)
    if target.topic:
        return prefix(target.topic)


def target_to_subscribe_filter(target):
    if target.topic and target.server:
        attributes = ['topic', 'server']
        key = "/".join(getattr(target, attr) for attr in attributes)
        return six.b(key)
    if target.topic:
        return six.b(target.topic)
    if target.server:
        return six.b(target.server)
