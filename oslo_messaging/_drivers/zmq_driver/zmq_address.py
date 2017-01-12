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
    return "tcp://%s" % conf.oslo_messaging_zmq.rpc_zmq_bind_address


def prefix_str(key, listener_type):
    return listener_type + "/" + key


def target_to_key(target, listener_type=None):
    key = target.topic
    if target.server and not target.fanout:
        # FIXME(ozamiatin): Workaround for Cinder.
        # Remove split when Bug #1630975 is being fixed.
        key += "/" + target.server.split('@')[0]
    return prefix_str(key, listener_type) if listener_type else key


def target_to_subscribe_filter(target):
    return six.b(target.topic)
