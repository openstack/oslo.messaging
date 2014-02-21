
# Copyright 2014 Red Hat, Inc.
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

__all__ = [
    'list_opts'
]

import copy
import itertools

from oslo.messaging._drivers import amqp
from oslo.messaging._drivers import impl_qpid
from oslo.messaging._drivers import impl_rabbit
from oslo.messaging._drivers import impl_zmq
from oslo.messaging._drivers import matchmaker
from oslo.messaging._drivers import matchmaker_redis
from oslo.messaging._drivers import matchmaker_ring
from oslo.messaging._drivers.protocols.amqp import driver as amqp1_driver
from oslo.messaging._executors import impl_eventlet
from oslo.messaging.notify import notifier
from oslo.messaging.rpc import client
from oslo.messaging import transport

_global_opt_lists = [
    amqp.amqp_opts,
    impl_qpid.qpid_opts,
    impl_rabbit.rabbit_opts,
    impl_zmq.zmq_opts,
    matchmaker.matchmaker_opts,
    impl_eventlet._eventlet_opts,
    notifier._notifier_opts,
    client._client_opts,
    transport._transport_opts,
    amqp1_driver.get_opts()
]

_opts = [
    (None, list(itertools.chain(*_global_opt_lists))),
    ('matchmaker_redis', matchmaker_redis.matchmaker_redis_opts),
    ('matchmaker_ring', matchmaker_ring.matchmaker_opts),
]


def list_opts():
    """Return a list of oslo.config options available in the library.

    The returned list includes all oslo.config options which may be registered
    at runtime by the library.

    Each element of the list is a tuple. The first element is the name of the
    group under which the list of elements in the second element will be
    registered. A group name of None corresponds to the [DEFAULT] group in
    config files.

    This function is also discoverable via the 'oslo.messaging' entry point
    under the 'oslo.config.opts' namespace.

    The purpose of this is to allow tools like the Oslo sample config file
    generator to discover the options exposed to users by this library.

    :returns: a list of (group_name, opts) tuples
    """
    return [(g, copy.deepcopy(o)) for g, o in _opts]
