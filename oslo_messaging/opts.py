
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

from oslo_messaging._drivers import amqp
from oslo_messaging._drivers import impl_qpid
from oslo_messaging._drivers import impl_rabbit
from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers import matchmaker
from oslo_messaging._drivers import matchmaker_redis
from oslo_messaging._drivers import matchmaker_ring
from oslo_messaging._drivers.protocols.amqp import opts as amqp_opts
from oslo_messaging._executors import base
from oslo_messaging.notify import notifier
from oslo_messaging.rpc import client
from oslo_messaging import transport

_global_opt_lists = [
    impl_zmq.zmq_opts,
    matchmaker.matchmaker_opts,
    base._pool_opts,
    notifier._notifier_opts,
    client._client_opts,
    transport._transport_opts,
]

_opts = [
    (None, list(itertools.chain(*_global_opt_lists))),
    ('matchmaker_redis', matchmaker_redis.matchmaker_redis_opts),
    ('matchmaker_ring', matchmaker_ring.matchmaker_opts),
    ('oslo_messaging_amqp', amqp_opts.amqp1_opts),
    ('oslo_messaging_rabbit', list(itertools.chain(amqp.amqp_opts,
                                                   impl_rabbit.rabbit_opts))),
    ('oslo_messaging_qpid', list(itertools.chain(amqp.amqp_opts,
                                                 impl_qpid.qpid_opts)))
]


def list_opts():
    """Return a list of oslo.config options available in the library.

    The returned list includes all oslo.config options which may be registered
    at runtime by the library.

    Each element of the list is a tuple. The first element is the name of the
    group under which the list of elements in the second element will be
    registered. A group name of None corresponds to the [DEFAULT] group in
    config files.

    This function is also discoverable via the 'oslo_messaging' entry point
    under the 'oslo.config.opts' namespace.

    The purpose of this is to allow tools like the Oslo sample config file
    generator to discover the options exposed to users by this library.

    :returns: a list of (group_name, opts) tuples
    """
    return [(g, copy.deepcopy(o)) for g, o in _opts]
