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

import copy
import itertools

from oslo_messaging._drivers import amqp
from oslo_messaging._drivers import impl_rabbit
from oslo_messaging._drivers.kafka_driver import kafka_options
from oslo_messaging.notify import notifier
from oslo_messaging.rpc import client
from oslo_messaging.rpc import dispatcher
from oslo_messaging import server
from oslo_messaging import transport

__all__ = [
    'list_opts'
]

_global_opt_lists = [
    server._pool_opts,
    client._client_opts,
    transport._transport_opts,
    dispatcher._dispatcher_opts,
]

_opts = [
    (None, list(itertools.chain(*_global_opt_lists))),
    ('oslo_messaging_notifications', notifier._notifier_opts),
    ('oslo_messaging_rabbit', list(
        itertools.chain(amqp.amqp_opts, impl_rabbit.rabbit_opts))),
    ('oslo_messaging_kafka', kafka_options.KAFKA_OPTS),
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


def set_defaults(conf, executor_thread_pool_size=None):
    """Set defaults for configuration variables.

    Overrides default options values.

    :param conf: Config instance specified to set default options in it. Using
     of instances instead of a global config object prevents conflicts between
     options declaration.
    :type conf: oslo.config.cfg.ConfigOpts instance.

    :keyword executor_thread_pool_size: Size of executor thread pool.
    :type executor_thread_pool_size: int
    :default executor_thread_pool_size: None

    """
    if executor_thread_pool_size is not None:
        conf.set_default('executor_thread_pool_size',
                         executor_thread_pool_size)
