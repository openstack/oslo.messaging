
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
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

import urlparse

from oslo.config import cfg
from stevedore import driver

from openstack.common.messaging import drivers

_transport_opts = [
    cfg.StrOpt('transport_url',
               default=None,
               help='A URL representing the messaging driver to use and its '
                    'full configuration. If not set, we fall back to the '
                    'rpc_backend option and driver specific configuration.'),
    cfg.StrOpt('rpc_backend',
               default='kombu',
               help='The messaging driver to use, defaults to kombu. Other '
                    'drivers include qpid and zmq.'),
    cfg.StrOpt('control_exchange',
               default='openstack',
               help='The default exchange under which topics are scoped. May '
                    'be overridden by an exchange name specified in the '
                    'transport_url option.'),
]


def set_defaults(control_exchange):
    cfg.set_defaults(_transport_opts,
                     control_exchange=control_exchange)


class Transport(object):

    def __init__(self, driver):
        self.conf = driver.conf
        self._driver = driver

    def _send(self, target, message, wait_for_reply=None, timeout=None):
        return self._driver.send(target, message,
                                 wait_for_reply=wait_for_reply,
                                 timeout=timeout)

    def _listen(self, target):
        return self._driver.listen(target)


def get_transport(conf, url=None):
    conf.register_opts(_transport_opts)

    url = url or conf.transport_url
    if url is not None:
        rpc_backend = urlparse.urlparse(url).scheme
    else:
        rpc_backend = conf.rpc_backend

    kwargs = dict(default_exchange=conf.control_exchange)
    if url is not None:
        kwargs['url'] = url

    mgr = driver.DriverManager(drivers.NAMESPACE,
                               rpc_backend,
                               invoke_on_load=True,
                               invoke_args=[conf],
                               invoke_kwds=kwargs)
    return Transport(mgr.driver)
