
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

__all__ = ['ConfFixture']

import sys

import fixtures


def _import_opts(conf, module, opts):
    __import__(module)
    conf.register_opts(getattr(sys.modules[module], opts))


class ConfFixture(fixtures.Fixture):

    """Tweak configuration options for unit testing.

    oslo.messaging registers a number of configuration options, but rather than
    directly referencing those options, users of the API should use this
    interface for querying and overriding certain configuration options.

    An example usage::

        self.messaging_conf = self.useFixture(messaging.ConfFixture(cfg.CONF))
        self.messaging_conf.transport_driver = 'fake'

    :param conf: a ConfigOpts instance
    :type conf: oslo.config.cfg.ConfigOpts
    """

    def __init__(self, conf):
        self.conf = conf
        _import_opts(self.conf,
                     'oslo.messaging._drivers.impl_rabbit', 'rabbit_opts')
        _import_opts(self.conf,
                     'oslo.messaging._drivers.impl_qpid', 'qpid_opts')
        _import_opts(self.conf,
                     'oslo.messaging._drivers.amqp', 'amqp_opts')
        _import_opts(self.conf, 'oslo.messaging.rpc.client', '_client_opts')
        _import_opts(self.conf, 'oslo.messaging.transport', '_transport_opts')
        _import_opts(self.conf,
                     'oslo.messaging.notify.notifier', '_notifier_opts')

    def setUp(self):
        super(ConfFixture, self).setUp()
        self.addCleanup(self.conf.reset)

    @property
    def transport_driver(self):
        """The transport driver - for example 'rabbit', 'qpid' or 'fake'."""
        return self.conf.rpc_backend

    @transport_driver.setter
    def transport_driver(self, value):
        self.conf.set_override('rpc_backend', value)

    @property
    def in_memory(self):
        """Use an in-memory transport; currently supported by rabbit driver."""
        if (('rabbit' in self.transport_driver or
             'kombu' in self.transport_driver)):
            return self.conf.fake_rabbit
        else:
            return False

    @in_memory.setter
    def in_memory(self, value):
        if (('rabbit' in self.transport_driver or
             'kombu' in self.transport_driver)):
            self.conf.set_override('fake_rabbit', value)

    @property
    def response_timeout(self):
        """Default number of seconds to wait for a response from a call."""
        return self.conf.rpc_response_timeout

    @response_timeout.setter
    def response_timeout(self, value):
        self.conf.set_override('rpc_response_timeout', value)
