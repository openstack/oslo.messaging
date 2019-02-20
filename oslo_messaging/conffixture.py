
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

import sys

import fixtures
from functools import wraps

__all__ = ['ConfFixture']


def _import_opts(conf, module, opts, group=None):
    __import__(module)
    conf.register_opts(getattr(sys.modules[module], opts), group=group)


class ConfFixture(fixtures.Fixture):

    """Tweak configuration options for unit testing.

    oslo.messaging registers a number of configuration options, but rather than
    directly referencing those options, users of the API should use this
    interface for querying and overriding certain configuration options.

    An example usage::

        self.messaging_conf = self.useFixture(messaging.ConfFixture(cfg.CONF))
        self.messaging_conf.transport_url = 'fake:/'

    :param conf: a ConfigOpts instance
    :type conf: oslo.config.cfg.ConfigOpts
    :param transport_url: override default transport_url value
    :type transport_url: str
    """

    def __init__(self, conf, transport_url=None):
        self.conf = conf
        _import_opts(self.conf,
                     'oslo_messaging._drivers.impl_rabbit', 'rabbit_opts',
                     'oslo_messaging_rabbit')
        _import_opts(self.conf,
                     'oslo_messaging._drivers.base', 'base_opts',
                     'oslo_messaging_rabbit')
        _import_opts(self.conf,
                     'oslo_messaging._drivers.amqp', 'amqp_opts',
                     'oslo_messaging_rabbit')
        _import_opts(self.conf,
                     'oslo_messaging._drivers.amqp1_driver.opts',
                     'amqp1_opts', 'oslo_messaging_amqp')
        _import_opts(self.conf, 'oslo_messaging.rpc.client', '_client_opts')
        _import_opts(self.conf, 'oslo_messaging.transport', '_transport_opts')
        _import_opts(self.conf,
                     'oslo_messaging.notify.notifier',
                     '_notifier_opts',
                     'oslo_messaging_notifications')

        if transport_url is not None:
            self.transport_url = transport_url

    def _setup_decorator(self):
        # Support older test cases that still use the set_override
        # with the old config key names
        def decorator_for_set_override(wrapped_function):
            @wraps(wrapped_function)
            def _wrapper(*args, **kwargs):
                group = 'oslo_messaging_notifications'
                if args[0] == 'notification_driver':
                    args = ('driver', args[1], group)
                elif args[0] == 'notification_transport_url':
                    args = ('transport_url', args[1], group)
                elif args[0] == 'notification_topics':
                    args = ('topics', args[1], group)
                return wrapped_function(*args, **kwargs)
            _wrapper.wrapped = wrapped_function
            return _wrapper

        def decorator_for_clear_override(wrapped_function):
            @wraps(wrapped_function)
            def _wrapper(*args, **kwargs):
                group = 'oslo_messaging_notifications'
                if args[0] == 'notification_driver':
                    args = ('driver', group)
                elif args[0] == 'notification_transport_url':
                    args = ('transport_url', group)
                elif args[0] == 'notification_topics':
                    args = ('topics', group)
                return wrapped_function(*args, **kwargs)
            _wrapper.wrapped = wrapped_function
            return _wrapper

        if not hasattr(self.conf.set_override, 'wrapped'):
            self.conf.set_override = decorator_for_set_override(
                self.conf.set_override)
        if not hasattr(self.conf.clear_override, 'wrapped'):
            self.conf.clear_override = decorator_for_clear_override(
                self.conf.clear_override)

    def _teardown_decorator(self):
        if hasattr(self.conf.set_override, 'wrapped'):
            self.conf.set_override = self.conf.set_override.wrapped
        if hasattr(self.conf.clear_override, 'wrapped'):
            self.conf.clear_override = self.conf.clear_override.wrapped

    def setUp(self):
        super(ConfFixture, self).setUp()
        self._setup_decorator()
        self.addCleanup(self._teardown_decorator)
        self.addCleanup(self.conf.reset)

    @property
    def transport_url(self):
        """The transport url"""
        return self.conf.transport_url

    @transport_url.setter
    def transport_url(self, value):
        self.conf.set_override('transport_url', value)

    @property
    def response_timeout(self):
        """Default number of seconds to wait for a response from a call."""
        return self.conf.rpc_response_timeout

    @response_timeout.setter
    def response_timeout(self, value):
        self.conf.set_override('rpc_response_timeout', value)
