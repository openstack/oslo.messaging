# Copyright 2014 Rackspace Hosting
# All Rights Reserved.
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

import fnmatch
import logging

import six
from stevedore import dispatch
import yaml

from oslo.config import cfg
from oslo.messaging.notify import notifier
from oslo.messaging.openstack.common.gettextutils import _  # noqa


LOG = logging.getLogger(__name__)

router_config = cfg.StrOpt('routing_notifier_config', default='',
                           help='RoutingNotifier configuration file location.')

CONF = cfg.CONF
CONF.register_opt(router_config)


class RoutingDriver(notifier._Driver):
    NOTIFIER_PLUGIN_NAMESPACE = 'oslo.messaging.notify.drivers'

    plugin_manager = None
    routing_groups = None  # The routing groups from the config file.
    used_drivers = None  # Used driver names, extracted from config file.

    def _should_load_plugin(self, ext, *args, **kwargs):
        # Hack to keep stevedore from circular importing since these
        # endpoints are used for different purposes.
        if ext.name == 'routing':
            return False
        return ext.name in self.used_drivers

    def _get_notifier_config_file(self, filename):
        """Broken out for testing."""
        return file(filename, 'r')

    def _load_notifiers(self):
        """One-time load of notifier config file."""
        self.routing_groups = {}
        self.used_drivers = set()
        filename = CONF.routing_notifier_config
        if not filename:
            return

        # Infer which drivers are used from the config file.
        self.routing_groups = yaml.load(
            self._get_notifier_config_file(filename))
        if not self.routing_groups:
            self.routing_groups = {}  # In case we got None from load()
            return

        for group in self.routing_groups.values():
            self.used_drivers.update(group.keys())

        LOG.debug('loading notifiers from %s', self.NOTIFIER_PLUGIN_NAMESPACE)
        self.plugin_manager = dispatch.DispatchExtensionManager(
            namespace=self.NOTIFIER_PLUGIN_NAMESPACE,
            check_func=self._should_load_plugin,
            invoke_on_load=True,
            invoke_args=None)
        if not list(self.plugin_manager):
            LOG.warning(_("Failed to load any notifiers for %s"),
                        self.NOTIFIER_PLUGIN_NAMESPACE)

    def _get_drivers_for_message(self, group, event_type, priority):
        """Which drivers should be called for this event_type
           or priority.
        """
        accepted_drivers = set()

        for driver, rules in six.iteritems(group):
            checks = []
            for key, patterns in six.iteritems(rules):
                if key == 'accepted_events':
                    c = [fnmatch.fnmatch(event_type, p)
                         for p in patterns]
                    checks.append(any(c))
                if key == 'accepted_priorities':
                    c = [fnmatch.fnmatch(priority, p.lower())
                         for p in patterns]
                    checks.append(any(c))
            if all(checks):
                accepted_drivers.add(driver)

        return list(accepted_drivers)

    def _filter_func(self, ext, context, message, priority, retry,
                     accepted_drivers):
        """True/False if the driver should be called for this message.
        """
        # context is unused here, but passed in by map()
        return ext.name in accepted_drivers

    def _call_notify(self, ext, context, message, priority, retry,
                     accepted_drivers):
        """Emit the notification.
        """
        # accepted_drivers is passed in as a result of the map() function
        LOG.info(_("Routing '%(event)s' notification to '%(driver)s' driver"),
                 {'event': message.get('event_type'), 'driver': ext.name})
        ext.obj.notify(context, message, priority, retry)

    def notify(self, context, message, priority, retry):
        if not self.plugin_manager:
            self._load_notifiers()

        # Fail if these aren't present ...
        event_type = message['event_type']

        accepted_drivers = set()
        for group in self.routing_groups.values():
            accepted_drivers.update(
                self._get_drivers_for_message(group, event_type,
                                              priority.lower()))
        self.plugin_manager.map(self._filter_func, self._call_notify, context,
                                message, priority, retry,
                                list(accepted_drivers))
