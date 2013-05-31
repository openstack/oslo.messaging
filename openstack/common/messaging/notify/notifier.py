
# Copyright 2011 OpenStack Foundation.
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

import abc

from oslo.config import cfg
from stevedore import named

from openstack.common.gettextutils import _
from openstack.common import log as logging
from openstack.common import messaging
from openstack.common import timeutils
from openstack.common import uuidutils

_notifier_opts = [
    cfg.MultiStrOpt('notification_driver',
                    default=[],
                    help='Driver or drivers to handle sending notifications'),
    cfg.ListOpt('notification_topics',
                default=['notifications', ],
                deprecated_name='topics',
                deprecated_group='rpc_notifier2',
                help='AMQP topic used for openstack notifications'),
]

LOG = logging.getLogger(__name__)


class _Driver(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, conf, topics, transport):
        self.conf = conf
        self.topics = topics
        self.transport = transport

    @abc.abstractmethod
    def notify(self, context, msg, priority):
        pass


class Notifier(object):

    def __init__(self, conf, publisher_id,
                 driver=None, topic=None, transport=None):
        self.conf = conf
        self.conf.register_opts(_notifier_opts)

        self.publisher_id = publisher_id

        self._driver_names = ([driver] if driver is not None
                              else conf.notification_driver)

        self._topics = ([topic] if topic is not None
                        else conf.notification_topics)
        self._transport = transport or messaging.get_transport(conf)

        self._driver_mgr = named.NamedExtensionManager(
            'openstack.common.notify.drivers',
            names=self._driver_names,
            invoke_on_load=True,
            invoke_args=[self.conf],
            invoke_kwds={
                'topics': self._topics,
                'transport': self._transport,
            },
        )

    def _notify(self, context, event_type, payload, priority):
        msg = dict(message_id=uuidutils.generate_uuid(),
                   publisher_id=self.publisher_id,
                   event_type=event_type,
                   priority=priority,
                   payload=payload,
                   timestamp=str(timeutils.utcnow()))

        def do_notify(ext):
            try:
                ext.obj.notify(context, msg, priority)
            except Exception as e:
                LOG.exception(_("Problem '%(e)s' attempting to send to "
                                "notification system. Payload=%(payload)s")
                              % dict(e=e, payload=payload))
        self._driver_mgr.map(do_notify)

    def debug(self, context, event_type, payload):
        self._notify(context, event_type, payload, 'DEBUG')

    def info(self, context, event_type, payload):
        self._notify(context, event_type, payload, 'INFO')

    def warn(self, context, event_type, payload):
        self._notify(context, event_type, payload, 'WARN')

    def error(self, context, event_type, payload):
        self._notify(context, event_type, payload, 'ERROR')

    def critical(self, context, event_type, payload):
        self._notify(context, event_type, payload, 'CRITICAL')
