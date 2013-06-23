
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
import logging

from oslo.config import cfg
from stevedore import named

from oslo import messaging
from oslo.messaging.openstack.common import timeutils
from oslo.messaging.openstack.common import uuidutils

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
    def notify(self, ctxt, msg, priority):
        pass


class Notifier(object):

    """Send notification messages.

    The Notifier class is used for sending notification messages over a
    messaging transport or other means.

    Notification messages follow the following format:

        {'message_id': str(uuid.uuid4()),
         'publisher_id': 'compute.host1',
         'timestamp': timeutils.utcnow(),
         'priority': 'WARN',
         'event_type': 'compute.create_instance',
         'payload': {'instance_id': 12, ... }}

    A Notifier object can be instantiated with a configuration object and a
    publisher ID:

        notifier = notifier.Notifier(cfg.CONF, 'compute.host1')

    and notifications are sent via drivers chosen with the notification_driver
    config option, on the topics consen with the notification_topics config
    option, on a transport contstructed using the supplied configuration.

    Alternatively, a Notifier object can be instantiated with a specific
    driver, topic or transport:

        notifier = notifier.Notifier(cfg.CONF,
                                     'compute.host',
                                     driver='messaging',
                                     topic='notifications',
                                     transport=RPC_TRANSPORT)
    """

    def __init__(self, conf, publisher_id,
                 driver=None, topic=None, transport=None):
        """Construct a Notifier object.

        :param conf: user configuration, used for e.g. notification_driver
        :type conf: a cfg.ConfigOpts instance
        :param publisher_id: field in notifications sent, e.g. 'compute.host1'
        :type publisher_id: str
        :param driver: a driver to lookup from oslo.messaging.notify.drivers
        :type driver: str
        :param topic: the topic which to send messages on
        :type topic: str
        :param transport: the transport to use for sending messages
        :type transport: oslo.messaging.Transport
        """
        self.conf = conf
        self.conf.register_opts(_notifier_opts)

        self.publisher_id = publisher_id

        self._driver_names = ([driver] if driver is not None
                              else conf.notification_driver)

        self._topics = ([topic] if topic is not None
                        else conf.notification_topics)
        self._transport = transport or messaging.get_transport(conf)

        self._driver_mgr = named.NamedExtensionManager(
            'oslo.messaging.notify.drivers',
            names=self._driver_names,
            invoke_on_load=True,
            invoke_args=[self.conf],
            invoke_kwds={
                'topics': self._topics,
                'transport': self._transport,
            },
        )

    def _notify(self, ctxt, event_type, payload, priority):
        msg = dict(message_id=uuidutils.generate_uuid(),
                   publisher_id=self.publisher_id,
                   event_type=event_type,
                   priority=priority,
                   payload=payload,
                   timestamp=str(timeutils.utcnow()))

        def do_notify(ext):
            try:
                ext.obj.notify(ctxt, msg, priority)
            except Exception as e:
                LOG.exception("Problem '%(e)s' attempting to send to "
                              "notification system. Payload=%(payload)s",
                              dict(e=e, payload=payload))
        self._driver_mgr.map(do_notify)

    def debug(self, ctxt, event_type, payload):
        """Send a notification at debug level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, e.g. 'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        """
        self._notify(ctxt, event_type, payload, 'DEBUG')

    def info(self, ctxt, event_type, payload):
        """Send a notification at info level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, e.g. 'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        """
        self._notify(ctxt, event_type, payload, 'INFO')

    def warn(self, ctxt, event_type, payload):
        """Send a notification at warning level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, e.g. 'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        """
        self._notify(ctxt, event_type, payload, 'WARN')

    def error(self, ctxt, event_type, payload):
        """Send a notification at error level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, e.g. 'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        """
        self._notify(ctxt, event_type, payload, 'ERROR')

    def critical(self, ctxt, event_type, payload):
        """Send a notification at critical level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, e.g. 'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        """
        self._notify(ctxt, event_type, payload, 'CRITICAL')
