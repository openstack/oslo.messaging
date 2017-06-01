
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
import argparse
import logging
import uuid

from oslo_config import cfg
from oslo_utils import timeutils
import six
from stevedore import extension
from stevedore import named

from oslo_messaging._i18n import _LE
from oslo_messaging import serializer as msg_serializer
from oslo_messaging import transport as msg_transport

_notifier_opts = [
    cfg.MultiStrOpt('driver',
                    default=[],
                    deprecated_name='notification_driver',
                    deprecated_group='DEFAULT',
                    help='The Drivers(s) to handle sending notifications. '
                         'Possible values are messaging, messagingv2, '
                         'routing, log, test, noop'),
    cfg.StrOpt('transport_url',
               deprecated_name='notification_transport_url',
               deprecated_group='DEFAULT',
               secret=True,
               help='A URL representing the messaging driver to use for '
                    'notifications. If not set, we fall back to the same '
                    'configuration used for RPC.'),
    cfg.ListOpt('topics',
                default=['notifications', ],
                deprecated_opts=[
                    cfg.DeprecatedOpt('topics',
                                      group='rpc_notifier2'),
                    cfg.DeprecatedOpt('notification_topics',
                                      group='DEFAULT')
                ],
                help='AMQP topic used for OpenStack notifications.'),
    cfg.IntOpt('retry', default=-1,
               help='The maximum number of attempts to re-send a notification '
                    'message which failed to be delivered due to a '
                    'recoverable error. 0 - No retry, -1 - indefinite'),
]

_LOG = logging.getLogger(__name__)


def _send_notification():
    """Command line tool to send notifications manually."""
    parser = argparse.ArgumentParser(
        description='Oslo.messaging notification sending',
    )
    parser.add_argument('--config-file',
                        help='Path to configuration file')
    parser.add_argument('--transport-url',
                        help='Transport URL')
    parser.add_argument('--publisher-id',
                        help='Publisher ID')
    parser.add_argument('--event-type',
                        default="test",
                        help="Event type")
    parser.add_argument('--topic',
                        nargs='*',
                        help="Topic to send to")
    parser.add_argument('--priority',
                        default="info",
                        choices=("info",
                                 "audit",
                                 "warn",
                                 "error",
                                 "critical",
                                 "sample"),
                        help='Event type')
    parser.add_argument('--driver',
                        default="messagingv2",
                        choices=extension.ExtensionManager(
                            'oslo.messaging.notify.drivers'
                        ).names(),
                        help='Notification driver')
    parser.add_argument('payload')
    args = parser.parse_args()
    conf = cfg.ConfigOpts()
    conf([],
         default_config_files=[args.config_file] if args.config_file else None)
    transport = get_notification_transport(conf, url=args.transport_url)
    notifier = Notifier(transport, args.publisher_id, topics=args.topic,
                        driver=args.driver)
    notifier._notify({}, args.event_type, args.payload, args.priority)


@six.add_metaclass(abc.ABCMeta)
class Driver(object):
    """Base driver for Notifications"""

    def __init__(self, conf, topics, transport):
        """base driver initialization

        :param conf: configuration options
        :param topics: list of topics
        :param transport: transport driver to use
        """
        self.conf = conf
        self.topics = topics
        self.transport = transport

    @abc.abstractmethod
    def notify(self, ctxt, msg, priority, retry):
        """send a single notification with a specific priority

        :param ctxt: current request context
        :param msg: message to be sent
        :type msg: str
        :param priority: priority of the message
        :type priority: str
        :param retry: connection retries configuration (used by the messaging
                      driver):
                      None or -1 means to retry forever.
                      0 means no retry is attempted.
                      N means attempt at most N retries.
        :type retry: int
        """
        pass


def get_notification_transport(conf, url=None,
                               allowed_remote_exmods=None, aliases=None):
    """A factory method for Transport objects for notifications.

    This method should be used for notifications, in case notifications are
    being sent over a different message bus than normal messaging
    functionality; for example, using a different driver, or with different
    access permissions.

    If no transport URL is provided, the URL in the notifications section of
    the config file will be used.  If that URL is also absent, the same
    transport as specified in the messaging section will be used.

    If a transport URL is provided, then this function works exactly the same
    as get_transport.

    :param conf: the user configuration
    :type conf: cfg.ConfigOpts
    :param url: a transport URL, see :py:class:`transport.TransportURL`
    :type url: str or TransportURL
    :param allowed_remote_exmods: a list of modules which a client using this
                                  transport will deserialize remote exceptions
                                  from
    :type allowed_remote_exmods: list
    :param aliases: A map of transport alias to transport name
    :type aliases: dict
    """
    conf.register_opts(_notifier_opts,
                       group='oslo_messaging_notifications')
    if url is None:
        url = conf.oslo_messaging_notifications.transport_url
    return msg_transport._get_transport(conf, url,
                                        allowed_remote_exmods, aliases)


class Notifier(object):

    """Send notification messages.

    The Notifier class is used for sending notification messages over a
    messaging transport or other means.

    Notification messages follow the following format::

        {'message_id': six.text_type(uuid.uuid4()),
         'publisher_id': 'compute.host1',
         'timestamp': timeutils.utcnow(),
         'priority': 'WARN',
         'event_type': 'compute.create_instance',
         'payload': {'instance_id': 12, ... }}

    A Notifier object can be instantiated with a transport object and a
    publisher ID:

        notifier = messaging.Notifier(get_notification_transport(CONF),
                                      'compute')

    and notifications are sent via drivers chosen with the driver
    config option and on the topics chosen with the topics config
    option in [oslo_messaging_notifications] section.

    Alternatively, a Notifier object can be instantiated with a specific
    driver or topic::

        transport = notifier.get_notification_transport(CONF)
        notifier = notifier.Notifier(transport,
                                     'compute.host',
                                     driver='messaging',
                                     topics=['notifications'])

    Notifier objects are relatively expensive to instantiate (mostly the cost
    of loading notification drivers), so it is possible to specialize a given
    Notifier object with a different publisher id using the prepare() method::

        notifier = notifier.prepare(publisher_id='compute')
        notifier.info(ctxt, event_type, payload)
    """

    def __init__(self, transport, publisher_id=None,
                 driver=None, serializer=None, retry=None,
                 topics=None):
        """Construct a Notifier object.

        :param transport: the transport to use for sending messages
        :type transport: oslo_messaging.Transport
        :param publisher_id: field in notifications sent, for example
                             'compute.host1'
        :type publisher_id: str
        :param driver: a driver to lookup from oslo_messaging.notify.drivers
        :type driver: str
        :param serializer: an optional entity serializer
        :type serializer: Serializer
        :param retry: connection retries configuration (used by the messaging
                      driver):
                      None or -1 means to retry forever.
                      0 means no retry is attempted.
                      N means attempt at most N retries.
        :type retry: int
        :param topics: the topics which to send messages on
        :type topics: list of strings
        """
        conf = transport.conf
        conf.register_opts(_notifier_opts,
                           group='oslo_messaging_notifications')

        self.transport = transport
        self.publisher_id = publisher_id
        if retry is not None:
            self.retry = retry
        else:
            self.retry = conf.oslo_messaging_notifications.retry

        self._driver_names = ([driver] if driver is not None else
                              conf.oslo_messaging_notifications.driver)

        if topics is not None:
            self._topics = topics
        else:
            self._topics = conf.oslo_messaging_notifications.topics
        self._serializer = serializer or msg_serializer.NoOpSerializer()

        self._driver_mgr = named.NamedExtensionManager(
            'oslo.messaging.notify.drivers',
            names=self._driver_names,
            invoke_on_load=True,
            invoke_args=[conf],
            invoke_kwds={
                'topics': self._topics,
                'transport': self.transport,
            }
        )

    _marker = object()

    def prepare(self, publisher_id=_marker, retry=_marker):
        """Return a specialized Notifier instance.

        Returns a new Notifier instance with the supplied publisher_id. Allows
        sending notifications from multiple publisher_ids without the overhead
        of notification driver loading.

        :param publisher_id: field in notifications sent, for example
                             'compute.host1'
        :type publisher_id: str
        :param retry: connection retries configuration (used by the messaging
                      driver):
                      None or -1 means to retry forever.
                      0 means no retry is attempted.
                      N means attempt at most N retries.
        :type retry: int
        """
        return _SubNotifier._prepare(self, publisher_id, retry=retry)

    def _notify(self, ctxt, event_type, payload, priority, publisher_id=None,
                retry=None):
        payload = self._serializer.serialize_entity(ctxt, payload)
        ctxt = self._serializer.serialize_context(ctxt)

        msg = dict(message_id=six.text_type(uuid.uuid4()),
                   publisher_id=publisher_id or self.publisher_id,
                   event_type=event_type,
                   priority=priority,
                   payload=payload,
                   timestamp=six.text_type(timeutils.utcnow()))

        def do_notify(ext):
            try:
                ext.obj.notify(ctxt, msg, priority, retry or self.retry)
            except Exception as e:
                _LOG.exception(_LE("Problem '%(e)s' attempting to send to "
                                   "notification system. Payload=%(payload)s"),
                               dict(e=e, payload=payload))

        if self._driver_mgr.extensions:
            self._driver_mgr.map(do_notify)

    def audit(self, ctxt, event_type, payload):
        """Send a notification at audit level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'AUDIT')

    def debug(self, ctxt, event_type, payload):
        """Send a notification at debug level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'DEBUG')

    def info(self, ctxt, event_type, payload):
        """Send a notification at info level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'INFO')

    def warn(self, ctxt, event_type, payload):
        """Send a notification at warning level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'WARN')

    warning = warn

    def error(self, ctxt, event_type, payload):
        """Send a notification at error level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'ERROR')

    def critical(self, ctxt, event_type, payload):
        """Send a notification at critical level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'CRITICAL')

    def sample(self, ctxt, event_type, payload):
        """Send a notification at sample level.

        Sample notifications are for high-frequency events
        that typically contain small payloads. eg: "CPU = 70%"

        Not all drivers support the sample level
        (log, for example) so these could be dropped.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        self._notify(ctxt, event_type, payload, 'SAMPLE')

    def is_enabled(self):
        """Check if the notifier will emit notifications anywhere.

        :return: false if the driver of the notifier is set only to noop, true
                 otherwise
        """
        return self._driver_mgr.names() != ['noop']


class _SubNotifier(Notifier):

    _marker = Notifier._marker

    def __init__(self, base, publisher_id, retry):
        self._base = base
        self.transport = base.transport
        self.publisher_id = publisher_id
        self.retry = retry

        self._serializer = self._base._serializer
        self._driver_mgr = self._base._driver_mgr

    def _notify(self, ctxt, event_type, payload, priority):
        super(_SubNotifier, self)._notify(ctxt, event_type, payload, priority)

    @classmethod
    def _prepare(cls, base, publisher_id=_marker, retry=_marker):
        if publisher_id is cls._marker:
            publisher_id = base.publisher_id
        if retry is cls._marker:
            retry = base.retry
        return cls(base, publisher_id, retry=retry)
