# Copyright 2011 OpenStack Foundation.
# All Rights Reserved.
# Copyright 2013 eNovance
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
"""A notification listener is used to process notification messages sent by a
notifier that uses the ``messaging`` driver.

A notification listener subscribes to the topic - and optionally exchange - in
the supplied target.  Notification messages sent by notifier clients to the
target's topic/exchange are received by the listener.

If multiple listeners subscribe to the same target, the notification will be
received by only one of the listeners. The receiving listener is selected from
the group using a best-effort round-robin algorithm.

This delivery pattern can be altered somewhat by specifying a pool name for the
listener. Listeners with the same pool name behave like a subgroup within the
group of listeners subscribed to the same topic/exchange.  Each subgroup of
listeners will receive a copy of the notification to be consumed by one member
of the subgroup. Therefore, multiple copies of the notification will be
delivered - one to the group of listeners that have no pool name (if they
exist), and one to each subgroup of listeners that share the same pool name.

Note that not all transport drivers have implemented support for listener
pools. Those drivers that do not support pools will raise a NotImplementedError
if a pool name is specified to get_notification_listener().

A notification listener exposes a number of endpoints, each of which contain a
set of methods. Each method's name corresponds to a notification's priority.
When a notification is received it is dispatched to the method named like the
notification's priority - e.g. ``info`` notifications are dispatched to the
info() method, etc.

Optionally a notification endpoint can define a NotificationFilter.
Notification messages that do not match the filter's rules will *not* be passed
to the endpoint's methods.

Parameters to endpoint methods are: the request context supplied by the client,
the publisher_id of the notification message, the event_type, the payload and
metadata. The metadata parameter is a mapping containing a unique message_id
and a timestamp.

An endpoint method can explicitly return
oslo_messaging.NotificationResult.HANDLED to acknowledge a message or
oslo_messaging.NotificationResult.REQUEUE to requeue the message. Note that not
all transport drivers implement support for requeueing. In order to use this
feature, applications should assert that the feature is available by passing
allow_requeue=True to get_notification_listener(). If the driver does not
support requeueing, it will raise NotImplementedError at this point.

The message is acknowledged only if all endpoints either return
oslo_messaging.NotificationResult.HANDLED or None.

Each notification listener is associated with an executor which controls how
incoming notification messages will be received and dispatched. By default, the
most simple executor is used - the blocking executor. This executor processes
inbound notifications on the server's thread, blocking it from processing
additional notifications until it finishes with the current one. Refer to the
Executor documentation for descriptions of the other types of executors.

*Note:* If the "eventlet" executor is used, the threading and time library need
to be monkeypatched.

Notification listener have start(), stop() and wait() messages to begin
handling requests, stop handling requests, and wait for all in-process
requests to complete after the listener has been stopped.

To create a notification listener, you supply a transport, list of targets and
a list of endpoints.

A transport can be obtained simply by calling the get_notification_transport()
method::

    transport = messaging.get_notification_transport(conf)

which will load the appropriate transport driver according to the user's
messaging configuration. See get_notification_transport() for more details.

A simple example of a notification listener with multiple endpoints might be::

    from oslo_config import cfg
    import oslo_messaging


    class NotificationEndpoint(object):
        filter_rule = oslo_messaging.NotificationFilter(
            publisher_id='^compute.*')

        def warn(self, ctxt, publisher_id, event_type, payload, metadata):
            do_something(payload)


    class ErrorEndpoint(object):
        filter_rule = oslo_messaging.NotificationFilter(
            event_type='^instance\..*\.start$',
            context={'ctxt_key': 'regexp'})

        def error(self, ctxt, publisher_id, event_type, payload, metadata):
            do_something(payload)

    transport = oslo_messaging.get_notification_transport(cfg.CONF)
    targets = [
        oslo_messaging.Target(topic='notifications'),
        oslo_messaging.Target(topic='notifications_bis')
    ]
    endpoints = [
        NotificationEndpoint(),
        ErrorEndpoint(),
    ]
    pool = "listener-workers"
    server = oslo_messaging.get_notification_listener(transport, targets,
                                                      endpoints, pool=pool)
    server.start()
    server.wait()


By supplying a serializer object, a listener can deserialize a request context
and arguments from primitive types.

"""
import itertools
import logging

from oslo_messaging._i18n import _LE
from oslo_messaging.notify import dispatcher as notify_dispatcher
from oslo_messaging import server as msg_server

LOG = logging.getLogger(__name__)


class NotificationServerBase(msg_server.MessageHandlingServer):
    def __init__(self, transport, targets, dispatcher, executor='blocking',
                 allow_requeue=True, pool=None, batch_size=1,
                 batch_timeout=None):
        super(NotificationServerBase, self).__init__(transport, dispatcher,
                                                     executor)
        self._allow_requeue = allow_requeue
        self._pool = pool
        self.targets = targets
        self._targets_priorities = set(
            itertools.product(self.targets,
                              self.dispatcher.supported_priorities)
        )

        self._batch_size = batch_size
        self._batch_timeout = batch_timeout

    def _create_listener(self):
        return self.transport._listen_for_notifications(
            self._targets_priorities, self._pool, self._batch_size,
            self._batch_timeout
        )


class NotificationServer(NotificationServerBase):
    def __init__(self, transport, targets, dispatcher, executor='blocking',
                 allow_requeue=True, pool=None):
        super(NotificationServer, self).__init__(
            transport, targets, dispatcher, executor, allow_requeue, pool, 1,
            None
        )

    def _process_incoming(self, incoming):
        message = incoming[0]
        try:
            res = self.dispatcher.dispatch(message)
        except Exception:
            LOG.exception(_LE('Exception during message handling.'))
            res = notify_dispatcher.NotificationResult.REQUEUE

        try:
            if (res == notify_dispatcher.NotificationResult.REQUEUE and
                    self._allow_requeue):
                message.requeue()
            else:
                message.acknowledge()
        except Exception:
            LOG.exception(_LE("Fail to ack/requeue message."))


class BatchNotificationServer(NotificationServerBase):

    def _process_incoming(self, incoming):
        try:
            not_processed_messages = self.dispatcher.dispatch(incoming)
        except Exception:
            not_processed_messages = set(incoming)
            LOG.exception(_LE('Exception during messages handling.'))
        for m in incoming:
            try:
                if m in not_processed_messages and self._allow_requeue:
                    m.requeue()
                else:
                    m.acknowledge()
            except Exception:
                LOG.exception(_LE("Fail to ack/requeue message."))


def get_notification_listener(transport, targets, endpoints,
                              executor='blocking', serializer=None,
                              allow_requeue=False, pool=None):
    """Construct a notification listener

    The executor parameter controls how incoming messages will be received and
    dispatched. By default, the most simple executor is used - the blocking
    executor.

    If the eventlet executor is used, the threading and time library need to be
    monkeypatched.

    :param transport: the messaging transport
    :type transport: Transport
    :param targets: the exchanges and topics to listen on
    :type targets: list of Target
    :param endpoints: a list of endpoint objects
    :type endpoints: list
    :param executor: name of a message executor - for example
                     'eventlet', 'blocking'
    :type executor: str
    :param serializer: an optional entity serializer
    :type serializer: Serializer
    :param allow_requeue: whether NotificationResult.REQUEUE support is needed
    :type allow_requeue: bool
    :param pool: the pool name
    :type pool: str
    :raises: NotImplementedError
    """
    dispatcher = notify_dispatcher.NotificationDispatcher(endpoints,
                                                          serializer)
    return NotificationServer(transport, targets, dispatcher, executor,
                              allow_requeue, pool)


def get_batch_notification_listener(transport, targets, endpoints,
                                    executor='blocking', serializer=None,
                                    allow_requeue=False, pool=None,
                                    batch_size=None, batch_timeout=None):
    """Construct a batch notification listener

    The executor parameter controls how incoming messages will be received and
    dispatched. By default, the most simple executor is used - the blocking
    executor.

    If the eventlet executor is used, the threading and time library need to be
    monkeypatched.

    :param transport: the messaging transport
    :type transport: Transport
    :param targets: the exchanges and topics to listen on
    :type targets: list of Target
    :param endpoints: a list of endpoint objects
    :type endpoints: list
    :param executor: name of a message executor - for example
                     'eventlet', 'blocking'
    :type executor: str
    :param serializer: an optional entity serializer
    :type serializer: Serializer
    :param allow_requeue: whether NotificationResult.REQUEUE support is needed
    :type allow_requeue: bool
    :param pool: the pool name
    :type pool: str
    :param batch_size: number of messages to wait before calling
                       endpoints callacks
    :type batch_size: int
    :param batch_timeout: number of seconds to wait before calling
                       endpoints callacks
    :type batch_timeout: int
    :raises: NotImplementedError
    """
    dispatcher = notify_dispatcher.BatchNotificationDispatcher(
        endpoints, serializer)
    return BatchNotificationServer(
        transport, targets, dispatcher, executor, allow_requeue, pool,
        batch_size, batch_timeout
    )
