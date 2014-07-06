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
"""
A notification listener exposes a number of endpoints, each of which
contain a set of methods. Each method corresponds to a notification priority.

To create a notification listener, you supply a transport, list of targets and
a list of endpoints.

A transport can be obtained simply by calling the get_transport() method::

    transport = messaging.get_transport(conf)

which will load the appropriate transport driver according to the user's
messaging configuration configuration. See get_transport() for more details.

The target supplied when creating a notification listener expresses the topic
and - optionally - the exchange to listen on. See Target for more details
on these attributes.

Notification listener have start(), stop() and wait() messages to begin
handling requests, stop handling requests and wait for all in-process
requests to complete.

Each notification listener is associated with an executor which integrates the
listener with a specific I/O handling framework. Currently, there are blocking
and eventlet executors available.

A simple example of a notification listener with multiple endpoints might be::

    from oslo.config import cfg
    from oslo import messaging

    class NotificationEndpoint(object):
        def warn(self, ctxt, publisher_id, event_type, payload, metadata):
            do_something(payload)

    class ErrorEndpoint(object):
        def error(self, ctxt, publisher_id, event_type, payload, metadata):
            do_something(payload)

    transport = messaging.get_transport(cfg.CONF)
    targets = [
        messaging.Target(topic='notifications')
        messaging.Target(topic='notifications_bis')
    ]
    endpoints = [
        NotificationEndpoint(),
        ErrorEndpoint(),
    ]
    server = messaging.get_notification_listener(transport, targets, endpoints)
    server.start()
    server.wait()

A notifier sends a notification on a topic with a priority, the notification
listener will receive this notification if the topic of this one have been set
in one of the targets and if an endpoint implements the method named like the
priority

Parameters to endpoint methods are the request context supplied by the client,
the publisher_id of the notification message, the event_type, the payload and
metadata. The metadata parameter is a mapping containing a unique message_id
and a timestamp.

By supplying a serializer object, a listener can deserialize a request context
and arguments from - and serialize return values to - primitive types.

An endpoint method can explicitly return messaging.NotificationResult.HANDLED
to acknowledge a message or messaging.NotificationResult.REQUEUE to requeue the
message.

The message is acknowledged only if all endpoints either return
messaging.NotificationResult.HANDLED or None.

Note that not all transport drivers implement support for requeueing. In order
to use this feature, applications should assert that the feature is available
by passing allow_requeue=True to get_notification_listener(). If the driver
does not support requeueing, it will raise NotImplementedError at this point.
"""

from oslo.messaging.notify import dispatcher as notify_dispatcher
from oslo.messaging import server as msg_server


def get_notification_listener(transport, targets, endpoints,
                              executor='blocking', serializer=None,
                              allow_requeue=False):
    """Construct a notification listener

    The executor parameter controls how incoming messages will be received and
    dispatched. By default, the most simple executor is used - the blocking
    executor.

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
    :raises: NotImplementedError
    """
    transport._require_driver_features(requeue=allow_requeue)
    dispatcher = notify_dispatcher.NotificationDispatcher(targets, endpoints,
                                                          serializer,
                                                          allow_requeue)
    return msg_server.MessageHandlingServer(transport, dispatcher, executor)
