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

"""
Notification drivers for sending notifications via messaging.

The messaging drivers publish notification messages to notification
listeners.

In case of the rabbit backend the driver will block the notifier's thread
until the notification message has been passed to the messaging transport.
There is no guarantee that the notification message will be consumed by a
notification listener.

In case of the kafka backend the driver will not block the notifier's thread
but return immediately. The driver will try to deliver the message in the
background.

Notification messages are sent 'at-most-once' - ensuring that they are not
duplicated.

If the connection to the messaging service is not active when a notification is
sent the rabbit backend will block waiting for the connection to complete.
If the connection fails to complete, the driver will try to re-establish that
connection. By default this will continue indefinitely until the connection
completes. However, the retry parameter can be used to have the notification
send fail. In this case an error is logged and the notifier's thread is resumed
without any error.

If the connection to the messaging service is not active when a notification is
sent the kafka backend will return immediately and the backend tries to
establish the connection and deliver the messages in the background.

"""

import logging

import oslo_messaging
from oslo_messaging.notify import notifier

LOG = logging.getLogger(__name__)


class MessagingDriver(notifier.Driver):

    """Send notifications using the 1.0 message format.

    This driver sends notifications over the configured messaging transport,
    but without any message envelope (also known as message format 1.0).

    This driver should only be used in cases where there are existing consumers
    deployed which do not support the 2.0 message format.
    """

    def __init__(self, conf, topics, transport, version=1.0):
        super().__init__(conf, topics, transport)
        self.version = version

    def notify(self, ctxt, message, priority, retry):
        priority = priority.lower()
        for topic in self.topics:
            target = oslo_messaging.Target(
                topic='{}.{}'.format(topic, priority))
            try:
                self.transport._send_notification(target, ctxt, message,
                                                  version=self.version,
                                                  retry=retry)
            except Exception:
                LOG.exception("Could not send notification to %(topic)s. "
                              "Payload=%(message)s",
                              {'topic': topic, 'message': message})


class MessagingV2Driver(MessagingDriver):

    "Send notifications using the 2.0 message format."

    def __init__(self, conf, **kwargs):
        super().__init__(conf, version=2.0, **kwargs)
