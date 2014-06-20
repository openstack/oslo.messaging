
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

import logging

from oslo import messaging
from oslo.messaging.notify import notifier

LOG = logging.getLogger(__name__)


class MessagingDriver(notifier._Driver):

    """Send notifications using the 1.0 message format.

    This driver sends notifications over the configured messaging transport,
    but without any message envelope (also known as message format 1.0).

    This driver should only be used in cases where there are existing consumers
    deployed which do not support the 2.0 message format.
    """

    def __init__(self, conf, topics, transport, version=1.0):
        super(MessagingDriver, self).__init__(conf, topics, transport)
        self.version = version

    def notify(self, ctxt, message, priority, retry):
        priority = priority.lower()
        for topic in self.topics:
            target = messaging.Target(topic='%s.%s' % (topic, priority))
            try:
                self.transport._send_notification(target, ctxt, message,
                                                  version=self.version,
                                                  retry=retry)
            except Exception:
                LOG.exception("Could not send notification to %(topic)s. "
                              "Payload=%(message)s",
                              dict(topic=topic, message=message))


class MessagingV2Driver(MessagingDriver):

    "Send notifications using the 2.0 message format."

    def __init__(self, conf, **kwargs):
        super(MessagingV2Driver, self).__init__(conf, version=2.0, **kwargs)
