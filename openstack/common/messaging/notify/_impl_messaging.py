
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

from openstack.common.gettextutils import _
from openstack.common import log as logging
from openstack.common import messaging
from openstack.common.messaging.notify import notifier

LOG = logging.getLogger(__name__)


class MessagingDriver(notifier._Driver):

    def __init__(self, conf, topics, transport, envelope=False):
        super(MessagingDriver, self).__init__(conf, topics, transport)
        self.envelope = envelope

    def notify(self, context, message, priority):
        for topic in self.topics:
            target = messaging.Target(topic='%s.%s' % (topic, priority))
            try:
                self.transport._send(target, context, message,
                                     envelope=self.envelope)
            except Exception:
                LOG.exception(_("Could not send notification to %(topic)s. "
                                "Payload=%(message)s"),
                              dict(topic=topic, message=message))


class MessagingV2Driver(MessagingDriver):

    def __init__(self, conf, **kwargs):
        super(MessagingDriver, self).__init__(conf, envelope=True, **kwargs)
