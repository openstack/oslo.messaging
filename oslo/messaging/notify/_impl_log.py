
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

from oslo.messaging.notify import notifier
from oslo.messaging.openstack.common import jsonutils


class LogDriver(notifier._Driver):

    "Publish notifications via Python logging infrastructure."

    LOGGER_BASE = 'oslo.messaging.notification'

    def notify(self, ctxt, message, priority, retry):
        logger = logging.getLogger('%s.%s' % (self.LOGGER_BASE,
                                              message['event_type']))
        method = getattr(logger, priority.lower(), None)
        if method:
            method(jsonutils.dumps(message))
