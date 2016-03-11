
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
import warnings

from oslo_serialization import jsonutils
from oslo_utils import strutils

from oslo_messaging.notify import notifier


class LogDriver(notifier.Driver):

    "Publish notifications via Python logging infrastructure."

    # NOTE(dhellmann): For backwards-compatibility with configurations
    # that may have modified the settings for this logger using a
    # configuration file, we keep the name
    # 'oslo.messaging.notification' even though the package is now
    # 'oslo_messaging'.
    LOGGER_BASE = 'oslo.messaging.notification'

    def notify(self, ctxt, message, priority, retry):
        logger = logging.getLogger('%s.%s' % (self.LOGGER_BASE,
                                              message['event_type']))
        method = getattr(logger, priority.lower(), None)
        if method:
            method(jsonutils.dumps(strutils.mask_dict_password(message)))
        else:
            warnings.warn('Unable to log message as notify cannot find a '
                          'logger with the priority specified '
                          '%s' % priority.lower())
