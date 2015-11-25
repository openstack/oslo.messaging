# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging

from oslo_config import cfg


class LoggingErrorNotificationHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        # NOTE(dhellmann): Avoid a cyclical import by doing this one
        # at runtime.
        import oslo_messaging
        logging.Handler.__init__(self, *args, **kwargs)
        self._transport = oslo_messaging.get_notification_transport(cfg.CONF)
        self._notifier = oslo_messaging.Notifier(
            self._transport,
            publisher_id='error.publisher')

    def emit(self, record):
        conf = self._transport.conf
        # NOTE(bnemec): Notifier registers this opt with the transport.
        if ('log' in conf.oslo_messaging_notifications.driver):
            # NOTE(lbragstad): If we detect that log is one of the
            # notification drivers, then return. This protects from infinite
            # recursion where something bad happens, it gets logged, the log
            # handler sends a notification, and the log_notifier sees the
            # notification and logs it.
            return
        self._notifier.error({},
                             'error_notification',
                             dict(error=record.msg))


PublishErrorsHandler = LoggingErrorNotificationHandler
