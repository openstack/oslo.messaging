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

import itertools
import logging
import operator

import six

from oslo_messaging._i18n import _LW
from oslo_messaging import dispatcher
from oslo_messaging import serializer as msg_serializer


LOG = logging.getLogger(__name__)

PRIORITIES = ['audit', 'debug', 'info', 'warn', 'error', 'critical', 'sample']


class NotificationResult(object):
    HANDLED = 'handled'
    REQUEUE = 'requeue'


class NotificationDispatcher(dispatcher.DispatcherBase):
    def __init__(self, endpoints, serializer):

        self.endpoints = endpoints
        self.serializer = serializer or msg_serializer.NoOpSerializer()

        self._callbacks_by_priority = {}
        for endpoint, prio in itertools.product(endpoints, PRIORITIES):
            if hasattr(endpoint, prio):
                method = getattr(endpoint, prio)
                screen = getattr(endpoint, 'filter_rule', None)
                self._callbacks_by_priority.setdefault(prio, []).append(
                    (screen, method))

    @property
    def supported_priorities(self):
        return self._callbacks_by_priority.keys()

    def dispatch(self, incoming):
        """Dispatch notification messages to the appropriate endpoint method.
        """
        priority, raw_message, message = self._extract_user_message(incoming)

        if priority not in PRIORITIES:
            LOG.warning(_LW('Unknown priority "%s"'), priority)
            return

        for screen, callback in self._callbacks_by_priority.get(priority,
                                                                []):
            if screen and not screen.match(message["ctxt"],
                                           message["publisher_id"],
                                           message["event_type"],
                                           message["metadata"],
                                           message["payload"]):
                continue

            ret = self._exec_callback(callback, message)
            if ret == NotificationResult.REQUEUE:
                return ret
        return NotificationResult.HANDLED

    def _exec_callback(self, callback, message):
        try:
            return callback(message["ctxt"],
                            message["publisher_id"],
                            message["event_type"],
                            message["payload"],
                            message["metadata"])
        except Exception:
            LOG.exception("Callback raised an exception.")
            return NotificationResult.REQUEUE

    def _extract_user_message(self, incoming):
        ctxt = self.serializer.deserialize_context(incoming.ctxt)
        message = incoming.message

        publisher_id = message.get('publisher_id')
        event_type = message.get('event_type')
        metadata = {
            'message_id': message.get('message_id'),
            'timestamp': message.get('timestamp')
        }
        priority = message.get('priority', '').lower()
        payload = self.serializer.deserialize_entity(ctxt,
                                                     message.get('payload'))
        return priority, incoming, dict(ctxt=ctxt,
                                        publisher_id=publisher_id,
                                        event_type=event_type,
                                        payload=payload,
                                        metadata=metadata)


class BatchNotificationDispatcher(NotificationDispatcher):
    """A message dispatcher which understands Notification messages.

    A MessageHandlingServer is constructed by passing a callable dispatcher
    which is invoked with a list of message dictionaries each time 'batch_size'
    messages are received or 'batch_timeout' seconds is reached.
    """

    def dispatch(self, incoming):
        """Dispatch notification messages to the appropriate endpoint method.
        """

        messages_grouped = itertools.groupby(sorted(
            (self._extract_user_message(m) for m in incoming),
            key=operator.itemgetter(0)), operator.itemgetter(0))

        requeues = set()
        for priority, messages in messages_grouped:
            __, raw_messages, messages = six.moves.zip(*messages)
            if priority not in PRIORITIES:
                LOG.warning(_LW('Unknown priority "%s"'), priority)
                continue
            for screen, callback in self._callbacks_by_priority.get(priority,
                                                                    []):
                if screen:
                    filtered_messages = [message for message in messages
                                         if screen.match(
                                             message["ctxt"],
                                             message["publisher_id"],
                                             message["event_type"],
                                             message["metadata"],
                                             message["payload"])]
                else:
                    filtered_messages = list(messages)

                if not filtered_messages:
                    continue

                ret = self._exec_callback(callback, filtered_messages)
                if ret == NotificationResult.REQUEUE:
                    requeues.update(raw_messages)
                    break
        return requeues

    def _exec_callback(self, callback, messages):
        try:
            return callback(messages)
        except Exception:
            LOG.exception("Callback raised an exception.")
            return NotificationResult.REQUEUE
