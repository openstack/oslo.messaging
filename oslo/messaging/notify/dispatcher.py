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

import contextlib
import itertools
import logging
import sys

from oslo.messaging import localcontext
from oslo.messaging import serializer as msg_serializer


LOG = logging.getLogger(__name__)

PRIORITIES = ['audit', 'debug', 'info', 'warn', 'error', 'critical', 'sample']


class NotificationResult(object):
    HANDLED = 'handled'
    REQUEUE = 'requeue'


class NotificationDispatcher(object):
    """A message dispatcher which understands Notification messages.

    A MessageHandlingServer is constructed by passing a callable dispatcher
    which is invoked with context and message dictionaries each time a message
    is received.

    NotifcationDispatcher is one such dispatcher which pass a raw notification
    message to the endpoints
    """

    def __init__(self, targets, endpoints, serializer, allow_requeue):
        self.targets = targets
        self.endpoints = endpoints
        self.serializer = serializer or msg_serializer.NoOpSerializer()
        self.allow_requeue = allow_requeue

        self._callbacks_by_priority = {}
        for endpoint, prio in itertools.product(endpoints, PRIORITIES):
            if hasattr(endpoint, prio):
                method = getattr(endpoint, prio)
                self._callbacks_by_priority.setdefault(prio, []).append(method)

        priorities = self._callbacks_by_priority.keys()
        self._targets_priorities = set(itertools.product(self.targets,
                                                         priorities))

    def _listen(self, transport):
        return transport._listen_for_notifications(self._targets_priorities)

    @contextlib.contextmanager
    def __call__(self, incoming):
        result_wrapper = []

        yield lambda: result_wrapper.append(
            self._dispatch_and_handle_error(incoming))

        if result_wrapper[0] == NotificationResult.HANDLED:
            incoming.acknowledge()
        else:
            incoming.requeue()

    def _dispatch_and_handle_error(self, incoming):
        """Dispatch a notification message to the appropriate endpoint method.

        :param incoming: the incoming notification message
        :type ctxt: IncomingMessage
        """
        try:
            return self._dispatch(incoming.ctxt, incoming.message)
        except Exception:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            LOG.error('Exception during message handling',
                      exc_info=exc_info)
            return NotificationResult.HANDLED

    def _dispatch(self, ctxt, message):
        """Dispatch an RPC message to the appropriate endpoint method.

        :param ctxt: the request context
        :type ctxt: dict
        :param message: the message payload
        :type message: dict
        """
        ctxt = self.serializer.deserialize_context(ctxt)

        publisher_id = message.get('publisher_id')
        event_type = message.get('event_type')
        metadata = {
            'message_id': message.get('message_id'),
            'timestamp': message.get('timestamp')
        }
        priority = message.get('priority', '').lower()
        if priority not in PRIORITIES:
            LOG.warning('Unknown priority "%s"', priority)
            return

        payload = self.serializer.deserialize_entity(ctxt,
                                                     message.get('payload'))

        for callback in self._callbacks_by_priority.get(priority, []):
            localcontext.set_local_context(ctxt)
            try:
                ret = callback(ctxt, publisher_id, event_type, payload,
                               metadata)
                ret = NotificationResult.HANDLED if ret is None else ret
                if self.allow_requeue and ret == NotificationResult.REQUEUE:
                    return ret
            finally:
                localcontext.clear_local_context()
        return NotificationResult.HANDLED
