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
import sys

from oslo_messaging import _utils as utils
from oslo_messaging import localcontext
from oslo_messaging import serializer as msg_serializer


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

    def __init__(self, targets, endpoints, serializer, allow_requeue,
                 pool=None):
        self.targets = targets
        self.endpoints = endpoints
        self.serializer = serializer or msg_serializer.NoOpSerializer()
        self.allow_requeue = allow_requeue
        self.pool = pool

        self._callbacks_by_priority = {}
        for endpoint, prio in itertools.product(endpoints, PRIORITIES):
            if hasattr(endpoint, prio):
                method = getattr(endpoint, prio)
                screen = getattr(endpoint, 'filter_rule', None)
                self._callbacks_by_priority.setdefault(prio, []).append(
                    (screen, method))

        priorities = self._callbacks_by_priority.keys()
        self._targets_priorities = set(itertools.product(self.targets,
                                                         priorities))

    def _listen(self, transport):
        return transport._listen_for_notifications(self._targets_priorities,
                                                   pool=self.pool)

    def __call__(self, incoming, executor_callback=None):
        return utils.DispatcherExecutorContext(
            incoming, self._dispatch_and_handle_error,
            executor_callback=executor_callback,
            post=self._post_dispatch)

    @staticmethod
    def _post_dispatch(incoming, result):
        if result == NotificationResult.HANDLED:
            incoming.acknowledge()
        else:
            incoming.requeue()

    def _dispatch_and_handle_error(self, incoming, executor_callback):
        """Dispatch a notification message to the appropriate endpoint method.

        :param incoming: the incoming notification message
        :type ctxt: IncomingMessage
        """
        try:
            return self._dispatch(incoming.ctxt, incoming.message,
                                  executor_callback)
        except Exception:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            LOG.error('Exception during message handling',
                      exc_info=exc_info)
            return NotificationResult.HANDLED

    def _dispatch(self, ctxt, message, executor_callback=None):
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

        for screen, callback in self._callbacks_by_priority.get(priority, []):
            if screen and not screen.match(ctxt, publisher_id, event_type,
                                           metadata, payload):
                continue
            localcontext._set_local_context(ctxt)
            try:
                if executor_callback:
                    ret = executor_callback(callback, ctxt, publisher_id,
                                            event_type, payload, metadata)
                else:
                    ret = callback(ctxt, publisher_id, event_type, payload,
                                   metadata)
                ret = NotificationResult.HANDLED if ret is None else ret
                if self.allow_requeue and ret == NotificationResult.REQUEUE:
                    return ret
            finally:
                localcontext._clear_local_context()
        return NotificationResult.HANDLED
