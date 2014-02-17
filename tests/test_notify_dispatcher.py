
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

import mock
import testscenarios

from oslo import messaging
from oslo.messaging.notify import dispatcher as notify_dispatcher
from oslo.messaging.openstack.common import timeutils
from tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


notification_msg = dict(
    publisher_id="publisher_id",
    event_type="compute.start",
    payload={"info": "fuu"},
    message_id="uuid",
    timestamp=str(timeutils.utcnow())
)


class TestDispatcher(test_utils.BaseTestCase):

    scenarios = [
        ('no_endpoints',
         dict(endpoints=[],
              endpoints_expect_calls=[],
              priority='info')),
        ('one_endpoints',
         dict(endpoints=[['warn']],
              endpoints_expect_calls=['warn'],
              priority='warn')),
        ('two_endpoints_only_one_match',
         dict(endpoints=[['warn'], ['info']],
              endpoints_expect_calls=[None, 'info'],
              priority='info')),
        ('two_endpoints_both_match',
         dict(endpoints=[['debug', 'info'], ['info', 'debug']],
              endpoints_expect_calls=['debug', 'debug'],
              priority='debug')),
    ]

    def test_dispatcher(self):
        endpoints = [mock.Mock(spec=endpoint_methods)
                     for endpoint_methods in self.endpoints]
        msg = notification_msg.copy()
        msg['priority'] = self.priority

        targets = [messaging.Target(topic='notifications')]
        dispatcher = notify_dispatcher.NotificationDispatcher(targets,
                                                              endpoints,
                                                              None)

        # check it listen on wanted topics
        self.assertEqual(sorted(dispatcher._targets_priorities),
                         sorted(set((targets[0], prio)
                                    for prio in itertools.chain.from_iterable(
                                        self.endpoints))))

        incoming = mock.Mock(ctxt={}, message=msg)
        with dispatcher(incoming) as callback:
            callback()

        # check endpoint callbacks are called or not
        for i, endpoint_methods in enumerate(self.endpoints):
            for m in endpoint_methods:
                if m == self.endpoints_expect_calls[i]:
                    method = getattr(endpoints[i], m)
                    expected = [mock.call({}, msg['publisher_id'],
                                          msg['event_type'],
                                          msg['payload'])]
                    self.assertEqual(method.call_args_list, expected)
                else:
                    self.assertEqual(endpoints[i].call_count, 0)

    @mock.patch('oslo.messaging.notify.dispatcher.LOG')
    def test_dispatcher_unknown_prio(self, mylog):
        msg = notification_msg.copy()
        msg['priority'] = 'what???'
        dispatcher = notify_dispatcher.NotificationDispatcher([mock.Mock()],
                                                              [mock.Mock()],
                                                              None)
        with dispatcher(mock.Mock(ctxt={}, message=msg)) as callback:
            callback()
        mylog.warning.assert_called_once_with('Unknown priority "what???"')
