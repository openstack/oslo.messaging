
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

from oslo_utils import timeutils
import testscenarios

import oslo_messaging
from oslo_messaging.notify import dispatcher as notify_dispatcher
from oslo_messaging.tests import utils as test_utils
from six.moves import mock

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
              priority='info',
              ex=None,
              return_value=oslo_messaging.NotificationResult.HANDLED)),
        ('one_endpoints',
         dict(endpoints=[['warn']],
              endpoints_expect_calls=['warn'],
              priority='warn',
              ex=None,
              return_value=oslo_messaging.NotificationResult.HANDLED)),
        ('two_endpoints_only_one_match',
         dict(endpoints=[['warn'], ['info']],
              endpoints_expect_calls=[None, 'info'],
              priority='info',
              ex=None,
              return_value=oslo_messaging.NotificationResult.HANDLED)),
        ('two_endpoints_both_match',
         dict(endpoints=[['debug', 'info'], ['info', 'debug']],
              endpoints_expect_calls=['debug', 'debug'],
              priority='debug',
              ex=None,
              return_value=oslo_messaging.NotificationResult.HANDLED)),
        ('no_return_value',
         dict(endpoints=[['warn']],
              endpoints_expect_calls=['warn'],
              priority='warn',
              ex=None, return_value=None)),
        ('requeue',
         dict(endpoints=[['debug', 'warn']],
              endpoints_expect_calls=['debug'],
              priority='debug', msg=notification_msg,
              ex=None,
              return_value=oslo_messaging.NotificationResult.REQUEUE)),
        ('exception',
         dict(endpoints=[['debug', 'warn']],
              endpoints_expect_calls=['debug'],
              priority='debug', msg=notification_msg,
              ex=Exception,
              return_value=oslo_messaging.NotificationResult.HANDLED)),
    ]

    def test_dispatcher(self):
        endpoints = []
        for endpoint_methods in self.endpoints:
            e = mock.Mock(spec=endpoint_methods)
            endpoints.append(e)
            for m in endpoint_methods:
                method = getattr(e, m)
                if self.ex:
                    method.side_effect = self.ex()
                else:
                    method.return_value = self.return_value

        msg = notification_msg.copy()
        msg['priority'] = self.priority

        dispatcher = notify_dispatcher.NotificationDispatcher(endpoints, None)

        incoming = mock.Mock(ctxt={}, message=msg)

        res = dispatcher.dispatch(incoming)

        expected_res = (
            notify_dispatcher.NotificationResult.REQUEUE
            if (self.return_value ==
                notify_dispatcher.NotificationResult.REQUEUE or
                self.ex is not None)
            else notify_dispatcher.NotificationResult.HANDLED
        )

        self.assertEqual(expected_res, res)

        # check endpoint callbacks are called or not
        for i, endpoint_methods in enumerate(self.endpoints):
            for m in endpoint_methods:
                if m == self.endpoints_expect_calls[i]:
                    method = getattr(endpoints[i], m)
                    method.assert_called_once_with(
                        {},
                        msg['publisher_id'],
                        msg['event_type'],
                        msg['payload'], {
                            'timestamp': mock.ANY,
                            'message_id': mock.ANY
                        })
                else:
                    self.assertEqual(0, endpoints[i].call_count)

    @mock.patch('oslo_messaging.notify.dispatcher.LOG')
    def test_dispatcher_unknown_prio(self, mylog):
        msg = notification_msg.copy()
        msg['priority'] = 'what???'
        dispatcher = notify_dispatcher.NotificationDispatcher(
            [mock.Mock()], None)
        res = dispatcher.dispatch(mock.Mock(ctxt={}, message=msg))
        self.assertIsNone(res)
        mylog.warning.assert_called_once_with('Unknown priority "%s"',
                                              'what???')


class TestDispatcherFilter(test_utils.BaseTestCase):
    scenarios = [
        ('publisher_id_match',
         dict(filter_rule=dict(publisher_id='^compute.*'),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=True)),
        ('publisher_id_nomatch',
         dict(filter_rule=dict(publisher_id='^compute.*'),
              publisher_id='network01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        ('event_type_match',
         dict(filter_rule=dict(event_type='^instance\.create'),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=True)),
        ('event_type_nomatch',
         dict(filter_rule=dict(event_type='^instance\.delete'),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        # this is only for simulation
        ('event_type_not_string',
         dict(filter_rule=dict(event_type='^instance\.delete'),
              publisher_id='compute01.manager',
              event_type=['instance.swim', 'instance.fly'],
              context={},
              match=False)),
        ('context_match',
         dict(filter_rule=dict(context={'user': '^adm'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={'user': 'admin'},
              match=True)),
        ('context_key_missing',
         dict(filter_rule=dict(context={'user': '^adm'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={'project': 'admin'},
              metadata={},
              match=False)),
        ('metadata_match',
         dict(filter_rule=dict(metadata={'message_id': '^99'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=True)),
        ('metadata_key_missing',
         dict(filter_rule=dict(metadata={'user': '^adm'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        ('payload_match',
         dict(filter_rule=dict(payload={'state': '^active$'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=True)),
        ('payload_no_match',
         dict(filter_rule=dict(payload={'state': '^deleted$'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        ('payload_key_missing',
         dict(filter_rule=dict(payload={'user': '^adm'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        ('payload_value_none',
         dict(filter_rule=dict(payload={'virtual_size': '2048'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={},
              match=False)),
        ('mix_match',
         dict(filter_rule=dict(event_type='^instance\.create',
                               publisher_id='^compute',
                               context={'user': '^adm'}),
              publisher_id='compute01.manager',
              event_type='instance.create.start',
              context={'user': 'admin'},
              match=True)),
    ]

    def test_filters(self):
        notification_filter = oslo_messaging.NotificationFilter(
            **self.filter_rule)
        endpoint = mock.Mock(spec=['info'], filter_rule=notification_filter)

        dispatcher = notify_dispatcher.NotificationDispatcher(
            [endpoint], serializer=None)
        message = {'payload': {'state': 'active', 'virtual_size': None},
                   'priority': 'info',
                   'publisher_id': self.publisher_id,
                   'event_type': self.event_type,
                   'timestamp': '2014-03-03 18:21:04.369234',
                   'message_id': '99863dda-97f0-443a-a0c1-6ed317b7fd45'}
        incoming = mock.Mock(ctxt=self.context, message=message)
        dispatcher.dispatch(incoming)

        if self.match:
            self.assertEqual(1, endpoint.info.call_count)
        else:
            self.assertEqual(0, endpoint.info.call_count)
