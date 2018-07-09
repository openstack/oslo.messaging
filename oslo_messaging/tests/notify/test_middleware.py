# Copyright 2013-2014 eNovance
# All Rights Reserved.
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

import uuid

import webob

from oslo_messaging.notify import middleware
from oslo_messaging.tests import utils
from six.moves import mock


class FakeApp(object):
    def __call__(self, env, start_response):
        body = 'Some response'
        start_response('200 OK', [
            ('Content-Type', 'text/plain'),
            ('Content-Length', str(sum(map(len, body))))
        ])
        return [body]


class FakeFailingApp(object):
    def __call__(self, env, start_response):
        raise Exception("It happens!")


class NotifierMiddlewareTest(utils.BaseTestCase):

    def test_notification(self):
        m = middleware.RequestNotifier(FakeApp())
        req = webob.Request.blank('/foo/bar',
                                  environ={'REQUEST_METHOD': 'GET',
                                           'HTTP_X_AUTH_TOKEN': uuid.uuid4()})
        with mock.patch(
                'oslo_messaging.notify.notifier.Notifier._notify') as notify:
            m(req)
            # Check first notification with only 'request'
            call_args = notify.call_args_list[0][0]
            self.assertEqual('http.request', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request']),
                             set(call_args[2].keys()))

            request = call_args[2]['request']
            self.assertEqual('/foo/bar', request['PATH_INFO'])
            self.assertEqual('GET', request['REQUEST_METHOD'])
            self.assertIn('HTTP_X_SERVICE_NAME', request)
            self.assertNotIn('HTTP_X_AUTH_TOKEN', request)
            self.assertFalse(any(map(lambda s: s.startswith('wsgi.'),
                                     request.keys())),
                             "WSGI fields are filtered out")

            # Check second notification with request + response
            call_args = notify.call_args_list[1][0]
            self.assertEqual('http.response', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request', 'response']),
                             set(call_args[2].keys()))

            request = call_args[2]['request']
            self.assertEqual('/foo/bar', request['PATH_INFO'])
            self.assertEqual('GET', request['REQUEST_METHOD'])
            self.assertIn('HTTP_X_SERVICE_NAME', request)
            self.assertNotIn('HTTP_X_AUTH_TOKEN', request)
            self.assertFalse(any(map(lambda s: s.startswith('wsgi.'),
                                     request.keys())),
                             "WSGI fields are filtered out")

            response = call_args[2]['response']
            self.assertEqual('200 OK', response['status'])
            self.assertEqual('13', response['headers']['content-length'])

    def test_notification_response_failure(self):
        m = middleware.RequestNotifier(FakeFailingApp())
        req = webob.Request.blank('/foo/bar',
                                  environ={'REQUEST_METHOD': 'GET',
                                           'HTTP_X_AUTH_TOKEN': uuid.uuid4()})
        with mock.patch(
                'oslo_messaging.notify.notifier.Notifier._notify') as notify:
            try:
                m(req)
                self.fail("Application exception has not been re-raised")
            except Exception:
                pass
            # Check first notification with only 'request'
            call_args = notify.call_args_list[0][0]
            self.assertEqual('http.request', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request']),
                             set(call_args[2].keys()))

            request = call_args[2]['request']
            self.assertEqual('/foo/bar', request['PATH_INFO'])
            self.assertEqual('GET', request['REQUEST_METHOD'])
            self.assertIn('HTTP_X_SERVICE_NAME', request)
            self.assertNotIn('HTTP_X_AUTH_TOKEN', request)
            self.assertFalse(any(map(lambda s: s.startswith('wsgi.'),
                                     request.keys())),
                             "WSGI fields are filtered out")

            # Check second notification with 'request' and 'exception'
            call_args = notify.call_args_list[1][0]
            self.assertEqual('http.response', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request', 'exception']),
                             set(call_args[2].keys()))

            request = call_args[2]['request']
            self.assertEqual('/foo/bar', request['PATH_INFO'])
            self.assertEqual('GET', request['REQUEST_METHOD'])
            self.assertIn('HTTP_X_SERVICE_NAME', request)
            self.assertNotIn('HTTP_X_AUTH_TOKEN', request)
            self.assertFalse(any(map(lambda s: s.startswith('wsgi.'),
                                     request.keys())),
                             "WSGI fields are filtered out")

            exception = call_args[2]['exception']
            self.assertIn('middleware.py', exception['traceback'][0])
            self.assertIn('It happens!', exception['traceback'][-1])
            self.assertTrue(exception['value'] in ("Exception('It happens!',)",
                                                   "Exception('It happens!')"))

    def test_process_request_fail(self):
        def notify_error(context, publisher_id, event_type,
                         priority, payload):
            raise Exception('error')
        with mock.patch('oslo_messaging.notify.notifier.Notifier._notify',
                        notify_error):
            m = middleware.RequestNotifier(FakeApp())
            req = webob.Request.blank('/foo/bar',
                                      environ={'REQUEST_METHOD': 'GET'})
            m.process_request(req)

    def test_process_response_fail(self):
        def notify_error(context, publisher_id, event_type,
                         priority, payload):
            raise Exception('error')
        with mock.patch('oslo_messaging.notify.notifier.Notifier._notify',
                        notify_error):
            m = middleware.RequestNotifier(FakeApp())
            req = webob.Request.blank('/foo/bar',
                                      environ={'REQUEST_METHOD': 'GET'})
            m.process_response(req, webob.response.Response())

    def test_ignore_req_opt(self):
        m = middleware.RequestNotifier(FakeApp(),
                                       ignore_req_list='get, PUT')
        req = webob.Request.blank('/skip/foo',
                                  environ={'REQUEST_METHOD': 'GET'})
        req1 = webob.Request.blank('/skip/foo',
                                   environ={'REQUEST_METHOD': 'PUT'})
        req2 = webob.Request.blank('/accept/foo',
                                   environ={'REQUEST_METHOD': 'POST'})
        with mock.patch(
                'oslo_messaging.notify.notifier.Notifier._notify') as notify:
            # Check GET request does not send notification
            m(req)
            m(req1)
            self.assertEqual(0, len(notify.call_args_list))

            # Check non-GET request does send notification
            m(req2)
            self.assertEqual(2, len(notify.call_args_list))
            call_args = notify.call_args_list[0][0]
            self.assertEqual('http.request', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request']),
                             set(call_args[2].keys()))

            request = call_args[2]['request']
            self.assertEqual('/accept/foo', request['PATH_INFO'])
            self.assertEqual('POST', request['REQUEST_METHOD'])

            call_args = notify.call_args_list[1][0]
            self.assertEqual('http.response', call_args[1])
            self.assertEqual('INFO', call_args[3])
            self.assertEqual(set(['request', 'response']),
                             set(call_args[2].keys()))
