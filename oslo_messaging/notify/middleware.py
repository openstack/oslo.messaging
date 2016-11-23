# Copyright (c) 2013-2014 eNovance
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

"""
Send notifications on request

"""
import logging
import os.path
import sys
import traceback as tb

from oslo_config import cfg
from oslo_middleware import base
import webob.dec

import oslo_messaging
from oslo_messaging._i18n import _LE
from oslo_messaging import notify

LOG = logging.getLogger(__name__)


def log_and_ignore_error(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            LOG.exception(_LE('An exception occurred processing '
                              'the API call: %s ') % e)
    return wrapped


class RequestNotifier(base.Middleware):
    """Send notification on request."""

    @classmethod
    def factory(cls, global_conf, **local_conf):
        """Factory method for paste.deploy."""
        conf = global_conf.copy()
        conf.update(local_conf)

        def _factory(app):
            return cls(app, **conf)
        return _factory

    def __init__(self, app, **conf):
        self.notifier = notify.Notifier(
            oslo_messaging.get_notification_transport(cfg.CONF,
                                                      conf.get('url')),
            publisher_id=conf.get('publisher_id',
                                  os.path.basename(sys.argv[0])))
        self.service_name = conf.get('service_name')
        self.ignore_req_list = [x.upper().strip() for x in
                                conf.get('ignore_req_list', '').split(',')]
        super(RequestNotifier, self).__init__(app)

    @staticmethod
    def environ_to_dict(environ):
        """Following PEP 333, server variables are lower case, so don't
        include them.

        """
        return dict((k, v) for k, v in environ.items()
                    if k.isupper() and k != 'HTTP_X_AUTH_TOKEN')

    @log_and_ignore_error
    def process_request(self, request):
        request.environ['HTTP_X_SERVICE_NAME'] = \
            self.service_name or request.host
        payload = {
            'request': self.environ_to_dict(request.environ),
        }

        self.notifier.info({},
                           'http.request',
                           payload)

    @log_and_ignore_error
    def process_response(self, request, response,
                         exception=None, traceback=None):
        payload = {
            'request': self.environ_to_dict(request.environ),
        }

        if response:
            payload['response'] = {
                'status': response.status,
                'headers': response.headers,
            }

        if exception:
            payload['exception'] = {
                'value': repr(exception),
                'traceback': tb.format_tb(traceback)
            }

        self.notifier.info({},
                           'http.response',
                           payload)

    @webob.dec.wsgify
    def __call__(self, req):
        if req.method in self.ignore_req_list:
            return req.get_response(self.application)
        else:
            self.process_request(req)
            try:
                response = req.get_response(self.application)
            except Exception:
                exc_type, value, traceback = sys.exc_info()
                self.process_response(req, None, value, traceback)
                raise
            else:
                self.process_response(req, response)
            return response
