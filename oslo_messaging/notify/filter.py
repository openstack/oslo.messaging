#
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

import re

import six


class NotificationFilter(object):

    """Filter notification messages

    The NotificationFilter class is used to filter notifications that an
    endpoint will received.

    The notification can be filter on different fields: context,
    publisher_id, event_type, metadata and payload.

    The filter is done via a regular expression

    filter_rule =  NotificationFilter(
         publisher_id='^compute.*',
         context={'tenant_id': '^5f643cfc-664b-4c69-8000-ce2ed7b08216$',
                  'roles': 'private'},
         event_type='^compute\.instance\..*',
         metadata={'timestamp': 'Aug'},
         payload={'state': '^active$')

    """

    def __init__(self, context=None, publisher_id=None, event_type=None,
                 metadata=None, payload=None):
        self._regex_publisher_id = None
        self._regex_event_type = None

        if publisher_id is not None:
            self._regex_publisher_id = re.compile(publisher_id)
        if event_type is not None:
            self._regex_event_type = re.compile(event_type)
        self._regexs_context = self._build_regex_dict(context)
        self._regexs_metadata = self._build_regex_dict(metadata)
        self._regexs_payload = self._build_regex_dict(payload)

    @staticmethod
    def _build_regex_dict(regex_list):
        if regex_list is None:
            return {}
        return dict((k, re.compile(regex_list[k])) for k in regex_list)

    @staticmethod
    def _check_for_single_mismatch(data, regex):
        if regex is None:
            return False
        if not isinstance(data, six.string_types):
            return True
        if not regex.match(data):
            return True
        return False

    @classmethod
    def _check_for_mismatch(cls, data, regex):
        if isinstance(regex, dict):
            for k in regex:
                if k not in data:
                    return True
                if cls._check_for_single_mismatch(data[k], regex[k]):
                    return True
            return False
        else:
            return cls._check_for_single_mismatch(data, regex)

    def match(self, context, publisher_id, event_type, metadata, payload):
        if (self._check_for_mismatch(publisher_id, self._regex_publisher_id) or
                self._check_for_mismatch(event_type, self._regex_event_type) or
                self._check_for_mismatch(context, self._regexs_context) or
                self._check_for_mismatch(metadata, self._regexs_metadata) or
                self._check_for_mismatch(payload, self._regexs_payload)):
            return False
        return True
