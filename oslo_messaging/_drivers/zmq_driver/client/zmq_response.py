#    Copyright 2015 Mirantis, Inc.
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

from oslo_messaging._drivers.zmq_driver import zmq_names


class Response(object):

    def __init__(self, id=None, type=None, message_id=None,
                 reply_id=None, reply_body=None,
                 failure=None, log_failure=None):

        self._id = id
        self._type = type
        self._message_id = message_id
        self._reply_id = reply_id
        self._reply_body = reply_body
        self._failure = failure
        self._log_failure = log_failure

    @property
    def id_(self):
        return self._id

    @property
    def type_(self):
        return self._type

    @property
    def message_id(self):
        return self._message_id

    @property
    def reply_id(self):
        return self._reply_id

    @property
    def reply_body(self):
        return self._reply_body

    @property
    def failure(self):
        return self._failure

    @property
    def log_failure(self):
        return self._log_failure

    def to_dict(self):
        return {zmq_names.FIELD_ID: self._id,
                zmq_names.FIELD_TYPE: self._type,
                zmq_names.FIELD_MSG_ID: self._message_id,
                zmq_names.FIELD_REPLY_ID: self._reply_id,
                zmq_names.FIELD_REPLY: self._reply_body,
                zmq_names.FIELD_FAILURE: self._failure,
                zmq_names.FIELD_LOG_FAILURE: self._log_failure}

    def __str__(self):
        return str(self.to_dict())
