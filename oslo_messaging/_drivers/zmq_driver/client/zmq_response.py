#    Copyright 2015-2016 Mirantis, Inc.
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

import abc

import six

from oslo_messaging._drivers.zmq_driver import zmq_names


@six.add_metaclass(abc.ABCMeta)
class Response(object):

    def __init__(self, message_id=None, reply_id=None, message_version=None):
        if self.msg_type not in zmq_names.RESPONSE_TYPES:
            raise RuntimeError("Unknown response type!")

        self._message_id = message_id
        self._reply_id = reply_id
        self._message_version = message_version

    @abc.abstractproperty
    def msg_type(self):
        """ZMQ response type"""

    @property
    def message_id(self):
        return self._message_id

    @property
    def reply_id(self):
        return self._reply_id

    @property
    def message_version(self):
        return self._message_version

    def to_dict(self):
        return {zmq_names.FIELD_MSG_ID: self._message_id,
                zmq_names.FIELD_REPLY_ID: self._reply_id,
                zmq_names.FIELD_MSG_VERSION: self._message_version}

    def __str__(self):
        return str(self.to_dict())


class Ack(Response):

    msg_type = zmq_names.ACK_TYPE


class Reply(Response):

    msg_type = zmq_names.REPLY_TYPE

    def __init__(self, message_id=None, reply_id=None, message_version=None,
                 reply_body=None, failure=None):
        super(Reply, self).__init__(message_id, reply_id, message_version)
        self._reply_body = reply_body
        self._failure = failure

    @property
    def reply_body(self):
        return self._reply_body

    @property
    def failure(self):
        return self._failure

    def to_dict(self):
        dict_ = super(Reply, self).to_dict()
        dict_.update({zmq_names.FIELD_REPLY_BODY: self._reply_body,
                      zmq_names.FIELD_FAILURE: self._failure})
        return dict_
