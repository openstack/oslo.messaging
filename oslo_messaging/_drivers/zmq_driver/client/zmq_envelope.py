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

from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_names


class Envelope(object):

    def __init__(self, msg_type=None, message_id=None, target=None,
                 target_hosts=None, **kwargs):
        self._msg_type = msg_type
        self._message_id = message_id
        self._target = target
        self._target_hosts = target_hosts
        self._reply_id = None
        self._kwargs = kwargs

    @property
    def reply_id(self):
        return self._reply_id

    @reply_id.setter
    def reply_id(self, value):
        self._reply_id = value

    @property
    def msg_type(self):
        return self._msg_type

    @property
    def message_id(self):
        return self._message_id

    @property
    def target(self):
        return self._target

    @property
    def target_hosts(self):
        return self._target_hosts

    @property
    def is_mult_send(self):
        return self._msg_type in zmq_names.MULTISEND_TYPES

    @property
    def topic_filter(self):
        return zmq_address.target_to_subscribe_filter(self._target)

    def has(self, key):
        return key in self._kwargs

    def set(self, key, value):
        self._kwargs[key] = value

    def get(self, key):
        self._kwargs.get(key)

    def to_dict(self):
        envelope = {zmq_names.FIELD_MSG_TYPE: self._msg_type,
                    zmq_names.FIELD_MSG_ID: self._message_id,
                    zmq_names.FIELD_TARGET: self._target,
                    zmq_names.FIELD_TARGET_HOSTS: self._target_hosts}
        envelope.update({k: v for k, v in self._kwargs.items()
                         if v is not None})
        return envelope

    def __str__(self):
        return str(self.to_dict())
