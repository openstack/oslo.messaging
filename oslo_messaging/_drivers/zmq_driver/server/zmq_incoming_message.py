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

from oslo_messaging._drivers import base


class ZmqIncomingMessage(base.RpcIncomingMessage):
    """Base class for RPC-messages via ZMQ-driver.
    Behaviour of messages is fully defined by consumers
    which produced them from obtained raw data.
    """

    def __init__(self, context, message, **kwargs):
        super(ZmqIncomingMessage, self).__init__(context, message)
        self._reply_method = kwargs.pop('reply_method',
                                        lambda self, reply, failure: None)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def acknowledge(self):
        """Acknowledge is not supported."""

    def reply(self, reply=None, failure=None):
        self._reply_method(self, reply=reply, failure=failure)

    def requeue(self):
        """Requeue is not supported."""
