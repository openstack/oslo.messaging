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
import logging
import uuid

import six

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_version
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class Request(object):

    """Zmq request abstract class

    Represents socket (publisher) independent data object to publish.
    Request object should contain all needed information for a publisher
    to publish it, for instance: message payload, target, timeout
    and retries etc.
    """

    def __init__(self, target, context=None, message=None, retry=None):

        """Construct request object

        :param target: Message destination target
        :type target: oslo_messaging.Target
        :param context: Message context
        :type context: dict
        :param message: Message payload to pass
        :type message: dict
        :param retry: an optional default connection retries configuration
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """

        if self.msg_type not in zmq_names.REQUEST_TYPES:
            raise RuntimeError("Unknown request type!")

        self.target = target
        self.context = context
        self.message = message

        self.retry = retry
        if not isinstance(retry, int) and retry is not None:
            raise ValueError(
                "retry must be an integer, not {0}".format(type(retry)))

        self.message_id = str(uuid.uuid1())

    @abc.abstractproperty
    def msg_type(self):
        """ZMQ request type"""

    @property
    def message_version(self):
        return zmq_version.MESSAGE_VERSION


class RpcRequest(Request):

    def __init__(self, *args, **kwargs):
        message = kwargs.get("message")
        if message['method'] is None:
            errmsg = _LE("No method specified for RPC call")
            LOG.error(_LE("No method specified for RPC call"))
            raise KeyError(errmsg)

        super(RpcRequest, self).__init__(*args, **kwargs)


class CallRequest(RpcRequest):

    msg_type = zmq_names.CALL_TYPE

    def __init__(self, *args, **kwargs):
        self.allowed_remote_exmods = kwargs.pop("allowed_remote_exmods")

        self.timeout = kwargs.pop("timeout")
        if self.timeout is None:
            raise ValueError("Timeout should be specified for a RPC call!")
        elif not isinstance(self.timeout, int):
            raise ValueError(
                "timeout must be an integer, not {0}"
                .format(type(self.timeout)))

        super(CallRequest, self).__init__(*args, **kwargs)


class CastRequest(RpcRequest):

    msg_type = zmq_names.CAST_TYPE


class FanoutRequest(RpcRequest):

    msg_type = zmq_names.CAST_FANOUT_TYPE


class NotificationRequest(Request):

    msg_type = zmq_names.NOTIFY_TYPE

    def __init__(self, *args, **kwargs):
        self.version = kwargs.pop("version")
        super(NotificationRequest, self).__init__(*args, **kwargs)
