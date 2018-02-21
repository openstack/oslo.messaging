# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2011 - 2012, Red Hat, Inc.
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
Utilities for drivers based on the AMQPDriverBase.

This module contains utility code used by drivers based on the AMQPDriverBase
class. Specifically this includes the impl_rabbit driver.
"""

import collections
import uuid

from oslo_config import cfg
import six

from oslo_messaging._drivers import common as rpc_common

deprecated_durable_opts = [
    cfg.DeprecatedOpt('amqp_durable_queues',
                      group='DEFAULT'),
    cfg.DeprecatedOpt('rabbit_durable_queues',
                      group='DEFAULT')
]

amqp_opts = [
    cfg.BoolOpt('amqp_durable_queues',
                default=False,
                deprecated_opts=deprecated_durable_opts,
                help='Use durable queues in AMQP.'),
    cfg.BoolOpt('amqp_auto_delete',
                default=False,
                deprecated_group='DEFAULT',
                help='Auto-delete queues in AMQP.'),
]

UNIQUE_ID = '_unique_id'


class RpcContext(rpc_common.CommonRpcContext):
    """Context that supports replying to a rpc.call."""
    def __init__(self, **kwargs):
        self.msg_id = kwargs.pop('msg_id', None)
        self.reply_q = kwargs.pop('reply_q', None)
        super(RpcContext, self).__init__(**kwargs)

    def deepcopy(self):
        values = self.to_dict()
        values['conf'] = self.conf
        values['msg_id'] = self.msg_id
        values['reply_q'] = self.reply_q
        return self.__class__(**values)


def unpack_context(msg):
    """Unpack context from msg."""
    context_dict = {}
    for key in list(msg.keys()):
        key = six.text_type(key)
        if key.startswith('_context_'):
            value = msg.pop(key)
            context_dict[key[9:]] = value
    context_dict['msg_id'] = msg.pop('_msg_id', None)
    context_dict['reply_q'] = msg.pop('_reply_q', None)
    context_dict['client_timeout'] = msg.pop('_timeout', None)
    return RpcContext.from_dict(context_dict)


def pack_context(msg, context):
    """Pack context into msg.

    Values for message keys need to be less than 255 chars, so we pull
    context out into a bunch of separate keys. If we want to support
    more arguments in rabbit messages, we may want to do the same
    for args at some point.

    """
    if isinstance(context, dict):
        context_d = context.items()
    else:
        context_d = context.to_dict().items()

    msg.update(('_context_%s' % key, value)
               for (key, value) in context_d)


class _MsgIdCache(object):
    """This class checks any duplicate messages."""

    # NOTE: This value is considered can be a configuration item, but
    #       it is not necessary to change its value in most cases,
    #       so let this value as static for now.
    DUP_MSG_CHECK_SIZE = 16

    def __init__(self, **kwargs):
        self.prev_msgids = collections.deque([],
                                             maxlen=self.DUP_MSG_CHECK_SIZE)

    def check_duplicate_message(self, message_data):
        """AMQP consumers may read same message twice when exceptions occur
           before ack is returned. This method prevents doing it.
        """
        try:
            msg_id = message_data.pop(UNIQUE_ID)
        except KeyError:
            return
        if msg_id in self.prev_msgids:
            raise rpc_common.DuplicateMessageError(msg_id=msg_id)
        return msg_id

    def add(self, msg_id):
        if msg_id and msg_id not in self.prev_msgids:
            self.prev_msgids.append(msg_id)


def _add_unique_id(msg):
    """Add unique_id for checking duplicate messages."""
    unique_id = uuid.uuid4().hex
    msg.update({UNIQUE_ID: unique_id})


class AMQPDestinationNotFound(Exception):
    pass
