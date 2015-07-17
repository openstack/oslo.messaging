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

import logging
import os
import re

import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._i18n import _LE, _LW

LOG = logging.getLogger(__name__)

MESSAGE_CALL_TYPE_POSITION = 1
MESSAGE_CALL_TARGET_POSITION = 2
MESSAGE_CALL_TOPIC_POSITION = 3

FIELD_FAILURE = 'failure'
FIELD_REPLY = 'reply'
FIELD_LOG_FAILURE = 'log_failure'

CALL_TYPE = 'call'
CAST_TYPE = 'cast'
FANOUT_TYPE = 'fanout'
NOTIFY_TYPE = 'notify'

MESSAGE_TYPES = (CALL_TYPE, CAST_TYPE, FANOUT_TYPE, NOTIFY_TYPE)


def get_msg_type(message):
    type = message[MESSAGE_CALL_TYPE_POSITION]
    if type not in MESSAGE_TYPES:
        errmsg = _LE("Unknown message type: %s") % str(type)
        LOG.error(errmsg)
        rpc_common.RPCException(errmsg)
    return type


def _get_topic_from_msg(message, position):
    pathsep = set((os.path.sep or '', os.path.altsep or '', '/', '\\'))
    badchars = re.compile(r'[%s]' % re.escape(''.join(pathsep)))

    if len(message) < position + 1:
        errmsg = _LE("Message did not contain a topic")
        LOG.error("%s: %s" % (errmsg, message))
        raise rpc_common.RPCException("%s: %s" % (errmsg, message))

    topic = message[position]

    if six.PY3:
        topic = topic.decode('utf-8')

    # The topic is received over the network, don't trust this input.
    if badchars.search(topic) is not None:
        errmsg = _LW("Topic contained dangerous characters")
        LOG.warn("%s: %s" % (errmsg, topic))
        raise rpc_common.RPCException("%s: %s" % (errmsg, topic))

    topic_items = topic.split('.', 1)

    if len(topic_items) != 2:
        errmsg = _LE("Topic was not formatted correctly")
        LOG.error("%s: %s" % (errmsg, topic))
        raise rpc_common.RPCException("%s: %s" % (errmsg, topic))

    return topic_items[0], topic_items[1]


def get_topic_from_call_message(message):
    """Extract topic and server from message.

    :param message: A message
    :type message: list

    :returns: (topic: str, server: str)
    """
    return _get_topic_from_msg(message, MESSAGE_CALL_TOPIC_POSITION)


def get_target_from_call_message(message):
    """Extract target from message.

    :param message: A message
    :type message: list

    :returns: target: Target
    """
    return message[MESSAGE_CALL_TARGET_POSITION]
