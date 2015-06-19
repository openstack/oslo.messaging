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

MESSAGE_CALL_TOPIC_POSITION = 2


def _get_topic_from_msg(message, position):
    pathsep = set((os.path.sep or '', os.path.altsep or '', '/', '\\'))
    badchars = re.compile(r'[%s]' % re.escape(''.join(pathsep)))
    topic = message[position]
    topic_items = None

    if six.PY3:
        topic = topic.decode('utf-8')

    try:
        # The topic is received over the network,
        # don't trust this input.
        if badchars.search(topic) is not None:
            emsg = _LW("Topic contained dangerous characters")
            LOG.warn(emsg)
            raise rpc_common.RPCException(emsg)
        topic_items = topic.split('.', 1)
    except Exception as e:
        errmsg = _LE("Failed topic string parsing, %s") % str(e)
        LOG.error(errmsg)
        rpc_common.RPCException(errmsg)
    return topic_items[0], topic_items[1]


def get_topic_from_call_message(message):
    return _get_topic_from_msg(message, MESSAGE_CALL_TOPIC_POSITION)
