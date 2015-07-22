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
