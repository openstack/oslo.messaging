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

from oslo_messaging._drivers.zmq_driver import zmq_async

zmq = zmq_async.import_zmq()


ZMQ_SOCKET_STR = {zmq.DEALER: "DEALER",
                  zmq.ROUTER: "ROUTER",
                  zmq.PUSH: "PUSH",
                  zmq.PULL: "PULL",
                  zmq.REQ: "REQ",
                  zmq.REP: "REP",
                  zmq.PUB: "PUB",
                  zmq.SUB: "SUB"}

FIELD_FAILURE = 'failure'
FIELD_REPLY = 'reply'
FIELD_LOG_FAILURE = 'log_failure'
FIELD_ID = 'id'

CALL_TYPE = 'call'
CAST_TYPE = 'cast'
CAST_FANOUT_TYPE = 'cast-f'
NOTIFY_TYPE = 'notify'
NOTIFY_FANOUT_TYPE = 'notify-f'

MESSAGE_TYPES = (CALL_TYPE,
                 CAST_TYPE,
                 CAST_FANOUT_TYPE,
                 NOTIFY_TYPE,
                 NOTIFY_FANOUT_TYPE)

MULTISEND_TYPES = (CAST_FANOUT_TYPE, NOTIFY_FANOUT_TYPE)
DIRECT_TYPES = (CALL_TYPE, CAST_TYPE, NOTIFY_TYPE)
CAST_TYPES = (CAST_TYPE, CAST_FANOUT_TYPE)
NOTIFY_TYPES = (NOTIFY_TYPE, NOTIFY_FANOUT_TYPE)


def socket_type_str(socket_type):
    return ZMQ_SOCKET_STR[socket_type]
