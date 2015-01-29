#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This script is executed inside gate_hook function in devstack gate.

RPC_BACKEND=$1

DEVSTACK_LOCAL_CONFIG=$'QPID_USERNAME=stackqpid\n'
DEVSTACK_LOCAL_CONFIG+=$'QPID_PASSWORD=secretqpid\n'
DEVSTACK_LOCAL_CONFIG+=$'RABBIT_USERID=stackrabbit\n'
DEVSTACK_LOCAL_CONFIG+=$'RABBIT_PASSWORD=secretrabbit\n'

case $RPC_BACKEND in
    qpid)
        export DEVSTACK_GATE_QPID=1
        ;;
    amqp1)
        export DEVSTACK_GATE_QPID=1
        DEVSTACK_LOCAL_CONFIG+=$'RPC_MESSAGING_PROTOCOL=AMQP1\n'
        ;;
esac

export DEVSTACK_LOCAL_CONFIG
export KEEP_LOCALRC=1

$BASE/new/devstack-gate/devstack-vm-gate.sh
