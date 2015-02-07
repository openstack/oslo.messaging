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

# This script is executed inside post_test_hook function in devstack gate.

RPC_BACKEND=$1

case $RPC_BACKEND in
    amqp1|qpid)
        # Ensure authentification works before continuing, otherwise tests
        # will retries forever
        sudo yum install -y qpid-tools
        qpid-config --sasl-mechanism=PLAIN -a stackqpid/secretqpid@127.0.0.1
        ;;
esac

cd $BASE/new/oslo.messaging
sudo -H -u stack tox -e py27-func-$RPC_BACKEND
