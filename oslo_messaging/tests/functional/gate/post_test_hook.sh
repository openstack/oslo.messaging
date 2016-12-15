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


# NOTE(sileht): we have to rename the zmq tox target to shorter name
# this is to not break the current dsvm gating system until we switch to the tox one
case $1 in
    zeromq) RPC_BACKEND=zmq;;
    zeromq-proxy) RPC_BACKEND=zmq-proxy;;
    zeromq-pub-sub) RPC_BACKEND=zmq-pubsub;;
    *) RPC_BACKEND=$1;;
esac

PYTHON=${2:-py27}

function generate_testr_results {
    if [ -f .testrepository/0 ]; then
        sudo .tox/${PYTHON}-func-${RPC_BACKEND}/bin/testr last --subunit > $WORKSPACE/testrepository.subunit
        sudo mv $WORKSPACE/testrepository.subunit $BASE/logs/testrepository.subunit
        sudo /usr/os-testr-env/bin/subunit2html $BASE/logs/testrepository.subunit $BASE/logs/testr_results.html
        sudo gzip -9 $BASE/logs/testrepository.subunit
        sudo gzip -9 $BASE/logs/testr_results.html
        sudo chown jenkins:jenkins $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
        sudo chmod a+r $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
    fi
}

# Allow jenkins to retrieve reports
sudo chown -R jenkins:stack $BASE/new/oslo.messaging

set +e

if [ -x "$(command -v yum)" ]; then
    sudo yum install -y libuuid-devel swig pkg-config
else
    sudo apt-get update -y
    sudo apt-get install -y uuid-dev swig pkg-config
fi

# Install required packages
case $RPC_BACKEND in
    zmq|zmq-proxy|zmq-pubsub)
        sudo apt-get update -y
        sudo apt-get install -y redis-server python-redis
        ;;
    rabbit)
        sudo apt-get update -y
        sudo apt-get install -y rabbitmq-server
        ;;
esac

# Got to the oslo.messaging dir
cd $BASE/new/oslo.messaging

# Run tests
echo "Running oslo.messaging functional test suite"
# Preserve env for OS_ credentials
sudo -E -H -u jenkins tox -e ${PYTHON}-func-$RPC_BACKEND
EXIT_CODE=$?
set -e

# Collect and parse result
generate_testr_results
exit $EXIT_CODE
