#!/bin/bash -xe

# This script will be run by OpenStack CI before unit tests are run,
# it sets up the test system as needed.
# Developer should setup their test systems in a similar way.

# This setup for amqp1 needs to be run by a user that can run sudo.

# qdrouterd needs to be installed from qpid/testing repo in Ubuntu.
# bindep does not allow setting up another repo, so we just install
# this package here.

# inspired from project-config install-distro-packages.sh
if apt-get -v >/dev/null 2>&1 ; then
    sudo add-apt-repository -y ppa:qpid/testing
    sudo apt-get -qq update
    sudo PATH=/usr/sbin:/sbin:$PATH DEBIAN_FRONTEND=noninteractive \
        apt-get -q --option "Dpkg::Options::=--force-confold" \
        --assume-yes install qdrouterd
fi
