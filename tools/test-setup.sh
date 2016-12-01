#!/bin/bash -xe

# This script will be run by OpenStack CI before unit tests are run,
# it sets up the test system as needed.
# Developer should setup their test systems in a similar way.

# This setup needs to be run by a user that can run sudo.

function is_fedora {
    [ -f /usr/bin/yum ] && cat /etc/*release | grep -q -e "Fedora"
}

# Looks like /home/jenkins/worspace/gate-oslo.messaging-tox-py27-func-rabbit-ubuntu-xenial
JOB_NAME=${WORKSPACE##*/}

if [ "${JOB_NAME//gate-oslo.messaging-tox-py*-func-/}" == "${JOB_NAME}" ]; then
    # not a functional test
    exit 0
fi

BINDEP_PROFILE=$(echo $JOB_NAME | cut -d- -f6)
PACKAGES=$(tox -e bindep -- -b -f bindep.txt $BINDEP_PROFILE)

# inspired from project-config install-distro-packages.sh
if apt-get -v >/dev/null 2>&1 ; then
    sudo apt-get -qq update
    sudo PATH=/usr/sbin:/sbin:$PATH DEBIAN_FRONTEND=noninteractive \
        apt-get -q --option "Dpkg::Options::=--force-confold" \
        --assume-yes install $PACKAGES
elif emerge --version >/dev/null 2>&1 ; then
    sudo emerge -uDNq --jobs=4 @world
    sudo PATH=/usr/sbin:/sbin:$PATH emerge -q --jobs=4 $PACKAGES
else
    is_fedora && YUM=dnf || YUM=yum
    sudo PATH=/usr/sbin:/sbin:$PATH $YUM install -y $PACKAGES
fi
