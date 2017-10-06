#!/bin/bash -xe

# This script will be run by OpenStack CI before unit tests are run,
# it sets up the test system as needed.
# Developer should setup their test systems in a similar way.

# This setup needs to be run by a user that can run sudo.

function is_fedora {
    [ -f /usr/bin/yum ] && cat /etc/*release | grep -q -e "Fedora"
}

# NOTE(sileht): we create the virtualenv only and use bindep directly
# because tox doesn't have a quiet option...
tox -ebindep --notest

# TODO(kgiusti) for now install all profile deps, need to fix this to
# install only the deps needed by the particular test's profile
PROFILES="rabbit zmq amqp1"
PACKAGES=
for PROFILE in $PROFILES; do
    # NOTE(sileht): bindep return 1 if some packages have to be installed
    PACKAGES="$PACKAGES $(.tox/bindep/bin/bindep -b -f bindep.txt $PROFILE || true)"
done
[ -n "$PACKAGES" ] || exit 0

# inspired from project-config install-distro-packages.sh
if apt-get -v >/dev/null 2>&1 ; then
    sudo add-apt-repository -y ppa:qpid/testing
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
