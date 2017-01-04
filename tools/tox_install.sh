#!/usr/bin/env bash

# Client constraint file contains this client version pin that is in conflict
# with installing the client from source. We should remove the version pin in
# the constraints file before applying it for from-source installation.

CONSTRAINTS_FILE=$1
shift 1

set -e

# NOTE(tonyb): Place this in the tox enviroment's log dir so it will get
# published to logs.openstack.org for easy debugging.
localfile="$VIRTUAL_ENV/log/upper-constraints.txt"

if [[ $CONSTRAINTS_FILE != http* ]]; then
    CONSTRAINTS_FILE=file://$CONSTRAINTS_FILE
fi
# NOTE(tonyb): need to add curl to bindep.txt if the project supports bindep
curl $CONSTRAINTS_FILE --insecure --progress-bar --output $localfile

pip install -c$localfile openstack-requirements

# This is the main purpose of the script: Allow local installation of
# the current repo. It is listed in constraints file and thus any
# install will be constrained and we need to unconstrain it.
edit-constraints $localfile -- $CLIENT_NAME

pip install -c$localfile -U $*
# NOTE(sileht) temporary overrided since requirements repo cap it to <1.0.0
# due to monasca project that have some concern with newer version.
# The driver is currently experimental, python-kafka<1.0.0 API have major issue
# that can't make the oslo.messaging driver works, so we prefer having a working
# driver with a non-synced dep, that the reverse
pip install -U 'kafka-python>=1.3.1'

exit $?
