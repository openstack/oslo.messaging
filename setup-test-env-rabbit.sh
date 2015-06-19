#!/bin/bash
set -e

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-RABBIT.XXXXX)
trap "clean_exit $DATADIR" EXIT

export RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
export RABBITMQ_NODE_PORT=65123
export RABBITMQ_NODENAME=oslomsg-test@localhost
export RABBITMQ_LOG_BASE=$DATADIR
export RABBITMQ_MNESIA_BASE=$DATADIR
export RABBITMQ_PID_FILE=$DATADIR/pid
export HOME=$DATADIR

# NOTE(sileht): We directly use the rabbitmq scripts
# to avoid distribution check, like running as root/rabbitmq
# enforcing.
export PATH=/usr/lib/rabbitmq/bin/:$PATH


mkfifo ${DATADIR}/out
rabbitmq-server &> ${DATADIR}/out &
wait_for_line "Starting broker... completed" "ERROR:" ${DATADIR}/out

rabbitmqctl add_user oslomsg oslosecret
rabbitmqctl set_permissions "oslomsg" ".*" ".*" ".*"


export TRANSPORT_URL=rabbit://oslomsg:oslosecret@127.0.0.1:65123//
$*
