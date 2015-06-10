#!/bin/bash
set -e

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-ZEROMQ.XXXXX)
trap "clean_exit $DATADIR" EXIT

export TRANSPORT_URL=zmq://
export ZMQ_MATCHMAKER=redis
export ZMQ_REDIS_PORT=65123
export ZMQ_IPC_DIR=${DATADIR}

cat > ${DATADIR}/zmq.conf <<EOF
[DEFAULT]
transport_url=${TRANSPORT_URL}
rpc_zmq_matchmaker=${ZMQ_MATCHMAKER}
rpc_zmq_ipc_dir=${ZMQ_IPC_DIR}
[matchmaker_redis]
port=${ZMQ_REDIS_PORT}
EOF

redis-server --port $ZMQ_REDIS_PORT &

oslo-messaging-zmq-receiver --config-file ${DATADIR}/zmq.conf > ${DATADIR}/receiver.log 2>&1 &

# FIXME(sileht): This does the same kind of setup that devstack does
# But this doesn't work yet, a zeromq maintener should take a look on that

$*
