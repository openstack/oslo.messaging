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
use_router_proxy=True
[matchmaker_redis]
port=${ZMQ_REDIS_PORT}
EOF

redis-server --port $ZMQ_REDIS_PORT &

oslo-messaging-zmq-proxy --config-file ${DATADIR}/zmq.conf > ${DATADIR}/zmq-publisher.log 2>&1 &

$*
