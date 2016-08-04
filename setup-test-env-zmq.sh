#!/bin/bash
set -e

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-ZEROMQ.XXXXX)
trap "clean_exit $DATADIR" EXIT

export TRANSPORT_URL=zmq://
export ZMQ_MATCHMAKER=redis
export ZMQ_REDIS_PORT=65123
export ZMQ_IPC_DIR=${DATADIR}
export ZMQ_USE_PUB_SUB=false
export ZMQ_USE_ROUTER_PROXY=false

cat > ${DATADIR}/zmq.conf <<EOF
[DEFAULT]
transport_url=${TRANSPORT_URL}
[oslo_messaging_zmq]
rpc_zmq_matchmaker=${ZMQ_MATCHMAKER}
rpc_zmq_ipc_dir=${ZMQ_IPC_DIR}
use_pub_sub=${ZMQ_USE_PUB_SUB}
use_router_proxy=${ZMQ_USE_ROUTER_PROXY}
[matchmaker_redis]
port=${ZMQ_REDIS_PORT}
EOF

redis-server --port $ZMQ_REDIS_PORT &

oslo-messaging-zmq-proxy --debug True --config-file ${DATADIR}/zmq.conf > ${DATADIR}/zmq-proxy.log 2>&1 &

$*
