#!/bin/bash
set -e

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-ZEROMQ.XXXXX)
trap "clean_exit $DATADIR" EXIT

export ZMQ_MATCHMAKER=redis
export ZMQ_REDIS_PORT=65123
export ZMQ_IPC_DIR=${DATADIR}
export ZMQ_USE_PUB_SUB=false
export ZMQ_USE_ROUTER_PROXY=true
export ZMQ_USE_ACKS=false
export TRANSPORT_URL="zmq+${ZMQ_MATCHMAKER}://127.0.0.1:${ZMQ_REDIS_PORT}"

export ZMQ_PROXY_HOST=127.0.0.1

cat > ${DATADIR}/zmq.conf <<EOF
[DEFAULT]
transport_url=${TRANSPORT_URL}
[oslo_messaging_zmq]
rpc_zmq_ipc_dir=${ZMQ_IPC_DIR}
use_pub_sub=${ZMQ_USE_PUB_SUB}
use_router_proxy=${ZMQ_USE_ROUTER_PROXY}
rpc_use_acks=${ZMQ_USE_ACKS}

[zmq_proxy_opts]
host=${ZMQ_PROXY_HOST}
EOF

redis-server --port $ZMQ_REDIS_PORT &

oslo-messaging-zmq-proxy --debug --url ${TRANSPORT_URL} --config-file ${DATADIR}/zmq.conf > ${DATADIR}/zmq-proxy.log 2>&1 &

$*
