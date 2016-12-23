#!/bin/bash
#
# Usage: setup-test-env-amqp.sh <command to run>
# where AMQP1_BACKEND is the AMQP 1.0 intermediary to use. Valid
# values are "qdrouterd" for router and "qpidd" for broker.
set -e

# router requires qdrouterd, sasl2-bin/cyrus-sasl-plain+cyrus-sasl-lib
# broker requires qpidd, qpid-tools sasl2-bin/cyrus-sasl-plain+cyrus-sasl-lib

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-${AMQP1_BACKEND}.XXXXX)
trap "clean_exit $DATADIR" EXIT

function _setup_qdrouterd_user {
    echo secretqpid | saslpasswd2 -c -p -f ${DATADIR}/qdrouterd.sasldb stackqpid
}

function _setup_qpidd_user {
    echo secretqpid | saslpasswd2 -c -p -f ${DATADIR}/qpidd.sasldb -u QPID stackqpid
}

function _configure_qdrouterd {
    # create a stand alone router
    cat > ${DATADIR}/qdrouterd.conf <<EOF
router {
    mode: standalone
    id: Router.A
    workerThreads: 4
    saslConfigPath: ${DATADIR}/sasl2
    saslConfigName: qdrouterd
}

EOF

    # create a listener for incoming connect to the router
    cat >> ${DATADIR}/qdrouterd.conf <<EOF
listener {
    addr: 0.0.0.0
    port: 65123
    role: normal
    authenticatePeer: yes
}

EOF
    # create fixed address prefixes
    cat >> ${DATADIR}/qdrouterd.conf <<EOF
address {
    prefix: unicast
    distribution: closest
}

address {
    prefix: exclusive
    distribution: closest
}

address {
    prefix: broadcast
    distribution: multicast
}

address {
    prefix: openstack.org/om/rpc/multicast
    distribution: multicast
}

address {
    prefix: openstack.org/om/rpc/unicast
    distribution: closest
}

address {
    prefix: openstack.org/om/rpc/anycast
    distribution: balanced
}

address {
    prefix: openstack.org/om/notify/multicast
    distribution: multicast
}

address {
    prefix: openstack.org/om/notify/unicast
    distribution: closest
}

address {
    prefix: openstack.org/om/notify/anycast
    distribution: balanced
}

EOF

    # create log file configuration
    cat >> ${DATADIR}/qdrouterd.conf <<EOF
log {
    module: DEFAULT
    enable: trace+
    output: ${DATADIR}/out
}

EOF
    # sasl2 config
    mkdir -p ${DATADIR}/sasl2
    cat > ${DATADIR}/sasl2/qdrouterd.conf <<EOF
pwcheck_method: auxprop
auxprop_plugin: sasldb
sasldb_path: ${DATADIR}/qdrouterd.sasldb
mech_list: PLAIN ANONYMOUS
EOF

}

function _configure_qpidd {

    QPIDD=$(which qpidd 2>/dev/null)
    if [[ ! -x "$QPIDD" ]]; then
        echo "FAILURE: qpidd broker not installed"
        exit 1
    fi

    [ -f "/usr/lib/qpid/daemon/acl.so" ] && LIBACL="load-module=/usr/lib/qpid/daemon/acl.so"

    cat > ${DATADIR}/qpidd.conf <<EOF
port=65123
sasl-config=${DATADIR}/sasl2
${LIBACL}
mgmt-enable=yes
log-to-stderr=no
data-dir=${DATADIR}/.qpidd
pid-dir=${DATADIR}/.qpidd
EOF

if ! `$QPIDD --help | grep -q "sasl-service-name"`; then
    echo "This version of $QPIDD does not support SASL authentication with AMQP 1.0"
    cat >> ${DATADIR}/qpidd.conf <<EOF
auth=no
EOF
else
    cat >> ${DATADIR}/qpidd.conf <<EOF
auth=yes
acl-file=${DATADIR}/qpidd.acl
sasl-service-name=amqp
EOF
fi

    cat >> ${DATADIR}/qpidd.conf <<EOF
queue-patterns=exclusive
queue-patterns=unicast
topic-patterns=broadcast
EOF

    cat > ${DATADIR}/qpidd.acl <<EOF
group admin stackqpid@QPID
acl allow admin all
acl deny all all
EOF

    mkdir -p ${DATADIR}/sasl2
    cat > ${DATADIR}/sasl2/qpidd.conf <<EOF
pwcheck_method: auxprop
auxprop_plugin: sasldb
sasldb_path: ${DATADIR}/qpidd.sasldb
mech_list: PLAIN ANONYMOUS
sql_select: dummy select
EOF

}

function _start_qdrouterd {
    MAJOR=$(python -c 'import sys; print sys.version_info.major')
    MINOR=$(python -c 'import sys; print sys.version_info.minor')
    # qdrouterd needs access to global site packages
    # create path file and place in virtual env working directory
    SITEDIR=${WORKDIR}/${ENVNAME}/lib/python${MAJOR}.${MINOR}/site-packages
    cat > ${SITEDIR}/dispatch.pth <<EOF
/usr/lib/python${MAJOR}.${MINOR}/site-packages
EOF

    QDR=$(which qdrouterd 2>/dev/null)
    mkfifo ${DATADIR}/out
    $QDR --config ${DATADIR}/qdrouterd.conf &
    wait_for_line "Router .*started" "error" ${DATADIR}/out
    rm ${SITEDIR}/dispatch.pth
}

function _start_qpidd {
    chmod -R a+r ${DATADIR}
    QPIDD=$(which qpidd 2>/dev/null)
    mkfifo ${DATADIR}/out
    $QPIDD --log-enable trace+ --log-to-file ${DATADIR}/out --config ${DATADIR}/qpidd.conf &
    wait_for_line "Broker .*running" "error" ${DATADIR}/out
}

_configure_${AMQP1_BACKEND}
_setup_${AMQP1_BACKEND}_user
_start_${AMQP1_BACKEND}

$*
