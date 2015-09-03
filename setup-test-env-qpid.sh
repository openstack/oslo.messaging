#!/bin/bash
#
# Usage: setup-test-env-qpid.sh PROTOCOL <command to run>
# where PROTOCOL is the version of the AMQP protocol to use with
# qpidd.  Valid values for PROTOCOL are "1", "1.0", "0-10", "0.10"
set -e

# require qpidd, qpid-tools sasl2-bin/cyrus-sasl-plain+cyrus-sasl-lib

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-QPID.XXXXX)
trap "clean_exit $DATADIR" EXIT

QPIDD=$(which qpidd 2>/dev/null)

# which protocol should be used with qpidd?
# 1 for AMQP 1.0, 0.10 for AMQP 0.10
#
PROTOCOL=$1
case $PROTOCOL in
    "1" | "1.0")
        PROTOCOL="1"
        shift
        ;;
    "0.10" | "0-10")
        PROTOCOL="0-10"
        shift
        ;;
    *)
        # assume the old protocol
        echo "No protocol specified, assuming 0.10"
        PROTOCOL="0-10"
        ;;
esac

# ensure that the version of qpidd does support AMQP 1.0
if [ $PROTOCOL == "1" ] && ! `$QPIDD --help | grep -q "queue-patterns"`; then
    echo "This version of $QPIDD does not support AMQP 1.0"
    exit 1
fi

[ -f "/usr/lib/qpid/daemon/acl.so" ] && LIBACL="load-module=/usr/lib/qpid/daemon/acl.so"

cat > ${DATADIR}/qpidd.conf <<EOF
port=65123
acl-file=${DATADIR}/qpidd.acl
sasl-config=${DATADIR}/sasl2
${LIBACL}
mgmt-enable=yes
auth=yes
log-to-stderr=no
EOF

if [ $PROTOCOL == "1" ]; then
    cat >> ${DATADIR}/qpidd.conf <<EOF
# Used by AMQP1.0 only
queue-patterns=exclusive
queue-patterns=unicast
topic-patterns=broadcast
EOF
    # some versions of qpidd require this for AMQP 1 and SASL:
    if `$QPIDD --help | grep -q "sasl-service-name"`; then
        cat >> ${DATADIR}/qpidd.conf <<EOF
sasl-service-name=amqp
EOF
    fi
fi

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
mech_list: PLAIN
EOF

echo secretqpid | saslpasswd2 -c -p -f ${DATADIR}/qpidd.sasldb -u QPID stackqpid

mkfifo ${DATADIR}/out
$QPIDD --log-enable info+ --log-to-file ${DATADIR}/out --config ${DATADIR}/qpidd.conf &
wait_for_line "Broker .*running" "error" ${DATADIR}/out

$*
