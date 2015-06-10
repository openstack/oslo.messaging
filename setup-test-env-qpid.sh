#!/bin/bash
set -e

# require qpidd, qpid-tools sasl2-bin/cyrus-sasl-plain+cyrus-sasl-lib

. tools/functions.sh

DATADIR=$(mktemp -d /tmp/OSLOMSG-QPID.XXXXX)
trap "clean_exit $DATADIR" EXIT

[ -f "/usr/lib/qpid/daemon/acl.so" ] && LIBACL="load-module=/usr/lib/qpid/daemon/acl.so"

cat > ${DATADIR}/qpidd.conf <<EOF
port=65123
acl-file=${DATADIR}/qpidd.acl
sasl-config=${DATADIR}/sasl2
log-to-file=${DATADIR}/log
${LIBACL}
mgmt-enable=yes
auth=yes

# Used by AMQP1.0 only
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
mech_list: PLAIN
EOF

echo secretqpid | saslpasswd2 -c -p -f ${DATADIR}/qpidd.sasldb -u QPID stackqpid

QPIDD=$(which qpidd 2>/dev/null)
[ ! -x $QPIDD ] && /usr/sbin/qpidd

mkfifo ${DATADIR}/out
$QPIDD --config ${DATADIR}/qpidd.conf &> ${DATADIR}/out &
wait_for_line "Broker .*running" "error" ${DATADIR}/out

# Earlier failure if qpid-config is avialable
[ -x "$(which qpid-config)" ] && qpid-config -b stackqpid/secretqpid@localhost:65123

$*
