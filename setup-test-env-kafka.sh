#!/bin/bash
set -e

. tools/functions.sh

SCALA_VERSION=${SCALA_VERSION:-"2.12"}
KAFKA_VERSION=${KAFKA_VERSION:-"2.0.0"}

if [[ -z "$(which kafka-server-start)" ]] && [[ -z $(which kafka-server-start.sh) ]]; then
    DATADIR=$(mktemp -d /tmp/OSLOMSG-KAFKA.XXXXX)
    trap "clean_exit $DATADIR" EXIT

    tarball=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

    wget http://archive.apache.org/dist/kafka/${KAFKA_VERSION}/$tarball -O $DATADIR/$tarball
    tar -xzf $DATADIR/$tarball -C $DATADIR
    export PATH=$DATADIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/bin:$PATH
fi

pifpaf run kafka -- $*
