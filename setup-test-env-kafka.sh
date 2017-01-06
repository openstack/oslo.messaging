#!/bin/bash
set -e

. tools/functions.sh

if [[ -z "$(which kafka-server-start)" ]] && [[ -z $(which kafka-server-start.sh) ]]; then
    DATADIR=$(mktemp -d /tmp/OSLOMSG-KAFKA.XXXXX)
    trap "clean_exit $DATADIR" EXIT

    SCALA_VERSION="2.11"
    KAFKA_VERSION="0.10.1.0"
    tarball=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

    wget http://apache.crihan.fr/dist/kafka/${KAFKA_VERSION}/$tarball -O $DATADIR/$tarball
    tar -xzf $DATADIR/$tarball -C $DATADIR
    export PATH=$DATADIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/bin:$PATH
fi

pifpaf run kafka -- $*
