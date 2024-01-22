#!/bin/bash
set -e

. tools/functions.sh

SCENARIO=${SCENARIO:-"scenario01"}

function _setup_kafka {

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
}

case $SCENARIO in
    scenario01)
        export RPC_TRANSPORT_URL=rabbit://pifpaf:secret@127.0.0.1:5682/
        export NOTIFY_TRANSPORT_URL=rabbit://pifpaf:secret@127.0.0.1:5682/
        RUN="--env-prefix RABBITMQ run rabbitmq"
        ;;
    scenario02)
        _setup_kafka
        export RPC_TRANSPORT_URL=rabbit://pifpaf:secret@127.0.0.1:5682/
        export NOTIFY_TRANSPORT_URL=kafka://127.0.0.1:9092/
        RUN="--env-prefix RABBITMQ run rabbitmq -- pifpaf --env-prefix KAFKA run kafka"
        ;;
    *) ;;
esac

pifpaf $RUN -- $*
