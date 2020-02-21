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

function _setup_global_site_package_path {
    MAJOR=$(python -c 'import sys; print(sys.version_info.major)')
    MINOR=$(python -c 'import sys; print(sys.version_info.minor)')
    if [ -f "/etc/debian_version" ]; then
        PRE="dist"
    else
        PRE="site"
    fi
    # qdrouterd needs access to global site packages
    # create path file and place in virtual env working directory
    SITEDIR=${WORKDIR}/${ENVNAME}/lib/python${MAJOR}.${MINOR}/site-packages
    cat > ${SITEDIR}/dispatch.pth <<EOF
/usr/lib/python${MAJOR}.${MINOR}/${PRE}-packages
EOF
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
    scenario03)
        _setup_global_site_package_path
        export RPC_TRANSPORT_URL=amqp://stackqpid:secretqpid@127.0.0.1:5692/
        export NOTIFY_TRANSPORT_URL=rabbit://pifpaf:secret@127.0.0.1:5682/
        RUN="--env-prefix RABBITMQ run rabbitmq -- pifpaf --debug --env-prefix QDR run qdrouterd --username stackqpid --password secretqpid --port 5692"
        ;;
    scenario04)
        _setup_global_site_package_path
        _setup_kafka
        export RPC_TRANSPORT_URL=amqp://stackqpid:secretqpid@127.0.0.1:5692/
        export NOTIFY_TRANSPORT_URL=kafka://127.0.0.1:9092/
        RUN="--env-prefix KAFKA run kafka -- pifpaf --debug --env-prefix QDR run qdrouterd --username stackqpid --password secretqpid --port 5692"
        ;;
    *) ;;
esac

pifpaf $RUN -- $*
