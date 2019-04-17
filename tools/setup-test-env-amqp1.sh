#!/bin/bash
#
# Configuration files for the qdrouterd and artemis backends generated
# by pifpaf driver fixtures (https://github.com/jd/pifpaf)
set -e

. tools/functions.sh

ARTEMIS_VERSION=${ARTEMIS_VERSION:-"2.6.4"}

if [[ -z "$(which artemis)" ]]; then
    DATADIR=$(mktemp -d /tmp/OSLOMSG-ARTEMIS.XXXXX)
    trap "clean_exit $DATADIR" EXIT

    tarball=apache-artemis-${ARTEMIS_VERSION}-bin.tar.gz

    wget http://archive.apache.org/dist/activemq/activemq-artemis/${ARTEMIS_VERSION}/$tarball -O $DATADIR/$tarball
    tar -xzf $DATADIR/$tarball -C $DATADIR
    export PATH=$DATADIR/apache-artemis-${ARTEMIS_VERSION}/bin:$PATH
fi

# TODO(ansmith) look to move this to pifpaf driver
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

_setup_global_site_package_path
pifpaf --env-prefix ARTEMIS run artemis -- pifpaf --debug --env-prefix QDR run qdrouterd --username stackqpid --password secretqpid -- $*
