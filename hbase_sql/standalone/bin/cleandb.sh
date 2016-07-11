#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh

DB_DIR="${BASE_DIR}"/db

# Clean the Splice Machine database

# TODO need to check if running
# server still running - must stop first
#	SPID=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
#	ZPID=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
#    if [[ -n ${SPID} || -n ${ZPID} ]]; then
#        echo "Splice still running and must be shut down. Run stop-splice.sh"
#        exit 1;
#    fi
#fi

/bin/rm -rf "${BASE_DIR}"/log/target/SpliceTestYarnPlatform
/bin/rm -rf "${DB_DIR}"/zookeeper
/bin/rm -rf "${DB_DIR}"/hbase
# TODO do I have to do this?
# /bin/rm -rf "${BASE_DIR}"/lib/yarn-site.xml

