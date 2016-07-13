#!/bin/bash

#############################################################################
#
# copies needed jars after a complete build into lib for standalone packaging
# -- assumes CDH 5.5.2 build (at this point)
#
#############################################################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# TODO - bulletproof
rm -r lib
mkdir lib

cp ../target/dependency/*.jar lib
cp ../target/hbase_sql*.jar lib

# TODO - should remove others as well
rm -f lib/*mem*

# these hose CentOS
rm -f lib/*jasper*


