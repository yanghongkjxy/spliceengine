#!/bin/bash

#############################################################################
#
# copies needed jars after a complete build into lib for standalone packaging
# -- assumes CDH 5.5.2 build (at this point)
#
#############################################################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# TODO - bulletproof
mv lib libold
mkdir lib
cp ../target/dependency/*.jar lib
cp ../target/hbase_sql*.jar lib
cd lib

# TODO - should remove others as well
rm *mem*

# these hose CentOS
rm *jasper*


