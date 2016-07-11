#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh

_kill_em_all 9
exit 0

