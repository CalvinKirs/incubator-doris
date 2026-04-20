#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to generate generated source code
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/env.sh"

echo "Build generated code"
cd "${DORIS_HOME}/gensrc"

# Any argument keeps the existing incremental behavior used by build.sh.
# Support explicit flags to make the intent clearer for local development tools.
incremental_mode=0
if [[ "$#" != 0 ]]; then
    incremental_mode=1
fi
if [[ "${1-}" == "--incremental" ]]; then
    incremental_mode=1
fi
if [[ "${1-}" == "--clean" ]]; then
    incremental_mode=0
fi

# If calling from build.sh or local dev scripts in incremental mode, do not
# remove gensrc/build so make can reuse existing outputs. A clean build still
# removes the directory first.
if [[ "${incremental_mode}" -eq 0 ]]; then
    echo "rm -rf ${DORIS_HOME}/gensrc/build"
    rm -rf "${DORIS_HOME}/gensrc/build"
else
    echo "Reuse existing generated sources in ${DORIS_HOME}/gensrc/build"
fi

# DO NOT using parallel make(-j) for gensrc
make -j
rm -rf "${DORIS_HOME}/fe/fe-thrift/src/main/java/org/apache/doris/thrift" "${DORIS_HOME}/fe/fe-thrift/src/main/java/org/apache/parquet"
rm -rf "${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/doris/thrift" "${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/parquet"
rm -rf "${DORIS_HOME}/fe/fe-core/src/main/java/org/apache/doris/thrift" "${DORIS_HOME}/fe/fe-core/src/main/java/org/apache/parquet"

cd "${DORIS_HOME}/"
echo "Done"
exit 0
