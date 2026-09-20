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


set -euo pipefail
fixture_dir=$(realpath -m "${1:?Usage: start.sh NEW_RUNTIME_DIRECTORY HDFS_URI}")
fixture_hdfs=${2:?HDFS URI is required}
script_dir=$(cd "$(dirname "$0")" && pwd)
fixture_suffix="$(date +%s)-$$"
fixture_kdc="hms-krb-kdc-${fixture_suffix}"
fixture_hms="hms-krb-hms-${fixture_suffix}"
# Keep generated keytabs/configuration outside source control. Never overwrite an existing realm.
python3 "${script_dir}/configure.py" "$fixture_dir" "$fixture_hdfs"
fixture_port=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["hms"])' \
    "${fixture_dir}/ports.json")
docker build --network host --build-arg http_proxy --build-arg https_proxy \
    -t gq-hms-auth-kdc:local -f "${script_dir}/Dockerfile.kdc" "$script_dir"
docker run -d --name "$fixture_kdc" --network host \
    -e FIXTURE_UID="$(id -u)" -e FIXTURE_GID="$(id -g)" \
    -v "${fixture_dir}:/auth" gq-hms-auth-kdc:local
for attempt in $(seq 1 60); do
    if [[ -f "${fixture_dir}/ready" ]]; then
        break
    fi
    sleep 1
done
test -f "${fixture_dir}/ready"
docker run -d --name "$fixture_hms" --network host --memory 1536m \
    -e HMS_PORT="$fixture_port" -e HADOOP_HEAPSIZE=768 \
    -v "${fixture_dir}:/auth:ro" -v "${script_dir}/hms.sh:/opt/kerberos-hms.sh:ro" \
    --entrypoint bash gq-hms-auth:1.1.0 /opt/kerberos-hms.sh
{
    printf 'export HMS_KRB_TEST_DIR=%q\n' "$fixture_dir"
    printf 'export HMS_KRB_TEST_URI=%q\n' "thrift://127.0.0.1:${fixture_port}"
    printf 'export HMS_KRB_TEST_CONTAINER=%q\n' "$fixture_hms"
    printf 'export HMS_KRB_KDC_CONTAINER=%q\n' "$fixture_kdc"
} > "${fixture_dir}/fixture.env"
python3 - "$fixture_port" <<'PYTHON'
import socket
import sys
import time

deadline = time.monotonic() + 90
while True:
    try:
        with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=2):
            break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(1)
PYTHON
printf 'Fixture environment: %s/fixture.env\n' "$fixture_dir"
