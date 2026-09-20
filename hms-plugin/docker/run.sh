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

# Build an isolated real Hive 1.1.0 metastore and run the plugin's integration tests.
set -euo pipefail
plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worktree_root="$(cd "${plugin_root}/.." && pwd)"
build_dir="${plugin_root}/target/docker"
mkdir -p "${build_dir}"
archive="${build_dir}/apache-hive-1.1.0-bin.tar.gz"
if [[ ! -f "${archive}" ]]; then
    curl -fL --retry 2 https://archive.apache.org/dist/hive/hive-1.1.0/apache-hive-1.1.0-bin.tar.gz \
        -o "${archive}.partial"
    mv "${archive}.partial" "${archive}"
fi
printf '%s  %s\n' '7af11f13054f9aaba123457c95fedbacf44b8bd38c4fb522eba0ece9b1365c1f' "${archive}" | sha256sum -c -
cp "${plugin_root}/docker/"{Dockerfile,hive-site.xml,core-site.xml,start.sh,.dockerignore} "${build_dir}/"
docker build -t gq-hms-auth:1.1.0 "${build_dir}"
container="hms-auth-test-$(date +%s)-$$"
export HMS_AUTH_TEST_CONTAINER="${container}"
# Docker's automatic publish allocation can change on restart; bind an explicit free port.
port="$(python3 - <<'PORT'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PORT
)"
docker run -d --name "${container}" --label org.apache.doris.task=hms_auth_plugin \
    --memory 1536m --cpus 2 -e HADOOP_HEAPSIZE=768 -p "127.0.0.1:${port}:9083" gq-hms-auth:1.1.0
# Keep the container and its Derby data on success or failure for inspection.
trap 'docker logs "${container}" > "${build_dir}/${container}.log" 2>&1; \
    echo "Retained HMS container: ${container}; remove with: docker rm -f ${container}"' EXIT
address="$(docker port "${container}" 9083/tcp)"
export HMS_AUTH_TEST_URI="thrift://${address}"
python3 - "${address}" <<'READY'
import socket
import sys
import time
host, port = sys.argv[1].rsplit(":", 1)
deadline = time.monotonic() + 120
while True:
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(1)
print("HMS listening on", sys.argv[1], flush=True)
READY
cd "${worktree_root}"
export MAVEN_OPTS="${MAVEN_OPTS:--Xms512m -Xmx6g -XX:MaxMetaspaceSize=1g}"
export MAVEN_ARGS="-Dskip.clean=true ${MAVEN_ARGS:-}"
export CUSTOM_MVN="${plugin_root}/maven.sh"
export EXTRA_FE_MODULES='hms=../hms-plugin'
./run-fe-ut.sh --run 'org.apache.doris.plugin.hms.*Test'
