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

# Add the independent plugin to the reactor without changing fe/pom.xml.
set -euo pipefail
plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
reactor_file="$(mktemp "${plugin_root}/../fe/.hms-plugin-reactor.XXXXXX")"
trap 'rm -f "${reactor_file}"' EXIT
cat > "${reactor_file}" <<'XML'
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.doris.plugin</groupId>
    <artifactId>hms-plugin-reactor</artifactId>
    <version>1</version>
    <packaging>pom</packaging>
    <modules>
        <module>.</module>
        <module>../hms-plugin</module>
    </modules>
</project>
XML
cd "${plugin_root}/../fe"
mvn -f "${reactor_file}" "$@"
