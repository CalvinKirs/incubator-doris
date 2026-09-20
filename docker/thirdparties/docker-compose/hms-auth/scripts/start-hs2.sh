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

# The Hive 1.1 CLI refuses SQL Standard authorization, so role and grant SQL goes through this HiveServer2.
set -euo pipefail
exec "${HIVE_HOME}/bin/hive" --service hiveserver2 \
    --hiveconf hive.metastore.uris="${HMS_URI}" \
    --hiveconf hive.server2.thrift.port="${HS2_PORT}" \
    --hiveconf hive.server2.thrift.bind.host="${HS2_HOST}" \
    --hiveconf hive.server2.enable.doAs=false \
    --hiveconf hive.security.authorization.enabled=true \
    --hiveconf hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory
