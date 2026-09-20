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

# Keys are random per start and only ever written to the ignored runtime directory.
set -euo pipefail
export KRB5_CONFIG=/auth/krb5.conf
export KRB5_KDC_PROFILE=/auth/kdc.conf
if [[ ! -f /auth/ready ]]; then
    kdb5_util create -s -P "$(cat /proc/sys/kernel/random/uuid)"
    for principal in hive/localhost doris-hms other-hms doris-renew; do
        kadmin.local -q "addprinc -randkey ${principal}@${HMS_AUTH_REALM}"
    done
    # A one minute ticket lets the suites observe a real renewal.
    kadmin.local -q "modprinc -maxlife 60seconds doris-renew@${HMS_AUTH_REALM}"
    kadmin.local -q "ktadd -k /auth/hive.keytab hive/localhost@${HMS_AUTH_REALM}"
    kadmin.local -q "ktadd -k /auth/client.keytab doris-hms@${HMS_AUTH_REALM}"
    kadmin.local -q "ktadd -k /auth/other.keytab other-hms@${HMS_AUTH_REALM}"
    kadmin.local -q "ktadd -k /auth/renew.keytab doris-renew@${HMS_AUTH_REALM}"
    chmod 600 /auth/*.keytab
    touch /auth/ready
    chown "${FIXTURE_UID}:${FIXTURE_GID}" /auth/*.keytab /auth/ready
fi
exec krb5kdc -n
