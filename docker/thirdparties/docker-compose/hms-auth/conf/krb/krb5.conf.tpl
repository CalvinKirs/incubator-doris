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

# Standalone: FE and the regression JVM point -Djava.security.krb5.conf here; /etc/krb5.conf is never touched.
[libdefaults]
 default_realm = ${HMS_AUTH_REALM}
 dns_lookup_realm = false
 dns_lookup_kdc = false
 rdns = false
 udp_preference_limit = 1
 ticket_lifetime = 10m
 renewable = true
 default_tkt_enctypes = aes128-cts-hmac-sha1-96
 default_tgs_enctypes = aes128-cts-hmac-sha1-96
 permitted_enctypes = aes128-cts-hmac-sha1-96
[realms]
 ${HMS_AUTH_REALM} = {
  kdc = ${HMS_AUTH_HOST}:${HMS_AUTH_KDC_PORT}
 }
