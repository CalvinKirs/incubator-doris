<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# HMS native authorization fixture

Hive Metastore 1.1.0 with Hive SQL Standard authorization, for the suites of the HMS native authorization
plugin (`hms-plugin/`). It is **opt-in**: it is not in the default component set, not in the full set that a
bare `--stop` iterates, and is only ever started or stopped when `--hms-auth` is given.

```bash
cd docker/thirdparties
./run-thirdparties-docker.sh --hms-auth            # start only this fixture
./run-thirdparties-docker.sh --hms-auth -c hive3   # together with other components
./run-thirdparties-docker.sh --hms-auth --stop     # stop only this fixture
```

| Container (`doris-${CONTAINER_UID}-hms-auth-…`) | Role | Setting |
|---|---|---|
| `hdfs` | Hadoop 2.7.4, one NameNode and one DataNode, simple auth | `HMS_AUTH_FS_PORT` 8720 |
| `hms` | Metastore 1.1.0, Derby, simple auth | `HMS_AUTH_HMS_PORT` 9783 |
| `hs2` | HiveServer2 1.1.0 with SQL Standard authorization; the 1.1 CLI cannot enable it | `HMS_AUTH_HS2_PORT` 10783 |
| `kdc` | MIT KDC, realm `HMS.AUTH.TEST`, random keys per start | `HMS_AUTH_KDC_PORT` 8788 |
| `krb-hms` | Metastore 1.1.0 over SASL/Kerberos | `HMS_AUTH_KRB_HMS_PORT` 9793 |

Ports and the realm live in `hms-auth_settings.env`. All listeners bind `HMS_AUTH_HOST` (loopback by default)
on the host network, so FE, BE and the regression runner must be on the same machine. None of the ports
overlaps the `hive2`, `hive3` or `kerberos` components, and the host `/etc/krb5.conf` and `/etc/hosts` are
never modified.

Each start recreates the containers and `runtime/` (ignored): rendered Hadoop and Hive configuration,
`runtime/krb/krb5.conf`, the keytabs, and `runtime/fixture.env` with the variables the Java integration tests
read. Metadata and HDFS data live inside the containers and disappear with them; the suites rebuild their
fixtures on every run. The Hive 1.1.0 archive is downloaded once into this directory and verified against the
pinned SHA-256 before it enters the image build.

## Using it

Java integration tests:

```bash
source docker/thirdparties/docker-compose/hms-auth/runtime/fixture.env
./run-fe-ut.sh --run org.apache.doris.plugin.hms.HmsDockerIntegrationTest
./run-fe-ut.sh --run org.apache.doris.plugin.hms.HmsKerberosIntegrationTest
```

SQL suites: start FE with `-Djava.security.krb5.conf=<this dir>/runtime/krb/krb5.conf` (the path is stable
across restarts of the fixture) and set in the regression configuration:

```groovy
hmsAuthUri = "thrift://127.0.0.1:9783"
hmsAuthHdfsUri = "hdfs://127.0.0.1:8720"
hmsAuthContainer = "doris-<CONTAINER_UID>-hms-auth-hms"
hmsAuthHs2Container = "doris-<CONTAINER_UID>-hms-auth-hs2"
hmsAuthHs2Jdbc = "jdbc:hive2://127.0.0.1:10783/default"
hmsKrbUri = "thrift://127.0.0.1:9793"
hmsKrbDir = "<this dir>/runtime/krb"
```

The suites stop and start the `hms` container and the Kerberos Java test restarts `krb-hms`; run them serially.
