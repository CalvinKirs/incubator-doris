#!/usr/bin/env python3
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


"""Generate isolated fixture configuration; never modify the host krb5.conf."""
import json
from pathlib import Path
import socket
import sys
import xml.etree.ElementTree as ET

root = Path(sys.argv[1]).resolve()
hdfs = sys.argv[2]
root.mkdir(parents=True, exist_ok=False)
listeners = []
ports = []
for _ in range(2):
    tcp = socket.socket()
    tcp.bind(("127.0.0.1", 0))
    port = tcp.getsockname()[1]
    udp = socket.socket(type=socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", port))
    listeners.extend([tcp, udp])
    ports.append(port)
kdc, hms = ports
(root / "krb5.conf").write_text(f"""[libdefaults]
 default_realm = HMS.AUTH.TEST
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
 HMS.AUTH.TEST = {{
  kdc = 127.0.0.1:{kdc}
 }}
""")
(root / "kdc.conf").write_text(f"""[kdcdefaults]
 kdc_listen = 127.0.0.1:{kdc}
 kdc_tcp_listen = 127.0.0.1:{kdc}
[realms]
 HMS.AUTH.TEST = {{
  database_name = /var/kerberos/krb5kdc/principal
  key_stash_file = /var/kerberos/krb5kdc/.k5.HMS.AUTH.TEST
  max_life = 10m
  max_renewable_life = 1h
  supported_enctypes = aes128-cts-hmac-sha1-96:normal
 }}
[logging]
 kdc = FILE:/auth/kdc.log
""")
conf = root / "hms-conf"
conf.mkdir()
def xml(name, properties):
    tree = ET.Element("configuration")
    for key, value in properties.items():
        prop = ET.SubElement(tree, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = value
    ET.indent(tree)
    ET.ElementTree(tree).write(conf / name, encoding="UTF-8", xml_declaration=True)
xml("core-site.xml", {
    "fs.defaultFS": hdfs,
    "hadoop.security.authentication": "kerberos",
    "ipc.client.fallback-to-simple-auth-allowed": "true",
})
xml("hive-site.xml", {
    "javax.jdo.option.ConnectionURL": "jdbc:derby:;databaseName=/data/metastore;create=true",
    "javax.jdo.option.ConnectionDriverName": "org.apache.derby.jdbc.EmbeddedDriver",
    "hive.metastore.warehouse.dir": hdfs + "/user/hive/warehouse/kerberos",
    "hive.metastore.schema.verification": "true",
    "hive.metastore.sasl.enabled": "true",
    "hive.metastore.kerberos.principal": "hive/localhost@HMS.AUTH.TEST",
    "hive.metastore.kerberos.keytab.file": "/auth/hive.keytab",
    "hive.metastore.execute.setugi": "false",
})
(root / "ports.json").write_text(json.dumps({"kdc": kdc, "hms": hms}, indent=2) + "\n")
for listener in listeners:
    listener.close()
print(kdc, hms)
