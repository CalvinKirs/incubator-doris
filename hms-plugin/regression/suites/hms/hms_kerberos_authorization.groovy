// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege
import org.apache.hadoop.hive.metastore.api.HiveObjectRef
import org.apache.hadoop.hive.metastore.api.HiveObjectType
import org.apache.hadoop.hive.metastore.api.PrincipalType
import org.apache.hadoop.hive.metastore.api.PrivilegeBag
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo
import org.apache.hadoop.hive.metastore.api.Role
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore
import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSaslClientTransport
import org.apache.thrift.transport.TSocket

import java.security.PrivilegedExceptionAction

// Requires the isolated KDC + SASL HMS fixture in docker/kerberos; BE/HDFS stay Simple.
suite("hms_kerberos_authorization", "external,hms_auth") {
    def jdbc = context.config.jdbcUrl.replaceFirst("/[^/?]+[?]", "/?")
    def password = context.config.otherConfigs.get("hmsAuthPassword")
    def endpoint = context.config.otherConfigs.get("hmsKrbUri")
    def directory = context.config.otherConfigs.get("hmsKrbDir")
    def hdfs = context.config.otherConfigs.get("hmsAuthHdfsUri")
    def conf = new HiveConf()
    conf.set("hive.metastore.uris", endpoint)
    conf.setBoolean("hive.metastore.sasl.enabled", true)
    conf.set("hive.metastore.kerberos.principal", "hive/localhost@HMS.AUTH.TEST")
    conf.set("hadoop.security.authentication", "kerberos")
    conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true)
    UserGroupInformation.setConfiguration(conf)
    def ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            "doris-hms@HMS.AUTH.TEST", "${directory}/client.keytab")
    def asService = { Closure action -> ugi.doAs(action as PrivilegedExceptionAction) }
    // The regression runner bundles a newer Thrift than its Hive 2 fixture wrapper supports.
    // Open GSSAPI directly for fixture administration; production still uses Doris's Hive client.
    def address = new URI(endpoint)
    def transport = asService {
        def sasl = new TSaslClientTransport("GSSAPI", null, "hive", "localhost",
                ["javax.security.sasl.qop": "auth", "javax.security.sasl.server.authentication": "true"],
                null, new TSocket(address.host, address.port, 60000))
        sasl.open()
        sasl
    }
    def client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport))
    def grant = { String table, PrincipalType type, String principal, String operation, boolean revoke = false ->
        asService {
            def object = new HiveObjectRef(HiveObjectType.TABLE, "hms_krb_fixture", table, null, null)
            def info = new PrivilegeGrantInfo(operation, (int) (System.currentTimeMillis() / 1000),
                    "fixture_admin", PrincipalType.USER, false)
            def bag = new PrivilegeBag([new HiveObjectPrivilege(object, principal, type, info)])
            if (revoke) {
                client.revoke_privileges(bag)
            } else {
                client.grant_privileges(bag)
            }
        }
    }
    try {
        ["hms_krb", "hms_krb_renew", "hms_krb_wrong_key", "hms_krb_missing_key",
         "hms_krb_wrong_service", "hms_krb_simple"].each { name -> sql "drop catalog if exists ${name}" }
        asService {
            if (client.get_all_databases().contains("hms_krb_fixture")) {
                client.get_all_tables("hms_krb_fixture").each { client.drop_table("hms_krb_fixture", it, true) }
                client.drop_database("hms_krb_fixture", true, false)
            }
            def db = new Database()
            db.name = "hms_krb_fixture"
            db.ownerName = "hms_krb_writer"
            db.ownerType = PrincipalType.USER
            db.parameters = [:]
            client.create_database(db)
            ["source_table", "target_table"].each { name ->
                def source = name == "source_table"
                def sd = new StorageDescriptor()
                sd.cols = [new FieldSchema("id", "int", "fixture")]
                sd.location = "${hdfs}/user/hive/warehouse/hms_krb_fixture.db/${name}"
                sd.inputFormat = source ? "org.apache.hadoop.mapred.TextInputFormat" :
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
                sd.outputFormat = source ? "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" :
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
                sd.serdeInfo = new SerDeInfo("fixture", source ? "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" :
                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", [:])
                sd.bucketCols = []
                sd.sortCols = []
                sd.parameters = [:]
                def table = new Table()
                table.dbName = "hms_krb_fixture"
                table.tableName = name
                table.owner = "doris-hms"
                table.tableType = "MANAGED_TABLE"
                table.sd = sd
                table.partitionKeys = []
                table.parameters = [:]
                client.create_table(table)
            }
            def fsConf = new Configuration()
            fsConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true)
            FileSystem.newInstance(new URI(hdfs), fsConf, "hms_auth_service").withCloseable { fs ->
                fs.create(new Path("${hdfs}/user/hive/warehouse/hms_krb_fixture.db/source_table/data.txt"), true)
                        .withCloseable { it.write("11\n22\n33\n".getBytes("UTF-8")) }
            }
            if (client.get_role_names().contains("hms_krb_reader_role")) {
                client.drop_role("hms_krb_reader_role")
            }
            client.create_role(new Role("hms_krb_reader_role", 0, "fixture_admin"))
            client.grant_role("hms_krb_reader_role", "hms_krb_member", PrincipalType.USER,
                    "fixture_admin", PrincipalType.USER, false)
        }
        ["KrbReader", "krbreader", "hms_krb_writer", "hms_krb_member", "hms_krb_denied"].each { name ->
            sql "create user if not exists '${name}' identified by '${password}'"
        }
        sql "grant select_priv on *.*.* to 'hms_krb_denied'"
        grant("source_table", PrincipalType.USER, "KrbReader", "SELECT")
        grant("source_table", PrincipalType.USER, "hms_krb_writer", "SELECT")
        grant("source_table", PrincipalType.ROLE, "hms_krb_reader_role", "SELECT")
        grant("target_table", PrincipalType.USER, "KrbReader", "SELECT")
        grant("target_table", PrincipalType.USER, "hms_krb_writer", "INSERT")
        // Service identity has SELECT too: an ungranted query user must still be rejected.
        grant("source_table", PrincipalType.USER, "doris-hms", "SELECT")
        def createCatalog = { String name, String principal, String keytab, String service, String mode ->
            def credentials = mode == "kerberos" ? """
                'access_controller.properties.hive.metastore.client.principal'='${principal}',
                'access_controller.properties.hive.metastore.client.keytab'='${directory}/${keytab}.keytab',
            """ : ""
            sql """create catalog ${name} properties (
                'type'='hms', 'hive.version'='1.1.0', 'hive.metastore.uris'='${endpoint}',
                'hadoop.security.authentication'='simple', 'hadoop.username'='hms_auth_service',
                'fs.defaultFS'='${hdfs}',
                'hive.metastore.authentication.type'='kerberos',
                'hive.metastore.client.principal'='doris-hms@HMS.AUTH.TEST',
                'hive.metastore.client.keytab'='${directory}/client.keytab',
                'hive.metastore.service.principal'='hive/localhost@HMS.AUTH.TEST',
                'access_controller.class'='org.apache.doris.plugin.hms.HmsAccessControllerFactory',
                'access_controller.properties.hms.uri'='${endpoint}',
                'access_controller.properties.cache.ttl.seconds'='1',
                'access_controller.properties.hms.socket.timeout.seconds'='2',
                'access_controller.properties.hive.metastore.connect.retries'='1',
                'access_controller.properties.hive.metastore.authentication.type'='${mode}',
                ${credentials}
                'access_controller.properties.hive.metastore.service.principal'='${service}'
            )"""
        }
        createCatalog("hms_krb", "doris-hms@HMS.AUTH.TEST", "client", "hive/localhost@HMS.AUTH.TEST", "kerberos")
        createCatalog("hms_krb_renew", "doris-renew@HMS.AUTH.TEST", "renew", "hive/localhost@HMS.AUTH.TEST", "kerberos")
        connect("KrbReader", password, jdbc) {
            order_qt_reader "select id from hms_krb.hms_krb_fixture.source_table order by id"
            order_qt_before_renew "select id from hms_krb_renew.hms_krb_fixture.source_table order by id"
            test {
                sql "insert into hms_krb.hms_krb_fixture.target_table values (99)"
                exception "denied"
            }
        }
        ["krbreader", "hms_krb_denied"].each { user ->
            connect(user, password, jdbc) {
                test {
                    sql "select id from hms_krb.hms_krb_fixture.source_table"
                    exception "denied"
                }
            }
        }
        connect("hms_krb_writer", password, jdbc) {
            sql "insert into hms_krb.hms_krb_fixture.target_table select id from hms_krb.hms_krb_fixture.source_table"
            test {
                sql "insert overwrite table hms_krb.hms_krb_fixture.target_table values (99)"
                exception "denied"
            }
        }
        connect("KrbReader", password, jdbc) {
            order_qt_inserted "select id from hms_krb.hms_krb_fixture.target_table order by id"
        }
        connect("hms_krb_member", password, jdbc) {
            order_qt_role "select id from hms_krb.hms_krb_fixture.source_table order by id"
        }
        asService { client.revoke_role("hms_krb_reader_role", "hms_krb_member", PrincipalType.USER) }
        grant("source_table", PrincipalType.USER, "KrbReader", "SELECT", true)
        sleep(1500)
        ["hms_krb_member", "KrbReader"].each { user ->
            connect(user, password, jdbc) {
                test {
                    sql "select id from hms_krb.hms_krb_fixture.source_table"
                    exception "denied"
                }
            }
        }
        grant("source_table", PrincipalType.USER, "KrbReader", "SELECT")
        [ ["hms_krb_wrong_key", "other", "hive/localhost@HMS.AUTH.TEST", "kerberos"],
          ["hms_krb_missing_key", "missing", "hive/localhost@HMS.AUTH.TEST", "kerberos"],
          ["hms_krb_wrong_service", "client", "hive/missing@HMS.AUTH.TEST", "kerberos"],
          ["hms_krb_simple", "client", "hive/localhost@HMS.AUTH.TEST", "simple"] ].each { item ->
            // Metadata credentials remain valid; only the authorization plugin's credentials are changed.
            createCatalog(item[0], "doris-hms@HMS.AUTH.TEST", item[1], item[2], item[3])
            connect("KrbReader", password, jdbc) {
                test {
                    sql "select id from ${item[0]}.hms_krb_fixture.source_table"
                    exception(item[3] == "simple" ? "HMS authorization: failed to read role grants" :
                            "HMS authorization: cannot borrow client")
                }
            }
        }
        // The KDC limits doris-renew's TGT to 60 seconds. The Java test also verifies TGT timestamps.
        sleep(65000)
        connect("KrbReader", password, jdbc) {
            order_qt_after_renew "select id from hms_krb_renew.hms_krb_fixture.source_table order by id"
            order_qt_regranted "select id from hms_krb.hms_krb_fixture.source_table order by id"
        }
    } finally {
        transport.close()
    }
}
