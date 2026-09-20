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
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege
import org.apache.hadoop.hive.metastore.api.HiveObjectRef
import org.apache.hadoop.hive.metastore.api.HiveObjectType
import org.apache.hadoop.hive.metastore.api.PrincipalType
import org.apache.hadoop.hive.metastore.api.PrivilegeBag
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

// Cases are isolated from the main SQL suite and retain their fixtures after execution.
suite("hms_case_authorization", "external,hms_auth") {
    def jdbc = context.config.jdbcUrl.replaceFirst("/[^/?]+[?]", "/?")
    def password = context.config.otherConfigs.get("hmsAuthPassword")
    def endpoint = new URI(context.config.otherConfigs.get("hmsAuthUri"))
    def hdfs = context.config.otherConfigs.get("hmsAuthHdfsUri")
    def transport = new TSocket(endpoint.host, endpoint.port, 60000)
    transport.open()
    def client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport))
    def grant = { String database, String table, PrincipalType type, String principal, boolean revoke = false ->
        def object = new HiveObjectRef(HiveObjectType.TABLE, database, table, null, null)
        def info = new PrivilegeGrantInfo("SELECT", (int) (System.currentTimeMillis() / 1000),
                "fixture_admin", PrincipalType.USER, false)
        def bag = new PrivilegeBag([new HiveObjectPrivilege(object, principal, type, info)])
        if (revoke) {
            client.revoke_privileges(bag)
        } else {
            client.grant_privileges(bag)
        }
    }
    def hiveSql = { String statement ->
        def command = ["docker", "exec", "-i", context.config.otherConfigs.get("hmsAuthHs2Container"),
                "/opt/apache-hive-1.1.0-bin/bin/beeline", "-u", context.config.otherConfigs.get("hmsAuthHs2Jdbc"),
                "-n", "root", "-p", "", "-f", "/dev/stdin"]
        def log = new File(context.config.dataPath, "../hive-case-sql.log")
        def process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log)).start()
        process.outputStream.withCloseable { stream ->
            stream.write(("set role admin;\n" + statement.split(";").collect { it.trim() }
                    .findAll { !it.isEmpty() }.join(";\n") + ";\n").getBytes("UTF-8"))
        }
        if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("Hive 1.1 SQL fixture failed; see " + log)
        }
    }
    try {
        ["hms_case_sensitive", "hms_case_fold1", "hms_case_fold2"].each {
            sql "drop catalog if exists ${it}"
        }
        ["hms_case_db"].each { name ->
            if (client.get_all_databases().contains(name)) {
                client.get_all_tables(name).each { table -> client.drop_table(name, table, true) }
                client.drop_database(name, true, false)
            }
        }
        ["HmS_Case_Db"].each { name ->
            def db = new Database()
            db.name = name
            db.ownerName = "HmsCaseUser"
            db.ownerType = PrincipalType.USER
            db.parameters = [:]
            client.create_database(db)
        }
        def conf = new Configuration()
        conf.set("fs.defaultFS", hdfs)
        FileSystem.newInstance(new URI(hdfs), conf, "hms_auth_service").withCloseable { fs ->
            [["hms_case_db", "MiXeD_Table", 11], ["hms_case_db", "User_Only", 22],
             ["hms_case_db", "Role_Only", 33]].each { entry ->
                def sd = new StorageDescriptor()
                sd.cols = [new FieldSchema("id", "int", "case fixture")]
                sd.location = "${hdfs}/user/hive/warehouse/${entry[0]}.db/${entry[1].toLowerCase(Locale.ROOT)}"
                sd.inputFormat = "org.apache.hadoop.mapred.TextInputFormat"
                sd.outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                sd.serdeInfo = new SerDeInfo("fixture", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", [:])
                sd.bucketCols = []
                sd.sortCols = []
                sd.parameters = [:]
                def table = new Table()
                table.dbName = entry[0]
                table.tableName = entry[1]
                table.owner = "HmsCaseUser"
                table.tableType = "MANAGED_TABLE"
                table.sd = sd
                table.partitionKeys = []
                table.parameters = [:]
                client.create_table(table)
                fs.create(new Path(sd.location + "/data.txt"), true).withCloseable { stream ->
                    stream.write("${entry[2]}\n".getBytes("UTF-8"))
                }
            }
        }
        ["HmsCaseUser", "hmscaseuser", "HmsCaseMember", "hmscasemember"].each {
            sql "create user if not exists '${it}' identified by '${password}'"
        }
        ["hms_case_leaf", "hmscaseuser"].each { role ->
            if (client.get_role_names().contains(role)) {
                client.drop_role(role)
            }
        }
        if (!client.list_roles("root", PrincipalType.USER).any { it.roleName == "admin" }) {
            client.grant_role("admin", "root", PrincipalType.USER, "fixture_admin", PrincipalType.USER, false)
        }
        // Use the actual Hive 1.1 SQL Standard authorizer, not test-side lowercasing of role names.
        hiveSql("""CREATE ROLE HmS_Case_Leaf; CREATE ROLE HmScAsEuSeR;
            GRANT ROLE HMS_CASE_LEAF TO ROLE hMsCaSeUsEr;
            GRANT ROLE HMSCASEUSER TO USER HmsCaseMember;
            GRANT SELECT ON TABLE hms_case_db.role_only TO ROLE HmS_Case_Leaf;""")
        def storedRoles = client.get_role_names().findAll { it.toLowerCase(Locale.ROOT).startsWith("hmscase") ||
                it.toLowerCase(Locale.ROOT) == "hms_case_leaf" }.sort().join(",")
        qt_native_role_names "select '${storedRoles}'"
        def stored = client.get_table("HMS_CASE_DB", "MIXED_TABLE")
        qt_native_object_names "select '${stored.dbName}', '${stored.tableName}', '${stored.owner}'"
        grant("hms_case_db", "mixed_table", PrincipalType.USER, "HmsCaseUser")
        grant("hms_case_db", "user_only", PrincipalType.USER, "hmscaseuser")
        def properties = """'type'='hms','hive.metastore.uris'='${endpoint}','hive.version'='1.1.0',
            'hadoop.username'='hms_auth_service','fs.defaultFS'='${hdfs}',
            'include_database_list'='hms_case_db',
            'access_controller.class'='org.apache.doris.plugin.hms.HmsAccessControllerFactory',
            'access_controller.properties.hms.uri'='${endpoint}',
            'access_controller.properties.hive.metastore.authentication.type'='simple',
            'access_controller.properties.hive.metastore.username'='hms_auth_service',
            'access_controller.properties.cache.ttl.seconds'='2'"""
        sql "create catalog hms_case_sensitive properties (${properties})"
        [1, 2].each { mode ->
            sql """create catalog hms_case_fold${mode} properties (${properties},
                'lower_case_database_names'='${mode}','lower_case_table_names'='${mode}')"""
        }
        connect("HmsCaseUser", password, jdbc) {
            order_qt_user_upper "select id from hms_case_sensitive.hms_case_db.mixed_table order by id"
            test {
                sql "select id from hms_case_sensitive.HMS_CASE_DB.mixed_table"
                exception "Database [HMS_CASE_DB] does not exist"
            }
            test {
                sql "select id from hms_case_sensitive.hms_case_db.MIXED_TABLE"
                exception "does not exist"
            }
            order_qt_fold1 "select ID from hms_case_fold1.HMS_CASE_DB.MIXED_TABLE order by ID"
            order_qt_fold2 "select Id from hms_case_fold2.HmS_Case_Db.MiXeD_Table order by Id"
            order_qt_quoted_alias """select Src.ID from hms_case_fold2.`HmS_Case_Db`.`MiXeD_Table` AS Src
                    order by Src.ID"""
            sql "create table hms_case_sensitive.hms_case_db.owner_created (id int) engine=hive"
            sql "alter table hms_case_sensitive.hms_case_db.mixed_table set ('auto_analyze_policy'='disable')"
            sql "create table hms_case_fold1.HMS_CASE_DB.fold_created (id int) engine=hive"
        }
        connect("hmscaseuser", password, jdbc) {
            test {
                sql "select id from hms_case_sensitive.hms_case_db.mixed_table"
                exception "HMS authorization denied"
            }
            test {
                sql "select id from hms_case_fold2.HMS_CASE_DB.MIXED_TABLE"
                exception "HMS authorization denied"
            }
            order_qt_user_lower "select id from hms_case_sensitive.hms_case_db.user_only order by id"
            test {
                sql """select USER_ONLY.id from hms_case_fold2.`HMS_CASE_DB`.`MIXED_TABLE` AS USER_ONLY"""
                exception "HMS authorization denied"
            }
            test {
                sql "select id from hms_case_sensitive.hms_case_db.role_only"
                exception "HMS authorization denied"
            }
            test {
                sql "create table hms_case_sensitive.hms_case_db.denied_created (id int) engine=hive"
                exception "denied"
            }
            test {
                sql "create table hms_case_fold1.HMS_CASE_DB.denied_fold_created (id int) engine=hive"
                exception "denied"
            }
            test {
                sql "alter table hms_case_sensitive.hms_case_db.mixed_table set ('auto_analyze_policy'='enable')"
                exception "denied"
            }
        }
        connect("HmsCaseMember", password, jdbc) {
            order_qt_role_member "select id from hms_case_sensitive.hms_case_db.role_only order by id"
            test {
                sql "select id from hms_case_sensitive.hms_case_db.user_only"
                exception "HMS authorization denied"
            }
        }
        connect("hmscasemember", password, jdbc) {
            test {
                sql "select id from hms_case_sensitive.hms_case_db.role_only"
                exception "HMS authorization denied"
            }
        }
        // Reverse USER permissions while preserving the two different cache identities.
        grant("hms_case_db", "mixed_table", PrincipalType.USER, "HmsCaseUser", true)
        grant("hms_case_db", "mixed_table", PrincipalType.USER, "hmscaseuser")
        sleep(2500)
        connect("HmsCaseUser", password, jdbc) {
            test {
                sql "select id from hms_case_fold1.HMS_CASE_DB.MIXED_TABLE"
                exception "HMS authorization denied"
            }
        }
        connect("hmscaseuser", password, jdbc) {
            order_qt_reversed_user "select id from hms_case_fold2.HMS_CASE_DB.MIXED_TABLE order by id"
        }
        hiveSql("REVOKE ROLE hMsCaSeUsEr FROM USER HmsCaseMember;")
        sleep(2500)
        connect("HmsCaseMember", password, jdbc) {
            test {
                sql "select id from hms_case_sensitive.hms_case_db.role_only"
                exception "HMS authorization denied"
            }
        }
    } finally {
        transport.close()
    }
}
