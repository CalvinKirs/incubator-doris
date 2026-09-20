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
import org.apache.hadoop.hive.metastore.api.Role
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import java.sql.DriverManager

// Dedicated Docker HMS/HDFS only: fixture setup writes native grants; the plugin stays read-only.
suite("hms_native_authorization", "external,hms_auth") {
    def jdbc = context.config.jdbcUrl.replaceFirst("/[^/?]+[?]", "/?")
    def password = context.config.otherConfigs.get("hmsAuthPassword")
    def endpoint = new URI(context.config.otherConfigs.get("hmsAuthUri"))
    def hdfs = context.config.otherConfigs.get("hmsAuthHdfsUri")
    def transport = new TSocket(endpoint.host, endpoint.port, 60000)
    transport.open()
    def client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport))
    def grant = { String table, PrincipalType type, String principal, String operation, boolean revoke = false,
                  String database = "hms_auth_fixture" ->
        def object = new HiveObjectRef(HiveObjectType.TABLE, database, table, null, null)
        def info = new PrivilegeGrantInfo(operation, (int) (System.currentTimeMillis() / 1000),
                "fixture_admin", PrincipalType.USER, false)
        def privilege = new HiveObjectPrivilege(object, principal, type, info)
        def bag = new PrivilegeBag([privilege])
        if (revoke) {
            client.revoke_privileges(bag)
        } else {
            client.grant_privileges(bag)
        }
    }
    try {
        sql "drop catalog if exists hms_auth"
        sql "drop catalog if exists hms_auth_admin"
        if (client.get_all_databases().contains("hms_auth_fixture")) {
            client.get_all_tables("hms_auth_fixture").each { name ->
                client.drop_table("hms_auth_fixture", name, true)
            }
            client.drop_database("hms_auth_fixture", true, false)
        }
        if (client.get_all_databases().contains("hms_auth_roledb")) {
            client.get_all_tables("hms_auth_roledb").each { name ->
                client.drop_table("hms_auth_roledb", name, true)
            }
            client.drop_database("hms_auth_roledb", true, false)
        }
        if (client.get_all_databases().contains("reader_created_db")) {
            client.drop_database("reader_created_db", true, false)
        }
        def db = new Database()
        db.name = "hms_auth_fixture"
        db.ownerName = "hms_auth_creator"
        db.ownerType = PrincipalType.USER
        db.parameters = [:]
        client.create_database(db)
        def roleDb = new Database()
        roleDb.name = "hms_auth_roledb"
        roleDb.ownerName = "hms_auth_team"
        roleDb.ownerType = PrincipalType.ROLE
        roleDb.parameters = [:]
        client.create_database(roleDb)
        ["source_table", "target_table", "public_table"].each { name ->
            def source = name != "target_table"
            def sd = new StorageDescriptor()
            sd.cols = [new FieldSchema("id", "int", "fixture")]
            sd.location = "${hdfs}/user/hive/warehouse/hms_auth_fixture.db/${name}"
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
            table.dbName = "hms_auth_fixture"
            table.tableName = name
            table.owner = "hms_auth_service"
            table.tableType = "MANAGED_TABLE"
            table.sd = sd
            table.partitionKeys = []
            table.parameters = [:]
            client.create_table(table)
        }
        def conf = new Configuration()
        conf.set("fs.defaultFS", hdfs)
        FileSystem.newInstance(new URI(hdfs), conf, "hms_auth_service").withCloseable { fs ->
            ["source_table", "public_table"].each { name ->
                fs.create(new Path("${hdfs}/user/hive/warehouse/hms_auth_fixture.db/${name}/data.txt"), true)
                        .withCloseable { stream -> stream.write("1\n2\n3\n".getBytes("UTF-8")) }
            }
        }
        ["reader", "writer", "denied", "creator", "role_user"].each { suffix ->
            sql "create user if not exists 'hms_auth_${suffix}' identified by '${password}'"
        }
        sql "grant select_priv on *.*.* to 'hms_auth_denied'"
        grant("source_table", PrincipalType.USER, "hms_auth_reader", "SELECT")
        grant("source_table", PrincipalType.USER, "hms_auth_writer", "SELECT")
        grant("source_table", PrincipalType.USER, "hms_auth_creator", "SELECT")
        grant("target_table", PrincipalType.USER, "hms_auth_reader", "SELECT")
        grant("target_table", PrincipalType.USER, "hms_auth_writer", "INSERT")
        grant("public_table", PrincipalType.ROLE, "public", "SELECT")
        ["hms_auth_leaf", "hms_auth_team"].each { name ->
            if (client.get_role_names().contains(name)) {
                client.drop_role(name)
            }
            client.create_role(new Role(name, 0, "fixture_admin"))
        }
        client.grant_role("hms_auth_leaf", "hms_auth_team", PrincipalType.ROLE,
                "fixture_admin", PrincipalType.USER, false)
        client.grant_role("hms_auth_team", "hms_auth_role_user", PrincipalType.USER,
                "fixture_admin", PrincipalType.USER, false)
        grant("source_table", PrincipalType.ROLE, "hms_auth_leaf", "SELECT")
        if (client.list_roles("hms_auth_denied", PrincipalType.USER).any { it.roleName == "admin" }) {
            client.revoke_role("admin", "hms_auth_denied", PrincipalType.USER)
        }
        client.grant_role("admin", "hms_auth_denied", PrincipalType.USER,
                "fixture_admin", PrincipalType.USER, false)
        grant("source_table", PrincipalType.ROLE, "admin", "SELECT")
        def catalogProperties = """
            'type'='hms', 'hive.metastore.uris'='${endpoint}', 'hive.version'='1.1.0',
            'hadoop.username'='hms_auth_service', 'fs.defaultFS'='${hdfs}',
            'access_controller.class'='org.apache.doris.plugin.hms.HmsAccessControllerFactory',
            'access_controller.properties.hms.uri'='${endpoint}',
            'access_controller.properties.hive.metastore.authentication.type'='simple',
            'access_controller.properties.hive.metastore.username'='hms_auth_service',
            'access_controller.properties.cache.ttl.seconds'='2'
        """
        sql "create catalog hms_auth properties (${catalogProperties})"
        sql """create catalog hms_auth_admin properties (${catalogProperties},
                'access_controller.properties.doris.admin.bypass.enabled'='true')"""
        // Doris's default system-schema grants belong to internal; external catalog grants stay scoped.
        sql "grant select_priv on hms_auth.information_schema.* to 'hms_auth_reader'"
        sql "grant select_priv on hms_auth.mysql.* to 'hms_auth_reader'"

        connect("hms_auth_reader", password, jdbc) {
            order_qt_metadata_tables """select table_name from hms_auth.information_schema.tables
                    where table_catalog='hms_auth' and table_schema='hms_auth_fixture' order by table_name"""
            order_qt_metadata_count """select count(*) from hms_auth.information_schema.tables
                    where table_catalog='hms_auth' and table_schema='hms_auth_fixture'"""
            order_qt_metadata_columns """select column_name from hms_auth.information_schema.columns
                    where table_catalog='hms_auth' and table_schema='hms_auth_fixture' and table_name='source_table'
                    order by ordinal_position"""
            order_qt_mysql_metadata "select Host, User from hms_auth.mysql.user where 1=0 order by Host, User"
            test {
                sql "select * from hms_auth.information_schema.cluster_snapshots"
                exception "Access denied"
            }
            order_qt_reader "select id from hms_auth.hms_auth_fixture.source_table order by id"
            test {
                sql "insert into hms_auth.hms_auth_fixture.target_table values (99)"
                exception "denied"
            }
            test {
                sql "create table hms_auth.hms_auth_fixture.reader_created (id int) engine=hive"
                exception "denied"
            }
            test {
                sql "alter table hms_auth.hms_auth_fixture.target_table add column denied_column int"
                exception "denied"
            }
        }
        sql "revoke select_priv on hms_auth.information_schema.* from 'hms_auth_reader'"
        connect("hms_auth_reader", password, jdbc) {
            test {
                sql "select table_name from hms_auth.information_schema.tables limit 1"
                exception "Permission denied"
            }
            test {
                sql "select count(*) from hms_auth.information_schema.tables"
                exception "Permission denied"
            }
        }
        sql "grant select_priv on hms_auth.information_schema.* to 'hms_auth_reader'"
        connect("hms_auth_denied", password, jdbc) {
            order_qt_global_select_metadata """select table_name from hms_auth.information_schema.tables
                    where table_catalog='hms_auth' and table_schema='hms_auth_fixture' order by table_name"""
            test {
                sql """select t.table_name, s.id from hms_auth.information_schema.tables t
                        join hms_auth.hms_auth_fixture.source_table s on true
                        where t.table_catalog='hms_auth' and t.table_schema='hms_auth_fixture' limit 1"""
                exception "denied"
            }
            test {
                sql "select id from hms_auth.hms_auth_fixture.source_table"
                exception "denied"
            }
            order_qt_public "select id from hms_auth.hms_auth_fixture.public_table order by id"
            test {
                sql "select id from hms_auth_admin.hms_auth_fixture.source_table"
                exception "denied"
            }
        }
        // root has local ADMIN, but bypass is deliberately disabled in the first catalog.
        order_qt_admin_metadata """select table_name from hms_auth.information_schema.tables
                where table_catalog='hms_auth' and table_schema='hms_auth_fixture' order by table_name"""
        test {
            sql "select id from hms_auth.hms_auth_fixture.source_table"
            exception "denied"
        }
        order_qt_admin "select id from hms_auth_admin.hms_auth_fixture.source_table order by id"
        connect("hms_auth_writer", password, jdbc) {
            sql "insert into hms_auth.hms_auth_fixture.target_table values (7)"
            sql """insert into hms_auth.hms_auth_fixture.target_table
                    select id from hms_auth.hms_auth_fixture.source_table"""
            test {
                sql "insert overwrite table hms_auth.hms_auth_fixture.target_table select 8"
                exception "denied"
            }
        }
        connect("hms_auth_reader", password, jdbc) {
            order_qt_inserted "select id from hms_auth.hms_auth_fixture.target_table order by id"
        }
        grant("target_table", PrincipalType.USER, "hms_auth_writer", "DELETE")
        sleep(2500)
        connect("hms_auth_writer", password, jdbc) {
            sql "insert overwrite table hms_auth.hms_auth_fixture.target_table select 8"
        }
        connect("hms_auth_reader", password, jdbc) {
            order_qt_overwritten "select id from hms_auth.hms_auth_fixture.target_table order by id"
        }
        // Server-side prepared statements (COM_STMT_PREPARE): ExecuteCommand hands the real INSERT or OVERWRITE
        // plan to the plugin, so append and overwrite keep their different grant requirements.
        def prepared = { String user, String statement, Integer value ->
            def url = jdbc + "&useServerPrepStmts=true&cachePrepStmts=true"
            DriverManager.getConnection(url, user, password).withCloseable { conn ->
                conn.prepareStatement(statement).withCloseable { stmt ->
                    if (value != null) {
                        stmt.setInt(1, value)
                    }
                    stmt.executeUpdate()
                }
            }
        }
        prepared("hms_auth_writer", "insert into hms_auth.hms_auth_fixture.target_table values (?)", 9)
        connect("hms_auth_reader", password, jdbc) {
            order_qt_prepared_insert "select id from hms_auth.hms_auth_fixture.target_table order by id"
        }
        grant("target_table", PrincipalType.USER, "hms_auth_writer", "DELETE", true)
        sleep(2500)
        def preparedOverwrite = null
        try {
            prepared("hms_auth_writer", "insert overwrite table hms_auth.hms_auth_fixture.target_table select 8", null)
        } catch (Exception e) {
            preparedOverwrite = e
        }
        assert preparedOverwrite != null && preparedOverwrite.toString().contains("denied"),
                "prepared OVERWRITE without DELETE must be denied, got: ${preparedOverwrite}"
        grant("target_table", PrincipalType.USER, "hms_auth_writer", "DELETE")
        sleep(2500)
        prepared("hms_auth_writer", "insert overwrite table hms_auth.hms_auth_fixture.target_table select 8", null)
        connect("hms_auth_reader", password, jdbc) {
            order_qt_prepared_overwrite "select id from hms_auth.hms_auth_fixture.target_table order by id"
        }
        connect("hms_auth_creator", password, jdbc) {
            sql """create table hms_auth.hms_auth_fixture.creator_created (id int) engine=hive
                    properties ('file_format'='parquet')"""
            sql """create table hms_auth.hms_auth_fixture.creator_ctas engine=hive
                    properties ('file_format'='parquet') as
                    select id from hms_auth.hms_auth_fixture.source_table"""
            sql "alter table hms_auth.hms_auth_fixture.creator_created set ('auto_analyze_policy'='disable')"
        }
        grant("creator_ctas", PrincipalType.USER, "hms_auth_creator", "SELECT")
        sleep(2500)
        connect("hms_auth_creator", password, jdbc) {
            order_qt_ctas "select id from hms_auth.hms_auth_fixture.creator_ctas order by id"
        }
        // Hive SQL Standard: DROP and TRUNCATE need the owner; CREATE DATABASE is open to every user.
        connect("hms_auth_reader", password, jdbc) {
            test {
                sql "truncate table hms_auth.hms_auth_fixture.creator_ctas"
                exception "denied"
            }
            test {
                sql "drop table hms_auth.hms_auth_fixture.creator_ctas"
                exception "denied"
            }
            test {
                sql "drop database hms_auth.hms_auth_fixture"
                exception "denied"
            }
            sql "create database hms_auth.reader_created_db"
            // This kernel records no HMS owner for a database it creates, so ownership checks deny its creator.
            test {
                sql "drop database hms_auth.reader_created_db"
                exception "denied"
            }
        }
        client.drop_database("reader_created_db", true, false)
        connect("hms_auth_creator", password, jdbc) {
            test {
                // Authorization passes for the owner; HMS 1.1 has no truncate_table RPC for the kernel to call.
                sql "truncate table hms_auth.hms_auth_fixture.creator_ctas"
                exception "truncate_table"
            }
            sql "drop table hms_auth.hms_auth_fixture.creator_ctas"
            test {
                sql "select id from hms_auth.hms_auth_fixture.creator_ctas"
                exception "creator_ctas"
            }
        }
        connect("hms_auth_role_user", password, jdbc) {
            order_qt_role "select id from hms_auth.hms_auth_fixture.source_table order by id"
        }
        // A database owned by a ROLE: creation entitlement comes through role membership, not a USER owner.
        connect("hms_auth_role_user", password, jdbc) {
            sql """create table hms_auth.hms_auth_roledb.role_ctas engine=hive
                    properties ('file_format'='parquet') as
                    select id from hms_auth.hms_auth_fixture.source_table"""
        }
        grant("role_ctas", PrincipalType.USER, "hms_auth_role_user", "SELECT", false, "hms_auth_roledb")
        sleep(2500)
        connect("hms_auth_role_user", password, jdbc) {
            order_qt_role_ctas "select id from hms_auth.hms_auth_roledb.role_ctas order by id"
        }
        connect("hms_auth_creator", password, jdbc) {
            test {
                sql """create table hms_auth.hms_auth_roledb.creator_denied engine=hive
                        properties ('file_format'='parquet') as
                        select id from hms_auth.hms_auth_fixture.source_table"""
                exception "denied"
            }
        }
        client.revoke_role("hms_auth_team", "hms_auth_role_user", PrincipalType.USER)
        sleep(2500)
        connect("hms_auth_role_user", password, jdbc) {
            test {
                sql "select id from hms_auth.hms_auth_fixture.source_table"
                exception "denied"
            }
            test {
                sql """create table hms_auth.hms_auth_roledb.role_ctas_denied engine=hive
                        properties ('file_format'='parquet') as select 1"""
                exception "denied"
            }
        }
        grant("source_table", PrincipalType.USER, "hms_auth_writer", "SELECT", true)
        grant("source_table", PrincipalType.USER, "hms_auth_creator", "SELECT", true)
        def created = client.get_table("hms_auth_fixture", "creator_created")
        created.owner = "hms_auth_service"
        client.alter_table("hms_auth_fixture", "creator_created", created)
        sleep(2500)
        connect("hms_auth_writer", password, jdbc) {
            test {
                sql """insert into hms_auth.hms_auth_fixture.target_table
                        select id from hms_auth.hms_auth_fixture.source_table"""
                exception "denied"
            }
        }
        connect("hms_auth_creator", password, jdbc) {
            test {
                sql """create table hms_auth.hms_auth_fixture.denied_ctas engine=hive
                        properties ('file_format'='parquet') as
                        select id from hms_auth.hms_auth_fixture.source_table"""
                exception "denied"
            }
            test {
                sql "alter table hms_auth.hms_auth_fixture.creator_created set ('auto_analyze_policy'='enable')"
                exception "denied"
            }
        }
        connect("hms_auth_reader", password, jdbc) {
            sql "select id from hms_auth.hms_auth_fixture.source_table order by id"
            sql "select id from hms_auth_admin.hms_auth_fixture.source_table order by id"
        }
        grant("source_table", PrincipalType.USER, "hms_auth_reader", "SELECT", true)
        // Keep sending actual SQL across the cache expiry boundary; reads must stop succeeding.
        connect("hms_auth_reader", password, jdbc) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3)
            while (System.nanoTime() < deadline) {
                try {
                    sql "select id from hms_auth.hms_auth_fixture.source_table order by id"
                } catch (Exception e) {
                    if (!e.toString().contains("denied")) {
                        throw e
                    }
                }
                sleep(100)
            }
            test {
                sql "select id from hms_auth.hms_auth_fixture.source_table"
                exception "denied"
            }
        }
        // The second catalog also expires its own permission facts.
        connect("hms_auth_reader", password, jdbc) {
            test {
                sql "select id from hms_auth_admin.hms_auth_fixture.source_table"
                exception "denied"
            }
        }
        def container = context.config.otherConfigs.get("hmsAuthContainer")
        def docker = { List<String> arguments ->
            def process = new ProcessBuilder(["docker"] + arguments).redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(
                            new File(context.config.dataPath, "../docker-control.log"))).start()
            if (!process.waitFor(45, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("Docker fixture control failed: " + arguments)
            }
        }
        connect("hms_auth_reader", password, jdbc) {
            sql "select id from hms_auth.hms_auth_fixture.public_table order by id"
        }
        try {
            docker(["stop", "--time", "2", container])
            sleep(2500)
            connect("hms_auth_reader", password, jdbc) {
                test {
                    sql "select id from hms_auth.hms_auth_fixture.public_table"
                    exception "HMS authorization"
                }
            }
        } finally {
            docker(["start", container])
        }
        connect("hms_auth_reader", password, jdbc) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90)
            while (true) {
                try {
                    sql "select id from hms_auth.hms_auth_fixture.public_table order by id"
                    break
                } catch (Exception e) {
                    if (System.nanoTime() >= deadline || !e.toString().contains("HMS authorization")) {
                        throw e
                    }
                    sleep(500)
                }
            }
            order_qt_reconnected "select id from hms_auth.hms_auth_fixture.public_table order by id"
        }
    } finally {
        transport.close()
    }
}
