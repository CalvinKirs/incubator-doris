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

package org.apache.doris.plugin.hms;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Opt-in tests against a real Docker HMS. Only the fixture client writes grants. */
@EnabledIfEnvironmentVariable(named = "HMS_AUTH_TEST_URI", matches = ".+")
class HmsDockerIntegrationTest {
    private HmsConnection fixture;
    private String database;
    private HmsObjectName table;

    private HmsPluginConfig config(long ttl) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", System.getenv("HMS_AUTH_TEST_URI"));
        properties.put("cache.ttl.seconds", Long.toString(ttl));
        properties.put("hive.metastore.authentication.type", "simple");
        properties.put("hive.metastore.username", "hms_auth_service");
        properties.put("hms.socket.timeout.seconds", "2");
        properties.put("hive.metastore.connect.retries", "1");
        return new HmsPluginConfig(properties);
    }

    private HmsAuthorization authorization(long ttl) {
        HmsPluginConfig config = config(ttl);
        return new HmsAuthorization(config, new PooledHmsPrivilegeSource(config));
    }

    @BeforeEach
    void createFixture() throws Exception {
        fixture = HmsConnection.open(config(0).hiveConf());
        database = "auth_" + UUID.randomUUID().toString().replace("-", "");
        table = new HmsObjectName(database, "orders");
        Database db = new Database();
        db.setName(database);
        db.setOwnerName("Alice");
        db.setOwnerType(PrincipalType.USER);
        db.setParameters(new HashMap<>());
        fixture.client.create_database(db);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(Collections.singletonList(new FieldSchema("id", "int", "fixture")));
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setSerdeInfo(new SerDeInfo("fixture", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                new HashMap<>()));
        sd.setBucketCols(Collections.emptyList());
        sd.setSortCols(Collections.emptyList());
        sd.setParameters(new HashMap<>());
        Table remote = new Table();
        remote.setDbName(database);
        remote.setTableName(table.table);
        remote.setOwner("Alice");
        remote.setTableType("MANAGED_TABLE");
        remote.setSd(sd);
        remote.setPartitionKeys(Collections.emptyList());
        remote.setParameters(new HashMap<>());
        fixture.client.create_table(remote);
        // Retain fixture metadata for debugging; every run uses a distinct namespace.
        System.out.println("Real HMS fixture: " + database);
    }

    @AfterEach
    void closeFixtureClient() {
        if (fixture != null) {
            fixture.close();
        }
    }

    private PrivilegeBag privilege(PrincipalType type, String name, String operation) {
        HiveObjectRef object = new HiveObjectRef();
        object.setObjectType(HiveObjectType.TABLE);
        object.setDbName(database);
        object.setObjectName(table.table);
        PrivilegeGrantInfo info = new PrivilegeGrantInfo(operation, (int) (System.currentTimeMillis() / 1000),
                "fixture_admin", PrincipalType.USER, false);
        HiveObjectPrivilege grant = new HiveObjectPrivilege();
        grant.setHiveObject(object);
        grant.setPrincipalName(name);
        grant.setPrincipalType(type);
        grant.setGrantInfo(info);
        return new PrivilegeBag(Collections.singletonList(grant));
    }

    private void grant(PrincipalType type, String name, String operation) throws Exception {
        Assertions.assertTrue(fixture.client.grant_privileges(privilege(type, name, operation)));
    }

    private String role(String suffix) throws Exception {
        String name = database + "_" + suffix;
        Assertions.assertTrue(fixture.client.create_role(new Role(name, 0, "fixture_admin")));
        return name;
    }

    private void grantRole(String role, String name, PrincipalType type) throws Exception {
        Assertions.assertTrue(fixture.client.grant_role(role, name, type, "fixture_admin", PrincipalType.USER, false));
    }

    private boolean allowed(HmsAuthorization auth, String user, String... operations) {
        return auth.table(table).allows(auth.identities(user), new HashSet<>(Arrays.asList(operations)));
    }

    @Test
    void nativeUserRoleInheritancePublicAndAdmin() throws Exception {
        HmsAuthorization auth = authorization(0);
        Assertions.assertFalse(allowed(auth, "Alice", "SELECT"));
        grant(PrincipalType.USER, "Alice", "SELECT");
        Assertions.assertTrue(allowed(auth, "Alice", "SELECT"));
        Assertions.assertFalse(allowed(auth, "alice", "SELECT"));
        String reader = role("reader");
        String team = role("team");
        grantRole(reader, team, PrincipalType.ROLE);
        grantRole(team, "Bob", PrincipalType.USER);
        grant(PrincipalType.ROLE, reader, "INSERT");
        grant(PrincipalType.USER, "Bob", "DELETE");
        Assertions.assertTrue(allowed(auth, "Bob", "INSERT", "DELETE"));
        Assertions.assertFalse(allowed(auth, "Bob", "SELECT"));
        grant(PrincipalType.ROLE, "public", "SELECT");
        Assertions.assertTrue(allowed(auth, "stranger", "SELECT"));
        grantRole("admin", "admin_" + database, PrincipalType.USER);
        grant(PrincipalType.ROLE, "admin", "INSERT");
        Assertions.assertFalse(allowed(auth, "admin_" + database, "INSERT"));
        Assertions.assertTrue(fixture.client.revoke_role(team, "Bob", PrincipalType.USER));
        Assertions.assertFalse(allowed(auth, "Bob", "INSERT"));
    }

    @Test
    void nativeDatabaseAndTableOwnershipChanges() throws Exception {
        HmsAuthorization auth = authorization(0);
        Assertions.assertTrue(auth.canCreate(auth.identities("Alice"), database));
        Assertions.assertFalse(auth.canCreate(auth.identities("alice"), database));
        Assertions.assertTrue(auth.canCreate(auth.identities("anyone"), "default"));
        Assertions.assertTrue(auth.table(table).ownedBy(auth.identities("Alice")));
        String creator = role("creator");
        grantRole(creator, "Bob", PrincipalType.USER);
        Database db = fixture.client.get_database(database);
        db.setOwnerName(creator);
        db.setOwnerType(PrincipalType.ROLE);
        fixture.client.alter_database(database, db);
        Assertions.assertTrue(auth.canCreate(auth.identities("Bob"), database));
        Assertions.assertFalse(auth.canCreate(auth.identities("Alice"), database));
        Table remote = fixture.client.get_table(database, table.table);
        remote.setOwner("Bob");
        fixture.client.alter_table(database, table.table, remote);
        Assertions.assertTrue(auth.table(table).ownedBy(auth.identities("Bob")));
        Assertions.assertFalse(auth.table(table).ownedBy(auth.identities("Alice")));
    }

    @Test
    void hotTrafficDoesNotExtendRevokedGrantsAcrossInstances() throws Exception {
        grant(PrincipalType.USER, "Alice", "SELECT");
        HmsAuthorization first = authorization(2);
        HmsAuthorization second = authorization(2);
        Assertions.assertTrue(allowed(first, "Alice", "SELECT"));
        Assertions.assertTrue(allowed(second, "Alice", "SELECT"));
        Assertions.assertTrue(fixture.client.revoke_privileges(privilege(PrincipalType.USER, "Alice", "SELECT")));
        Assertions.assertFalse(allowed(authorization(0), "Alice", "SELECT"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (allowed(first, "Alice", "SELECT") | allowed(second, "Alice", "SELECT")) {
            Assertions.assertTrue(System.nanoTime() < deadline, "Hot traffic extended the revocation window");
            Thread.sleep(25);
        }
        Assertions.assertFalse(allowed(first, "Alice", "SELECT"));
        Assertions.assertFalse(allowed(second, "Alice", "SELECT"));
    }

    @Test
    void cachedRoleRevocationAndFailedLoadsRecover() throws Exception {
        String reader = role("reader");
        grantRole(reader, "Alice", PrincipalType.USER);
        grant(PrincipalType.ROLE, reader, "SELECT");
        HmsAuthorization auth = authorization(1);
        Assertions.assertTrue(allowed(auth, "Alice", "SELECT"));
        Assertions.assertTrue(fixture.client.revoke_role(reader, "Alice", PrincipalType.USER));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
        while (allowed(auth, "Alice", "SELECT")) {
            Assertions.assertTrue(System.nanoTime() < deadline, "Role revocation did not expire");
            Thread.sleep(25);
        }
        HmsObjectName missing = new HmsObjectName(database, "created_later");
        Assertions.assertThrows(HmsAuthorizationException.class, () -> auth.table(missing));
        Table remote = fixture.client.get_table(database, table.table);
        remote.setTableName(missing.table);
        remote.getSd().unsetLocation();
        fixture.client.create_table(remote);
        Assertions.assertEquals(HmsPrincipal.user("Alice"), auth.table(missing).owner);
    }

    private HmsAccessController controller(String sql) {
        HmsPluginConfig config = config(0);
        HmsNameResolver names = new HmsNameResolver() {
            @Override
            String remoteDatabase(String catalog, String localDatabase) {
                return database;
            }

            @Override
            HmsObjectName remoteTable(String catalog, String localDatabase, String localTable) {
                return table;
            }
        };
        LogicalPlan plan = sql == null ? null : HmsTestPlans.parse(sql);
        return new HmsAccessController(config, authorization(0), names, user -> false, () -> plan);
    }

    @Test
    void controllerSelectAppendOverwriteCreateAndAlterUseRealGrants() throws Exception {
        UserIdentity alice = UserIdentity.createAnalyzedUserIdentWithIp("Alice", "%");
        HmsAccessController select = controller(null);
        Assertions.assertFalse(select.checkTblPriv(true, alice, "hms", "sales", "orders", PrivPredicate.SELECT));
        grant(PrincipalType.USER, "Alice", "SELECT");
        Assertions.assertTrue(select.checkTblPriv(true, alice, "hms", "sales", "orders", PrivPredicate.SELECT));
        HmsAccessController append = controller("insert into hms.sales.orders select 1");
        HmsAccessController overwrite = controller("insert overwrite table hms.sales.orders select 1");
        grant(PrincipalType.USER, "Alice", "INSERT");
        Assertions.assertTrue(append.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertFalse(overwrite.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.LOAD));
        grant(PrincipalType.ROLE, "public", "DELETE");
        Assertions.assertTrue(overwrite.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertTrue(select.checkTblPriv(alice, "hms", "sales", "new_table", PrivPredicate.CREATE));
        HmsAccessController alter = controller("alter table hms.sales.orders add column c int");
        Assertions.assertTrue(alter.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.ALTER));
        Table remote = fixture.client.get_table(database, table.table);
        remote.setOwner("service");
        fixture.client.alter_table(database, table.table, remote);
        Assertions.assertFalse(alter.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.ALTER));
        // DROP and TRUNCATE follow ownership: grants alone never suffice, ownership alone always does.
        HmsAccessController truncate = controller("truncate table hms.sales.orders");
        Assertions.assertFalse(select.checkTblPriv(true, alice, "hms", "sales", "orders", PrivPredicate.DROP));
        Assertions.assertFalse(truncate.checkTblPriv(true, alice, "hms", "sales", "orders", PrivPredicate.LOAD));
        remote.setOwner("Alice");
        fixture.client.alter_table(database, table.table, remote);
        Assertions.assertTrue(select.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.DROP));
        Assertions.assertTrue(truncate.checkTblPriv(alice, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertTrue(select.checkDbPriv(true, alice, "hms", "sales", PrivPredicate.CREATE));
        Assertions.assertTrue(select.checkDbPriv(alice, "hms", "sales", PrivPredicate.DROP));
        Database db = fixture.client.get_database(database);
        db.setOwnerName("service");
        db.setOwnerType(PrincipalType.USER);
        fixture.client.alter_database(database, db);
        Assertions.assertFalse(select.checkDbPriv(true, alice, "hms", "sales", PrivPredicate.DROP));
        Assertions.assertTrue(select.checkDbPriv(alice, "hms", "sales", PrivPredicate.CREATE));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "HMS_AUTH_TEST_CONTAINER", matches = ".+")
    void stoppedServerFailsClosedAndPoolReconnects() throws Exception {
        String container = System.getenv("HMS_AUTH_TEST_CONTAINER");
        HmsAuthorization auth = authorization(0);
        grant(PrincipalType.USER, "Alice", "SELECT");
        Assertions.assertTrue(allowed(auth, "Alice", "SELECT"));
        fixture.close();
        fixture = null;
        try {
            docker("stop", "--time", "2", container);
            Assertions.assertThrows(HmsAuthorizationException.class, () -> allowed(auth, "Alice", "SELECT"));
        } finally {
            docker("start", container);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                Assertions.assertTrue(allowed(auth, "Alice", "SELECT"));
                break;
            } catch (HmsAuthorizationException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "HMS_AUTH_TEST_CONTAINER", matches = ".+")
    void idleConnectionFromBeforeARestartDoesNotFailTheFirstCheckAfterIt() throws Exception {
        String container = System.getenv("HMS_AUTH_TEST_CONTAINER");
        HmsAuthorization stale = authorization(0);
        grant(PrincipalType.USER, "Alice", "SELECT");
        Assertions.assertTrue(allowed(stale, "Alice", "SELECT"));
        fixture.close();
        fixture = null;
        docker("restart", "--time", "2", container);
        // Wait until an instance that never held a pre-restart connection can read grants again.
        HmsAuthorization probe = authorization(0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                Assertions.assertTrue(allowed(probe, "Alice", "SELECT"));
                break;
            } catch (HmsAuthorizationException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
        // The pooled connection opened before the restart is dead; the single retry must hide that.
        Assertions.assertTrue(allowed(stale, "Alice", "SELECT"));
    }

    private void docker(String... arguments) throws Exception {
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add("docker");
        Collections.addAll(command, arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(new java.io.File("target/hms-docker-control.log")))
                .start();
        Assertions.assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Docker command timed out");
        Assertions.assertEquals(0, process.exitValue());
    }
}
