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
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class HmsAccessControllerTest {
    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private final HmsAuthorizationTest.Source source = new HmsAuthorizationTest.Source();
    private final HmsNameResolver names = new HmsNameResolver() {
        @Override
        String remoteDatabase(String catalog, String database) {
            return "sales";
        }

        @Override
        HmsObjectName remoteTable(String catalog, String database, String table) {
            return HmsAuthorizationTest.TABLE;
        }
    };

    private HmsAccessController controller(boolean bypass, boolean administrator, String sql) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "0");
        properties.put("doris.admin.bypass.enabled", Boolean.toString(bypass));
        HmsPluginConfig config = new HmsPluginConfig(properties);
        LogicalPlan plan = sql == null ? null : HmsTestPlans.parse(sql);
        return new HmsAccessController(config, new HmsAuthorization(config, source), names,
                user -> administrator, () -> plan);
    }

    @Test
    void systemReadsHonorLocalTableAndColumnPermissionsWithoutHmsCalls() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "0");
        HmsPluginConfig config = new HmsPluginConfig(properties);
        HmsPrivilegeSource source = Mockito.mock(HmsPrivilegeSource.class);
        HmsNameResolver names = Mockito.mock(HmsNameResolver.class);
        CatalogAccessController local = Mockito.mock(CatalogAccessController.class);
        Mockito.when(names.isSystemTable("hms", "information_schema", "tables")).thenReturn(true);
        HmsAccessController controller = new HmsAccessController(config, new HmsAuthorization(config, source),
                names, user -> false, () -> null, () -> local);
        Assertions.assertFalse(controller.checkTblPriv(true, ALICE,
                "hms", "information_schema", "tables", PrivPredicate.SELECT));
        Mockito.when(local.checkTblPriv(ALICE, "hms", "information_schema", "tables", PrivPredicate.SELECT))
                .thenReturn(true);
        Assertions.assertTrue(controller.checkTblPriv(false, ALICE,
                "hms", "information_schema", "tables", PrivPredicate.SELECT));
        Set<String> columns = Collections.singleton("table_name");
        controller.checkColsPriv(false, ALICE, "hms", "information_schema", "tables", columns, PrivPredicate.SELECT);
        Mockito.verify(local).checkColsPriv(ALICE,
                "hms", "information_schema", "tables", columns, PrivPredicate.SELECT);
        Mockito.doThrow(new AuthorizationException("local column denial")).when(local)
                .checkColsPriv(ALICE, "hms", "information_schema", "tables", columns, PrivPredicate.SELECT);
        Assertions.assertThrows(AuthorizationException.class, () -> controller.checkColsPriv(true, ALICE,
                "hms", "information_schema", "tables", columns, PrivPredicate.SELECT));
        Assertions.assertFalse(controller.checkTblPriv(true, ALICE,
                "hms", "information_schema", "tables", PrivPredicate.DROP));
        Mockito.verifyNoInteractions(source);
        Mockito.verify(names, Mockito.never())
                .remoteTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    void createDatabaseIsDeniedWithoutResolvingTheMissingDatabase() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "0");
        HmsPluginConfig config = new HmsPluginConfig(properties);
        HmsPrivilegeSource source = Mockito.mock(HmsPrivilegeSource.class);
        HmsNameResolver names = Mockito.mock(HmsNameResolver.class);
        HmsAccessController controller = new HmsAccessController(config, new HmsAuthorization(config, source),
                names, user -> false, () -> null);
        // CREATE DATABASE is the only caller of checkDbPriv(CREATE). Hive SQL Standard lets any user create a
        // database, and the target cannot be resolved yet, so nothing is looked up.
        Assertions.assertTrue(controller.checkDbPriv(true, ALICE, "hms", "new_db", PrivPredicate.CREATE));
        Assertions.assertTrue(controller.checkDbPriv(false, ALICE, "hms", "new_db", PrivPredicate.CREATE));
        Assertions.assertFalse(controller.checkDbPriv(ALICE, "hms", "new_db", PrivPredicate.ALTER));
        Mockito.verifyNoInteractions(source, names);
    }

    @Test
    void dropDatabaseAndTableFollowOwnershipNotGrants() {
        source.db = new HmsGrants(HmsPrincipal.user("alice"), Collections.emptyMap());
        source.grants = new HmsGrants(HmsPrincipal.user("bob"),
                Collections.singletonMap(HmsPrincipal.user("alice"), Collections.singleton("SELECT")));
        HmsAccessController controller = controller(false, true, null);
        Assertions.assertTrue(controller.checkDbPriv(false, ALICE, "hms", "sales", PrivPredicate.DROP));
        Assertions.assertFalse(controller.checkTblPriv(true, ALICE, "hms", "sales", "orders", PrivPredicate.DROP));
        source.db = new HmsGrants(HmsPrincipal.role("creator"), Collections.emptyMap());
        source.grants = new HmsGrants(HmsPrincipal.user("alice"), Collections.emptyMap());
        Assertions.assertFalse(controller.checkDbPriv(true, ALICE, "hms", "sales", PrivPredicate.DROP));
        Assertions.assertTrue(controller.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.DROP));
        HmsAccessController truncate = controller(false, false, "truncate table hms.sales.orders");
        Assertions.assertTrue(truncate.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
        Set<String> writes = new java.util.HashSet<>(java.util.Arrays.asList("INSERT", "DELETE"));
        source.grants = new HmsGrants(HmsPrincipal.user("bob"),
                Collections.singletonMap(HmsPrincipal.user("alice"), writes));
        Assertions.assertFalse(truncate.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertFalse(controller.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.DROP));
    }

    @Test
    void firstUseReportsTheCatalogExactlyOnce() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "0");
        HmsPluginConfig config = new HmsPluginConfig(properties);
        List<String> catalogs = new ArrayList<>();
        HmsAccessController controller = new HmsAccessController(config, new HmsAuthorization(config, source),
                names, user -> false, () -> null, () -> null, catalogs::add);
        Assertions.assertTrue(controller.checkDbPriv(ALICE, "hms", "sales", PrivPredicate.SHOW));
        Assertions.assertTrue(controller.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Assertions.assertTrue(controller.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Assertions.assertEquals(Collections.singletonList("hms"), catalogs);
    }

    @Test
    void failureDescriptionIsOneLineWithTheRootCause() {
        Exception root = new java.net.ConnectException("Connection refused\n\tat java.base/sun.nio.ch.Net.connect");
        Exception wrapped = new HmsAuthorizationException("HMS authorization: failed to read role grants",
                new RuntimeException("Could not connect to meta store", root));
        Assertions.assertEquals("HMS authorization: failed to read role grants (java.net.ConnectException: "
                + "Connection refused", HmsAccessController.describe(wrapped));
        Assertions.assertEquals("plain", HmsAccessController.describe(new HmsAuthorizationException("plain")));
    }

    @Test
    void globalSelectCannotBypassTableOrColumnChecks() {
        source.grants = new HmsGrants(null, Collections.emptyMap());
        HmsAccessController controller = controller(false, true, null);
        Assertions.assertFalse(controller.checkTblPriv(true, ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Assertions.assertThrows(AuthorizationException.class, () -> controller.checkColsPriv(true, ALICE,
                "hms", "sales", "orders", Collections.singleton("id"), PrivPredicate.SELECT));
        source.db = new HmsGrants(HmsPrincipal.user("bob"), Collections.emptyMap());
        Assertions.assertFalse(controller.checkTblPriv(true, ALICE, "hms", "sales", "new_table", PrivPredicate.CREATE));
        Assertions.assertFalse(controller.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.DROP));
    }

    @Test
    void administratorBypassRequiresBothConfigurationAndActualAdminPrivilege() {
        source.fail = true;
        Assertions.assertTrue(controller(true, true, null)
                .checkTblPriv(false, ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Assertions.assertThrows(HmsAuthorizationException.class, () -> controller(true, false, null)
                .checkTblPriv(true, ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Assertions.assertThrows(HmsAuthorizationException.class, () -> controller(false, true, null)
                .checkTblPriv(true, ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
    }

    @Test
    void appendAndOverwriteRequireDifferentGrants() {
        source.grants = HmsAuthorizationTest.grants("alice", "INSERT");
        HmsAccessController append = controller(false, false, "insert into hms.sales.orders select 1");
        HmsAccessController overwrite = controller(false, false, "insert overwrite table hms.sales.orders select 1");
        Assertions.assertTrue(append.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertFalse(overwrite.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertFalse(append.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.SELECT));
        Map<HmsPrincipal, java.util.Set<String>> grants = new HashMap<>(source.grants.privileges);
        grants.put(HmsPrincipal.role("public"), Collections.singleton("DELETE"));
        source.grants = new HmsGrants(HmsPrincipal.user("alice"), grants);
        Assertions.assertTrue(overwrite.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
    }

    @Test
    void createResolvesOnlyDatabaseAndAlterUsesOwner() {
        source.db = new HmsGrants(HmsPrincipal.user("alice"), Collections.emptyMap());
        HmsAccessController create = controller(false, false, null);
        Assertions.assertTrue(create.checkTblPriv(ALICE, "hms", "sales", "new_table", PrivPredicate.CREATE));
        org.junit.jupiter.api.Assertions.assertEquals(0, source.tableLoads.get());
        HmsAccessController alter = controller(false, false, "alter table hms.sales.orders add column c int");
        Assertions.assertTrue(alter.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.ALTER));
        source.grants = HmsAuthorizationTest.grants("bob", "ALTER");
        Assertions.assertFalse(alter.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.ALTER));
        Assertions.assertThrows(HmsAuthorizationException.class, () -> create
                .checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
    }

    @Test
    void ctasVerifiesActualOwnerAndSourceReadPrivileges() {
        source.db = new HmsGrants(HmsPrincipal.user("alice"), Collections.emptyMap());
        source.grants = new HmsGrants(HmsPrincipal.user("alice"), Collections.emptyMap());
        HmsAccessController ctas = controller(false, false,
                "create table hms.sales.orders as select * from hms.sales.source");
        Assertions.assertTrue(ctas.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.CREATE));
        Assertions.assertTrue(ctas.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
        Assertions.assertFalse(ctas.checkTblPriv(ALICE, "hms", "sales", "source", PrivPredicate.SELECT));
        source.grants = new HmsGrants(HmsPrincipal.user("service"), Collections.emptyMap());
        Assertions.assertFalse(ctas.checkTblPriv(ALICE, "hms", "sales", "orders", PrivPredicate.LOAD));
    }
}
