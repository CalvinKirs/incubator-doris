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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import shade.doris.hive.org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class PooledHmsPrivilegeSourceTest {
    static class Authentication implements ExecutionAuthenticator {
        final AtomicBoolean inside = new AtomicBoolean();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            Assertions.assertFalse(inside.getAndSet(true));
            calls.incrementAndGet();
            try {
                return task.call();
            } finally {
                inside.set(false);
            }
        }

        @Override
        public void execute(Runnable task) throws Exception {
            execute(() -> {
                task.run();
                return null;
            });
        }
    }

    @Test
    void creationEveryRpcAndReplacementUseAuthentication() throws Exception {
        Authentication authentication = new Authentication();
        ThriftHiveMetastore.Iface client = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Mockito.when(client.get_database("sales")).thenAnswer(invocation -> {
            Assertions.assertTrue(authentication.inside.get());
            return database("alice", PrincipalType.USER);
        });
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(1, authentication, () -> {
            Assertions.assertTrue(authentication.inside.get());
            created.incrementAndGet();
            return new HmsConnection(client, closed::incrementAndGet);
        });
        Assertions.assertEquals(HmsPrincipal.user("alice"), source.database("sales").owner);
        source.database("sales");
        Assertions.assertEquals(1, created.get());
        Assertions.assertEquals(3, authentication.calls.get());
        Mockito.doThrow(new TTransportException("connection lost")).when(client).get_database("sales");
        // A persistent transport failure is retried exactly once on a fresh connection, then reported.
        Assertions.assertThrows(HmsAuthorizationException.class, () -> source.database("sales"));
        Assertions.assertEquals(2, closed.get());
        Assertions.assertEquals(2, created.get());
        Mockito.doReturn(database("creator", PrincipalType.ROLE))
                .when(client).get_database("sales");
        Assertions.assertEquals(HmsPrincipal.role("creator"), source.database("sales").owner);
        Assertions.assertEquals(3, created.get());
    }

    @Test
    void transportFailureRetriesOnceOnFreshConnectionButApplicationErrorsDoNot() throws Exception {
        ThriftHiveMetastore.Iface stale = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Mockito.when(stale.get_database("sales")).thenThrow(new TTransportException("server restarted"));
        ThriftHiveMetastore.Iface fresh = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Mockito.when(fresh.get_database("sales")).thenReturn(database("alice", PrincipalType.USER));
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(2, new Authentication(), () ->
                new HmsConnection(created.getAndIncrement() == 0 ? stale : fresh, closed::incrementAndGet));
        Assertions.assertEquals(HmsPrincipal.user("alice"), source.database("sales").owner);
        Assertions.assertEquals(2, created.get());
        Assertions.assertEquals(1, closed.get());
        Mockito.verify(stale).get_database("sales");
        Mockito.verify(fresh).get_database("sales");

        ThriftHiveMetastore.Iface denied = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Mockito.when(denied.get_database("sales")).thenThrow(new MetaException("no such database"));
        AtomicInteger deniedCreated = new AtomicInteger();
        PooledHmsPrivilegeSource strict = new PooledHmsPrivilegeSource(2, new Authentication(), () -> {
            deniedCreated.incrementAndGet();
            return new HmsConnection(denied, () -> {});
        });
        HmsAuthorizationException error = Assertions.assertThrows(HmsAuthorizationException.class,
                () -> strict.database("sales"));
        Assertions.assertTrue(error.getMessage().contains("read database owner for sales"));
        Assertions.assertEquals(1, deniedCreated.get());
        Mockito.verify(denied, Mockito.times(1)).get_database("sales");
    }

    @Test
    void tableReadsLegacyRpcAndNullPrincipalGrantsWithoutCatalogField() throws Exception {
        ThriftHiveMetastore.Iface client = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Table table = new Table();
        table.setOwner("Alice");
        Mockito.when(client.get_table("sales", "orders")).thenReturn(table);
        Mockito.when(client.list_privileges(ArgumentMatchers.isNull(), ArgumentMatchers.isNull(),
                ArgumentMatchers.any(HiveObjectRef.class))).thenAnswer(invocation -> {
                    HiveObjectRef object = invocation.getArgument(2);
                    Assertions.assertEquals(HiveObjectType.TABLE, object.getObjectType());
                    Assertions.assertEquals("sales", object.getDbName());
                    Assertions.assertEquals("orders", object.getObjectName());
                    Assertions.assertFalse(object.isSetCatName());
                    return Arrays.asList(privilege(PrincipalType.USER, "Alice", "SELECT"),
                            privilege(PrincipalType.ROLE, "Writer", "INSERT"),
                            privilege(PrincipalType.ROLE, "Writer", "DELETE"),
                            privilege(PrincipalType.GROUP, "staff", "SELECT"));
                });
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(1, new Authentication(),
                () -> new HmsConnection(client, () -> {}));
        HmsGrants grants = source.table(new HmsObjectName("SALES", "Orders"));
        Assertions.assertEquals(HmsPrincipal.user("Alice"), grants.owner);
        Assertions.assertTrue(grants.allows(Collections.singleton(HmsPrincipal.user("Alice")),
                Collections.singleton("SELECT")));
        Assertions.assertFalse(grants.allows(Collections.singleton(HmsPrincipal.user("alice")),
                Collections.singleton("SELECT")));
        Assertions.assertTrue(grants.allows(Collections.singleton(HmsPrincipal.role("writer")),
                Collections.singleton("DELETE")));
        Assertions.assertEquals(2, grants.privileges.size());
        Mockito.verify(client).get_table("sales", "orders");
        Mockito.when(client.list_privileges(ArgumentMatchers.isNull(), ArgumentMatchers.isNull(),
                ArgumentMatchers.any(HiveObjectRef.class)))
                .thenReturn(Collections.emptyList());
        Assertions.assertTrue(source.table(HmsAuthorizationTest.TABLE).privileges.isEmpty());
    }

    @Test
    void roleRequestsPreservePrincipalTypeAndUserCase() throws Exception {
        ThriftHiveMetastore.Iface client = Mockito.mock(ThriftHiveMetastore.Iface.class);
        RolePrincipalGrant role = new RolePrincipalGrant();
        role.setRoleName("ANALYST");
        Mockito.when(client.get_role_grants_for_principal(ArgumentMatchers.any(GetRoleGrantsForPrincipalRequest.class)))
                .thenReturn(new GetRoleGrantsForPrincipalResponse(Collections.singletonList(
                        role)));
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(1, new Authentication(),
                () -> new HmsConnection(client, () -> {}));
        Assertions.assertEquals(Collections.singleton(HmsPrincipal.role("analyst")),
                source.roles(HmsPrincipal.user("Alice")));
        GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest();
        request.setPrincipal_name("Alice");
        request.setPrincipal_type(PrincipalType.USER);
        Mockito.verify(client).get_role_grants_for_principal(ArgumentMatchers.eq(request));
    }

    /** Thread-safe stand-in: the pool must serialize callers, not the authenticator. */
    static class ConcurrentAuthentication implements ExecutionAuthenticator {
        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            return task.call();
        }

        @Override
        public void execute(Runnable task) {
            task.run();
        }
    }

    @Test
    void poolBoundsConcurrentCallersAndPartialFailuresLeakNothing() throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch twoInside = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ThriftHiveMetastore.Iface client = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Mockito.when(client.get_database(ArgumentMatchers.anyString())).thenAnswer(invocation -> {
            peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            twoInside.countDown();
            try {
                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                String name = invocation.getArgument(0);
                if ("broken".equals(name)) {
                    throw new TTransportException("connection reset");
                }
                return database(name + "_owner", PrincipalType.USER);
            } finally {
                inFlight.decrementAndGet();
            }
        });
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(2, new ConcurrentAuthentication(), () -> {
            created.incrementAndGet();
            return new HmsConnection(client, closed::incrementAndGet);
        });
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<HmsGrants>> results = new ArrayList<>();
            for (String name : Arrays.asList("a", "broken", "b", "c")) {
                results.add(executor.submit(() -> source.database(name)));
            }
            Assertions.assertTrue(twoInside.await(10, TimeUnit.SECONDS));
            Thread.sleep(200);
            // Only pool-size callers may be inside the client at once; the rest wait to borrow.
            Assertions.assertEquals(2, inFlight.get());
            release.countDown();
            int failures = 0;
            for (Future<HmsGrants> result : results) {
                try {
                    Assertions.assertNotNull(result.get(10, TimeUnit.SECONDS).owner);
                } catch (ExecutionException e) {
                    Assertions.assertTrue(e.getCause() instanceof HmsAuthorizationException, e.toString());
                    failures++;
                }
            }
            Assertions.assertEquals(1, failures);
            Assertions.assertEquals(2, peak.get());
            // "broken" failed twice (once per attempt), destroying exactly two connections. Whether each
            // replacement was created or an idle survivor was reused depends on thread interleaving.
            Assertions.assertEquals(2, closed.get());
            Assertions.assertEquals(0, source.activeConnections());
            Assertions.assertEquals(created.get() - closed.get(), source.idleConnections());
            Assertions.assertTrue(source.idleConnections() >= 1 && source.idleConnections() <= 2);
            // Every survivor was returned: a second round completes and again leaves nothing active.
            List<Future<HmsGrants>> again = new ArrayList<>();
            for (String name : Arrays.asList("d", "e", "f", "g")) {
                again.add(executor.submit(() -> source.database(name)));
            }
            for (Future<HmsGrants> result : again) {
                Assertions.assertNotNull(result.get(10, TimeUnit.SECONDS).owner);
            }
            Assertions.assertEquals(2, closed.get());
            Assertions.assertEquals(0, source.activeConnections());
            Assertions.assertEquals(created.get() - closed.get(), source.idleConnections());
            Assertions.assertTrue(source.idleConnections() <= 2);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static HiveObjectPrivilege privilege(PrincipalType type, String name, String privilege) {
        HiveObjectPrivilege grant = new HiveObjectPrivilege();
        grant.setPrincipalType(type);
        grant.setPrincipalName(name);
        PrivilegeGrantInfo info = new PrivilegeGrantInfo();
        info.setPrivilege(privilege);
        grant.setGrantInfo(info);
        return grant;
    }

    private static Database database(String owner, PrincipalType type) {
        Database database = new Database();
        database.setOwnerName(owner);
        database.setOwnerType(type);
        return database;
    }

}
