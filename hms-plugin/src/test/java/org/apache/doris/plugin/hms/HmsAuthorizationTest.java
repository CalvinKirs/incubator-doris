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

import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class HmsAuthorizationTest {
    static final HmsObjectName TABLE = new HmsObjectName("sales", "orders");

    static HmsPluginConfig config(long ttl) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", Long.toString(ttl));
        return new HmsPluginConfig(properties);
    }

    static HmsGrants grants(String user, String privilege) {
        return new HmsGrants(HmsPrincipal.user(user),
                Collections.singletonMap(HmsPrincipal.user(user), Collections.singleton(privilege)));
    }

    static class Source implements HmsPrivilegeSource {
        final Map<HmsPrincipal, Set<HmsPrincipal>> edges = new HashMap<>();
        final AtomicInteger tableLoads = new AtomicInteger();
        HmsGrants grants = grants("alice", "SELECT");
        HmsGrants db = new HmsGrants(HmsPrincipal.role("creator"), Collections.emptyMap());
        boolean fail;

        @Override
        public Set<HmsPrincipal> roles(HmsPrincipal principal) {
            return edges.getOrDefault(principal, Collections.emptySet());
        }

        @Override
        public HmsGrants table(HmsObjectName name) {
            tableLoads.incrementAndGet();
            if (fail) {
                throw new HmsAuthorizationException("RPC failed");
            }
            return grants;
        }

        @Override
        public HmsGrants database(String name) {
            return db;
        }
    }

    @Test
    void fixedTtlExpiresUnderHotTrafficAndFailedReloadNeverAllows() {
        AtomicLong nanos = new AtomicLong();
        Source source = new Source();
        HmsAuthorization auth = new HmsAuthorization(config(10), source, nanos::get);
        Assertions.assertTrue(auth.table(TABLE).allows(auth.identities("alice"), Collections.singleton("SELECT")));
        source.grants = new HmsGrants(null, Collections.emptyMap());
        for (int second = 1; second < 10; second++) {
            nanos.set(TimeUnit.SECONDS.toNanos(second));
            Assertions.assertTrue(auth.table(TABLE).allows(auth.identities("alice"), Collections.singleton("SELECT")));
        }
        nanos.set(TimeUnit.SECONDS.toNanos(10));
        source.fail = true;
        Assertions.assertThrows(HmsAuthorizationException.class, () -> auth.table(TABLE));
        source.fail = false;
        Assertions.assertFalse(auth.table(TABLE).allows(auth.identities("alice"), Collections.singleton("SELECT")));
        Assertions.assertEquals(3, source.tableLoads.get());
        auth.table(TABLE);
        Assertions.assertEquals(3, source.tableLoads.get());
    }

    @Test
    void rolesHandleInheritanceCyclesCasePublicAndAdminWithoutExtendingExpiry() {
        Source source = new Source();
        source.edges.put(HmsPrincipal.user("alice"), new HashSet<>(java.util.Arrays.asList(
                HmsPrincipal.role("Analyst"), HmsPrincipal.role("admin"))));
        source.edges.put(HmsPrincipal.role("analyst"), Collections.singleton(HmsPrincipal.role("reader")));
        source.edges.put(HmsPrincipal.role("reader"), Collections.singleton(HmsPrincipal.role("ANALYST")));
        source.edges.put(HmsPrincipal.role("admin"), Collections.singleton(HmsPrincipal.role("secret")));
        AtomicLong nanos = new AtomicLong();
        HmsAuthorization auth = new HmsAuthorization(config(10), source, nanos::get);
        Set<HmsPrincipal> identities = auth.identities("alice");
        Assertions.assertTrue(identities.contains(HmsPrincipal.role("reader")));
        Assertions.assertTrue(identities.contains(HmsPrincipal.role("public")));
        Assertions.assertFalse(identities.contains(HmsPrincipal.role("admin")));
        Assertions.assertFalse(identities.contains(HmsPrincipal.role("secret")));
        Assertions.assertFalse(auth.identities("Alice").contains(HmsPrincipal.role("analyst")));
        source.edges.clear();
        nanos.set(TimeUnit.SECONDS.toNanos(9));
        Assertions.assertTrue(auth.identities("alice").contains(HmsPrincipal.role("reader")));
        nanos.set(TimeUnit.SECONDS.toNanos(10));
        Assertions.assertFalse(auth.identities("alice").contains(HmsPrincipal.role("reader")));
        Assertions.assertFalse(HmsPrincipal.user("reader").equals(HmsPrincipal.role("reader")));
    }

    @Test
    void zeroTtlAndSeparateInstancesNeverReusePermissions() {
        Source source = new Source();
        HmsAuthorization auth = new HmsAuthorization(config(0), source);
        auth.table(TABLE);
        source.grants = new HmsGrants(null, Collections.emptyMap());
        Assertions.assertFalse(auth.table(TABLE).allows(auth.identities("alice"), Collections.singleton("SELECT")));
        Assertions.assertEquals(2, source.tableLoads.get());
        HmsAuthorization second = new HmsAuthorization(config(100), source);
        second.table(TABLE);
        Assertions.assertEquals(3, source.tableLoads.get());
    }

    @Test
    void mergesGrantsAcrossIdentitiesButOwnershipDoesNotImplySelect() {
        Map<HmsPrincipal, Set<String>> privileges = new HashMap<>();
        privileges.put(HmsPrincipal.user("alice"), Collections.singleton("INSERT"));
        privileges.put(HmsPrincipal.role("writer"), Collections.singleton("DELETE"));
        HmsGrants grants = new HmsGrants(HmsPrincipal.user("alice"), privileges);
        Set<HmsPrincipal> identities = new HashSet<>(privileges.keySet());
        Assertions.assertTrue(grants.allows(identities, new HashSet<>(java.util.Arrays.asList("INSERT", "DELETE"))));
        Assertions.assertFalse(grants.allows(identities, Collections.singleton("SELECT")));
        Assertions.assertTrue(grants.ownedBy(identities));
        privileges.clear();
        Assertions.assertTrue(grants.allows(identities, Collections.singleton("INSERT")));
    }

    @Test
    void sameKeyConcurrentLoadsAreCoalesced() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Source source = new Source() {
            @Override
            public HmsGrants table(HmsObjectName name) {
                entered.countDown();
                try {
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return super.table(name);
            }
        };
        HmsAuthorization auth = new HmsAuthorization(config(10), source, Ticker.systemTicker());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<HmsGrants> first = executor.submit(() -> auth.table(TABLE));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<HmsGrants> second = executor.submit(() -> auth.table(TABLE));
            // The second caller must be waiting on the in-flight load, not finding an empty cache later.
            Assertions.assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> second.get(300, TimeUnit.MILLISECONDS));
            release.countDown();
            Assertions.assertEquals(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, source.tableLoads.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void revocationDuringAnInFlightLoadIsVisibleOneTtlAfterTheLoadCompletes() throws Exception {
        CountDownLatch read = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicLong nanos = new AtomicLong();
        Source source = new Source() {
            @Override
            public HmsGrants table(HmsObjectName name) {
                HmsGrants snapshot = super.table(name);
                read.countDown();
                try {
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return snapshot;
            }
        };
        HmsAuthorization auth = new HmsAuthorization(config(10), source, nanos::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<HmsGrants> loading = executor.submit(() -> auth.table(TABLE));
            Assertions.assertTrue(read.await(10, TimeUnit.SECONDS));
            // Hive revokes after the RPC already read the old grants, while the load is still in flight.
            source.grants = new HmsGrants(null, Collections.emptyMap());
            nanos.set(TimeUnit.SECONDS.toNanos(4));
            release.countDown();
            Set<HmsPrincipal> alice = auth.identities("alice");
            Assertions.assertTrue(loading.get(10, TimeUnit.SECONDS).allows(alice, Collections.singleton("SELECT")));
            // The stale snapshot is served for a full TTL measured from when the load completed, not started.
            nanos.set(TimeUnit.SECONDS.toNanos(13));
            Assertions.assertTrue(auth.table(TABLE).allows(alice, Collections.singleton("SELECT")));
            nanos.set(TimeUnit.SECONDS.toNanos(14));
            Assertions.assertFalse(auth.table(TABLE).allows(alice, Collections.singleton("SELECT")));
            Assertions.assertEquals(2, source.tableLoads.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void roleOwnerChangeAndDefaultDatabaseCreation() {
        Source source = new Source();
        source.edges.put(HmsPrincipal.user("alice"), Collections.singleton(HmsPrincipal.role("creator")));
        HmsAuthorization auth = new HmsAuthorization(config(0), source);
        Assertions.assertTrue(auth.canCreate(auth.identities("alice"), "sales"));
        Assertions.assertFalse(auth.canCreate(auth.identities("bob"), "sales"));
        Assertions.assertTrue(auth.canCreate(auth.identities("bob"), "default"));
        source.db = new HmsGrants(HmsPrincipal.user("bob"), Collections.emptyMap());
        Assertions.assertFalse(auth.canCreate(auth.identities("alice"), "sales"));
    }

    @Test
    void capacityEvictionReloadsFactsWithoutChangingDecisions() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "600");
        properties.put("cache.maximum.size", "1");
        Source source = new Source();
        HmsAuthorization auth = new HmsAuthorization(new HmsPluginConfig(properties), source);
        for (int i = 0; i < 20; i++) {
            Assertions.assertTrue(auth.table(new HmsObjectName("sales", "table" + i))
                    .allows(Collections.singleton(HmsPrincipal.user("alice")), Collections.singleton("SELECT")));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (auth.stats().get("tables").evictionCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(auth.stats().get("tables").evictionCount() > 0);
        for (int i = 0; i < 20; i++) {
            Assertions.assertTrue(auth.table(new HmsObjectName("sales", "table" + i))
                    .allows(Collections.singleton(HmsPrincipal.user("alice")), Collections.singleton("SELECT")));
        }
        Assertions.assertTrue(source.tableLoads.get() > 20);
    }

}
