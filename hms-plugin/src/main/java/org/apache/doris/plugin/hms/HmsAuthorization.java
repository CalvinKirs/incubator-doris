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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Cache source facts, never the expanded role closure or final user/table decision. */
final class HmsAuthorization {
    private final LoadingCache<HmsPrincipal, Set<HmsPrincipal>> roles;
    private final LoadingCache<HmsObjectName, HmsGrants> tables;
    private final LoadingCache<String, HmsGrants> databases;

    HmsAuthorization(HmsPluginConfig config, HmsPrivilegeSource source) {
        this(config, source, Ticker.systemTicker());
    }

    HmsAuthorization(HmsPluginConfig config, HmsPrivilegeSource source, Ticker ticker) {
        roles = cache(config, ticker).build(key -> Collections.unmodifiableSet(new HashSet<>(source.roles(key))));
        tables = cache(config, ticker).build(source::table);
        databases = cache(config, ticker).build(source::database);
    }

    private static Caffeine<Object, Object> cache(HmsPluginConfig config, Ticker ticker) {
        return Caffeine.newBuilder().maximumSize(config.maximumSize)
                .expireAfterWrite(config.ttlSeconds, TimeUnit.SECONDS).ticker(ticker).recordStats();
    }

    Set<HmsPrincipal> identities(String username) {
        Set<HmsPrincipal> result = new HashSet<>();
        Deque<HmsPrincipal> pending = new ArrayDeque<>();
        pending.add(HmsPrincipal.user(username));
        pending.add(HmsPrincipal.role("public"));
        while (!pending.isEmpty()) {
            HmsPrincipal principal = pending.removeFirst();
            // Hive SQL Standard excludes admin unless explicitly enabled. This plugin has no SET ROLE admin.
            if (principal.equals(HmsPrincipal.role("admin")) || !result.add(principal)) {
                continue;
            }
            pending.addAll(roles.get(principal));
        }
        return result;
    }

    boolean canCreate(Set<HmsPrincipal> identities, String database) {
        HmsGrants grants = databases.get(database);
        // Hive 1.1 SQLAuthorizationUtils.isOwner grants every user creation entitlement in default.
        return "default".equals(database) || grants.ownedBy(identities);
    }

    boolean ownsDatabase(Set<HmsPrincipal> identities, String database) {
        return databases.get(database).ownedBy(identities);
    }

    HmsGrants table(HmsObjectName name) {
        return tables.get(name);
    }

    Map<String, CacheStats> stats() {
        Map<String, CacheStats> stats = new HashMap<>();
        stats.put("roles", roles.stats());
        stats.put("tables", tables.stats());
        stats.put("databases", databases.stats());
        return Collections.unmodifiableMap(stats);
    }
}
