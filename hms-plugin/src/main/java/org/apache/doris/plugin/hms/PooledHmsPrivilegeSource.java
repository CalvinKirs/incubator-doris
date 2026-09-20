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
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import shade.doris.hive.org.apache.thrift.transport.TTransportException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/** Uses the same shaded Hive client and authenticator as branch-4.1, with a private pool. */
final class PooledHmsPrivilegeSource implements HmsPrivilegeSource {
    private final GenericObjectPool<HmsConnection> pool;
    private final ExecutionAuthenticator authenticator;

    PooledHmsPrivilegeSource(HmsPluginConfig config) {
        this(config, config.hiveConf());
    }

    private PooledHmsPrivilegeSource(HmsPluginConfig config, HiveConf conf) {
        this(config.poolSize, new HadoopExecutionAuthenticator(config.hmsAuthenticator()),
                () -> HmsConnection.open(conf));
    }

    PooledHmsPrivilegeSource(int poolSize, ExecutionAuthenticator authenticator, Callable<HmsConnection> provider) {
        this.authenticator = authenticator;
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(poolSize);
        config.setMaxIdle(poolSize);
        config.setMinIdle(0);
        config.setMaxWaitMillis(60_000);
        config.setTimeBetweenEvictionRunsMillis(-1);
        pool = new GenericObjectPool<>(new BasePooledObjectFactory<HmsConnection>() {
            @Override
            public HmsConnection create() throws Exception {
                ClassLoader previous = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
                    return authenticator.execute(provider);
                } finally {
                    Thread.currentThread().setContextClassLoader(previous);
                }
            }

            @Override
            public PooledObject<HmsConnection> wrap(HmsConnection client) {
                return new DefaultPooledObject<>(client);
            }

            @Override
            public void destroyObject(PooledObject<HmsConnection> object) {
                object.getObject().close();
            }
        }, config);
    }

    @Override
    public Set<HmsPrincipal> roles(HmsPrincipal principal) {
        return execute("read role grants", client -> {
            GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest();
            request.setPrincipal_name(principal.name);
            request.setPrincipal_type(principal.type);
            Set<HmsPrincipal> result = new HashSet<>();
            for (RolePrincipalGrant grant : client.get_role_grants_for_principal(request).getPrincipalGrants()) {
                result.add(HmsPrincipal.role(grant.getRoleName()));
            }
            return result;
        });
    }

    @Override
    public HmsGrants database(String name) {
        return execute("read database owner for " + name, client -> {
            Database db = client.get_database(name);
            HmsPrincipal owner = db.isSetOwnerName() && db.isSetOwnerType()
                    ? new HmsPrincipal(db.getOwnerType(), db.getOwnerName()) : null;
            return new HmsGrants(owner, Collections.emptyMap());
        });
    }

    @Override
    public HmsGrants table(HmsObjectName name) {
        return execute("read table privileges for " + name, client -> {
            Table table = client.get_table(name.database, name.table);
            // Hive 1.1 stores table ownership as a USER string, unlike database ownership.
            HmsPrincipal owner = table.isSetOwner() ? HmsPrincipal.user(table.getOwner()) : null;
            HiveObjectRef object = new HiveObjectRef();
            object.setObjectType(HiveObjectType.TABLE);
            object.setDbName(name.database);
            object.setObjectName(name.table);
            // Do not set catName: the target HMS 1.1 protocol has no catalog namespace.
            Map<HmsPrincipal, Set<String>> grants = new HashMap<>();
            for (HiveObjectPrivilege privilege : client.list_privileges(null, null, object)) {
                PrincipalType type = privilege.getPrincipalType();
                if (type != PrincipalType.USER && type != PrincipalType.ROLE) {
                    continue;
                }
                HmsPrincipal principal = new HmsPrincipal(type, privilege.getPrincipalName());
                String grant = privilege.getGrantInfo().getPrivilege().toUpperCase(Locale.ROOT);
                Set<String> values = grants.computeIfAbsent(principal, key -> new HashSet<>());
                if ("ALL".equals(grant)) {
                    Collections.addAll(values, "SELECT", "INSERT", "DELETE");
                } else {
                    values.add(grant);
                }
            }
            return new HmsGrants(owner, grants);
        });
    }

    private <T> T execute(String operation, Rpc<T> rpc) {
        try {
            return attempt(operation, rpc);
        } catch (HmsAuthorizationException first) {
            if (!transportFailure(first)) {
                throw first;
            }
            // An idle connection dies silently when the server restarts and the pool only learns on use.
            // Retry exactly once on a fresh connection so the first requests after recovery do not fail.
            try {
                return attempt(operation, rpc);
            } catch (HmsAuthorizationException second) {
                second.addSuppressed(first);
                throw second;
            }
        }
    }

    private static boolean transportFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof TTransportException) {
                return true;
            }
        }
        return false;
    }

    private <T> T attempt(String operation, Rpc<T> rpc) {
        HmsConnection client;
        try {
            client = pool.borrowObject();
        } catch (Exception e) {
            throw new HmsAuthorizationException("HMS authorization: cannot borrow client to " + operation, e);
        }
        T result;
        try {
            // A failed connection is destroyed; the next borrow reconnects in create() under doAs.
            result = authenticator.execute(() -> rpc.call(client.client));
        } catch (Exception e) {
            try {
                pool.invalidateObject(client);
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw new HmsAuthorizationException("HMS authorization: failed to " + operation, e);
        }
        pool.returnObject(client);
        return result;
    }

    /** Test hooks: a leak shows up as a connection that never returns to idle. */
    int activeConnections() {
        return pool.getNumActive();
    }

    int idleConnections() {
        return pool.getNumIdle();
    }

    @FunctionalInterface
    interface Rpc<T> {
        T call(ThriftHiveMetastore.Iface client) throws Exception;
    }
}
