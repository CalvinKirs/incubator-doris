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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class HmsAccessController implements CatalogAccessController {
    private static final Logger LOG = LogManager.getLogger(HmsAccessController.class);
    private final HmsPluginConfig config;
    private final HmsAuthorization authorization;
    private final HmsNameResolver names;
    private final Predicate<UserIdentity> administrator;
    private final Supplier<LogicalPlan> plan;
    private final Supplier<CatalogAccessController> localController;
    private final Consumer<String> onFirstUse;
    private final AtomicBoolean used = new AtomicBoolean();

    HmsAccessController(HmsPluginConfig config, HmsAuthorization authorization) {
        this(config, authorization, new HmsNameResolver(),
                user -> internal().checkGlobalPriv(user, PrivPredicate.ADMIN), HmsOperation::currentPlan,
                HmsAccessController::internal, catalog -> HmsMetrics.register(catalog, authorization));
    }

    HmsAccessController(HmsPluginConfig config, HmsAuthorization authorization, HmsNameResolver names,
            Predicate<UserIdentity> administrator, Supplier<LogicalPlan> plan) {
        this(config, authorization, names, administrator, plan, HmsAccessController::internal);
    }

    HmsAccessController(HmsPluginConfig config, HmsAuthorization authorization, HmsNameResolver names,
            Predicate<UserIdentity> administrator, Supplier<LogicalPlan> plan,
            Supplier<CatalogAccessController> localController) {
        this(config, authorization, names, administrator, plan, localController, catalog -> { });
    }

    HmsAccessController(HmsPluginConfig config, HmsAuthorization authorization, HmsNameResolver names,
            Predicate<UserIdentity> administrator, Supplier<LogicalPlan> plan,
            Supplier<CatalogAccessController> localController, Consumer<String> onFirstUse) {
        this.onFirstUse = onFirstUse;
        this.config = config;
        this.authorization = authorization;
        this.names = names;
        this.administrator = administrator;
        this.plan = plan;
        this.localController = localController;
    }

    private static CatalogAccessController internal() {
        return Env.getCurrentEnv().getAccessManager()
                .getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME);
    }

    /** The factory never learns the catalog name; the first check does. */
    private void firstUse(String ctl) {
        if (used.compareAndSet(false, true)) {
            onFirstUse.accept(ctl);
        }
    }

    private boolean bypass(UserIdentity user) {
        return config.adminBypass && administrator.test(user);
    }

    private Set<HmsPrincipal> identities(UserIdentity user) {
        return authorization.identities(ClusterNamespace.getNameFromFullName(user.getQualifiedUser()));
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity user, PrivPredicate wanted) {
        return internal().checkGlobalPriv(user, wanted);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity user, String ctl, PrivPredicate wanted) {
        return wanted == PrivPredicate.SHOW || bypass(user);
    }

    @Override
    public boolean checkCtlPriv(boolean hasGlobal, UserIdentity user, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(user, ctl, wanted);
    }

    @Override
    public boolean checkDbPriv(UserIdentity user, String ctl, String db, PrivPredicate wanted) {
        firstUse(ctl);
        if (wanted == PrivPredicate.SHOW || bypass(user)) {
            return true;
        }
        if (wanted == PrivPredicate.CREATE) {
            // CREATE TABLE arrives through checkTblPriv(CREATE); this is CREATE DATABASE. Hive SQL Standard
            // lets every user create a database, and the target cannot be resolved before it exists.
            return true;
        }
        if (wanted == PrivPredicate.DROP) {
            return authorization.ownsDatabase(identities(user), names.remoteDatabase(ctl, db));
        }
        return false;
    }

    @Override
    public boolean checkDbPriv(boolean hasGlobal, UserIdentity user, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(user, ctl, db, wanted);
    }

    @Override
    public boolean checkTblPriv(UserIdentity user, String ctl, String db, String tbl, PrivPredicate wanted) {
        firstUse(ctl);
        if (wanted == PrivPredicate.SHOW || wanted == PrivPredicate.SHOW_VIEW || bypass(user)) {
            return true;
        }
        try {
            if (names.isSystemTable(ctl, db, tbl)) {
                // Doris-local metadata tables: local SELECT rules apply, nothing else is ever allowed on them.
                return wanted == PrivPredicate.SELECT && localController.get().checkTblPriv(user, ctl, db, tbl, wanted);
            }
            if (wanted == PrivPredicate.CREATE) {
                // The table does not exist yet. Resolve only the existing remote database.
                return authorization.canCreate(identities(user), names.remoteDatabase(ctl, db));
            }
            if (wanted != PrivPredicate.SELECT && wanted != PrivPredicate.LOAD && wanted != PrivPredicate.ALTER
                    && wanted != PrivPredicate.DROP) {
                return false;
            }
            boolean needsPlan = wanted != PrivPredicate.SELECT && wanted != PrivPredicate.DROP;
            HmsOperation operation = HmsOperation.resolve(wanted, needsPlan ? plan.get() : null);
            HmsObjectName name = names.remoteTable(ctl, db, tbl);
            Set<HmsPrincipal> identities = identities(user);
            HmsGrants grants = authorization.table(name);
            if (operation.owner && !grants.ownedBy(identities)) {
                return false;
            }
            if (operation.ctas && !authorization.canCreate(identities, name.database)) {
                return false;
            }
            return grants.allows(identities, operation.privileges);
        } catch (HmsAuthorizationException e) {
            // One line per failed check; an HMS outage must not turn every query into a stack trace.
            LOG.warn("HMS authorization lookup failed for {}.{}.{}: {}", ctl, db, tbl, describe(e));
            LOG.debug("HMS authorization lookup failure detail for {}.{}.{}", ctl, db, tbl, e);
            throw e;
        }
    }

    @Override
    public boolean checkTblPriv(boolean hasGlobal, UserIdentity user, String ctl, String db, String tbl,
            PrivPredicate wanted) {
        return checkTblPriv(user, ctl, db, tbl, wanted);
    }

    @Override
    public void checkColsPriv(UserIdentity user, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        if (wanted == PrivPredicate.SELECT && names.isSystemTable(ctl, db, tbl)) {
            localController.get().checkColsPriv(user, ctl, db, tbl, cols, wanted);
            return;
        }
        if (!checkTblPriv(user, ctl, db, tbl, wanted)) {
            throw new AuthorizationException("HMS authorization denied " + wanted.getPrivs()
                    + " on " + ctl + "." + db + "." + tbl + " for " + user.getQualifiedUser());
        }
    }

    @Override
    public void checkColsPriv(boolean hasGlobal, UserIdentity user, String ctl, String db, String tbl,
            Set<String> cols, PrivPredicate wanted) throws AuthorizationException {
        checkColsPriv(user, ctl, db, tbl, cols, wanted);
    }

    @Override
    public boolean checkResourcePriv(UserIdentity user, String resource, PrivPredicate wanted) {
        return internal().checkResourcePriv(user, resource, wanted);
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity user, String group, PrivPredicate wanted) {
        return internal().checkWorkloadGroupPriv(user, group, wanted);
    }

    @Override
    public boolean checkCloudPriv(UserIdentity user, String cloud, PrivPredicate wanted, ResourceTypeEnum type) {
        return internal().checkCloudPriv(user, cloud, wanted, type);
    }

    @Override
    public boolean checkStorageVaultPriv(UserIdentity user, String vault, PrivPredicate wanted) {
        return internal().checkStorageVaultPriv(user, vault, wanted);
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity user, String ctl, String db, String tbl,
            String col) {
        return Optional.empty();
    }

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity user, String ctl, String db, String tbl) {
        return Collections.emptyList();
    }

    /** One line: the Hive client embeds whole stack traces in MetaException messages. */
    static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        String detail = root == error ? error.getMessage() : error.getMessage() + " (" + root + ")";
        int newline = detail.indexOf('\n');
        return newline < 0 ? detail : detail.substring(0, newline);
    }

    public Map<String, CacheStats> getCacheStats() {
        return authorization.stats();
    }
}
