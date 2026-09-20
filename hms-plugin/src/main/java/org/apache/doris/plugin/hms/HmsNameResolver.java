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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;

import java.util.Locale;
import java.util.function.Function;

/** Resolve names through the same catalog objects used by the query. */
class HmsNameResolver {
    private final Function<String, CatalogIf<?>> catalogs;

    HmsNameResolver() {
        this(name -> Env.getCurrentEnv().getCatalogMgr().getCatalog(name));
    }

    HmsNameResolver(Function<String, CatalogIf<?>> catalogs) {
        this.catalogs = catalogs;
    }

    private ExternalDatabase<?> database(String catalog, String database) {
        CatalogIf<?> target = catalogs.apply(catalog);
        if (!(target instanceof HMSExternalCatalog)) {
            throw new HmsAuthorizationException("HMS authorization requires an HMS catalog: " + catalog);
        }
        ExternalDatabase<?> db = ((HMSExternalCatalog) target)
                .getDbNullable(ClusterNamespace.getNameFromFullName(database));
        if (db == null) {
            throw new HmsAuthorizationException("HMS authorization: cannot resolve database " + database);
        }
        return db;
    }

    String remoteDatabase(String catalog, String database) {
        return database(catalog, database).getRemoteName().toLowerCase(Locale.ROOT);
    }

    boolean isSystemTable(String catalog, String database, String table) {
        String name = ClusterNamespace.getNameFromFullName(database);
        // Avoid an extra catalog lookup on ordinary Hive queries. Names alone never grant access.
        if (!InfoSchemaDb.DATABASE_NAME.equalsIgnoreCase(name) && !MysqlDb.DATABASE_NAME.equalsIgnoreCase(name)) {
            return false;
        }
        ExternalDatabase<?> db = database(catalog, database);
        ExternalTable target = db.getTableNullable(table);
        return (db instanceof ExternalInfoSchemaDatabase && target instanceof ExternalInfoSchemaTable)
                || (db instanceof ExternalMysqlDatabase && target instanceof ExternalMysqlTable);
    }

    HmsObjectName remoteTable(String catalog, String database, String table) {
        ExternalDatabase<?> db = database(catalog, database);
        ExternalTable target = db.getTableNullable(table);
        if (!(target instanceof HMSExternalTable)) {
            throw new HmsAuthorizationException("HMS authorization: cannot resolve Hive table " + table);
        }
        return new HmsObjectName(db.getRemoteName(), target.getRemoteName());
    }
}
