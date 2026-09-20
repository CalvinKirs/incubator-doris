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

import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HmsNameResolverTest {
    @Test
    void recognizesOnlyActualLocalSystemTables() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalInfoSchemaDatabase information = Mockito.mock(ExternalInfoSchemaDatabase.class);
        ExternalMysqlDatabase mysql = Mockito.mock(ExternalMysqlDatabase.class);
        HMSExternalDatabase remote = Mockito.mock(HMSExternalDatabase.class);
        HMSExternalTable hiveTable = Mockito.mock(HMSExternalTable.class);
        Mockito.doReturn(information).when(catalog).getDbNullable("information_schema");
        Mockito.doReturn(mysql).when(catalog).getDbNullable("mysql");
        Mockito.when(information.getTableNullable("tables")).thenReturn(Mockito.mock(ExternalInfoSchemaTable.class));
        Mockito.when(mysql.getTableNullable("user")).thenReturn(Mockito.mock(ExternalMysqlTable.class));
        Mockito.when(information.getTableNullable("remote_table")).thenReturn(hiveTable);
        HmsNameResolver names = new HmsNameResolver(name -> catalog);
        Assertions.assertTrue(names.isSystemTable("catalog", "information_schema", "tables"));
        Assertions.assertTrue(names.isSystemTable("catalog", "mysql", "user"));
        Assertions.assertFalse(names.isSystemTable("catalog", "information_schema", "missing"));
        Assertions.assertFalse(names.isSystemTable("catalog", "information_schema", "remote_table"));
        // An HMS object with a reserved spelling is not a local system table.
        Mockito.doReturn(remote).when(catalog).getDbNullable("information_schema");
        Mockito.when(remote.getTableNullable("tables")).thenReturn(hiveTable);
        Assertions.assertFalse(names.isSystemTable("catalog", "information_schema", "tables"));
        Assertions.assertFalse(names.isSystemTable("catalog", "sales", "tables"));
        Mockito.verify(catalog, Mockito.never()).getDbNullable("sales");
    }

    @Test
    void resolveCatalogObjectsBeforeNormalizingRemoteNames() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        HMSExternalDatabase db = Mockito.mock(HMSExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.doReturn(db).when(catalog).getDbNullable("HMS_DB");
        Mockito.when(db.getTableNullable("HMS_TABLE")).thenReturn(table);
        Mockito.when(db.getRemoteName()).thenReturn("hms_db");
        Mockito.when(table.getRemoteName()).thenReturn("hms_table");
        HmsNameResolver names = new HmsNameResolver(name -> catalog);
        Assertions.assertEquals(new HmsObjectName("hms_db", "hms_table"),
                names.remoteTable("catalog", "HMS_DB", "HMS_TABLE"));
        Assertions.assertEquals("hms_db", names.remoteDatabase("catalog", "HMS_DB"));
        Assertions.assertThrows(HmsAuthorizationException.class,
                () -> names.remoteTable("catalog", "HMS_DB", "missing"));
        Assertions.assertThrows(HmsAuthorizationException.class,
                () -> names.remoteDatabase("catalog", "missing"));
    }
}
