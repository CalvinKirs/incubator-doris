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
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import java.util.HashMap;
import java.util.Map;

/** INSERT parsing selects a sink using the catalog type, but does not load remote metadata. */
final class HmsTestPlans {
    static LogicalPlan parse(String sql) {
        ConnectContext previous = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
        try {
            if (Env.getCurrentEnv().getCatalogMgr().getCatalog("hms") == null) {
                CatalogLog catalog = new CatalogLog();
                catalog.setCatalogId(987654321L);
                catalog.setCatalogName("hms");
                Map<String, String> properties = new HashMap<>();
                properties.put("type", "hms");
                properties.put("hive.metastore.uris", "thrift://localhost:1");
                catalog.setProps(properties);
                Env.getCurrentEnv().getCatalogMgr().replayCreateCatalog(catalog);
            }
            return new NereidsParser().parseSingle(sql);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse test HMS command", e);
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
