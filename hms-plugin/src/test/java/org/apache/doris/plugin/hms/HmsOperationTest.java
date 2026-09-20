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

import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class HmsOperationTest {
    @Test
    void partitionClausesDoNotUseGenericAlterOwnership() {
        NereidsParser parser = new NereidsParser();
        HmsOperation add = HmsOperation.resolve(PrivPredicate.ALTER,
                parser.parseSingle("alter table hms.sales.orders add partition p values less than (10)"));
        Assertions.assertEquals(Collections.singleton("INSERT"), add.privileges);
        Assertions.assertFalse(add.owner);
        HmsOperation drop = HmsOperation.resolve(PrivPredicate.ALTER,
                parser.parseSingle("alter table hms.sales.orders drop partition p"));
        Assertions.assertEquals(Collections.singleton("DELETE"), drop.privileges);
        Assertions.assertFalse(drop.owner);
    }

    @Test
    void dropAndTruncateAreOwnerOperationsLikeHiveSqlStandard() {
        HmsOperation drop = HmsOperation.resolve(PrivPredicate.DROP, null);
        Assertions.assertTrue(drop.owner);
        Assertions.assertTrue(drop.privileges.isEmpty());
        Assertions.assertFalse(drop.ctas);
        HmsOperation truncate = HmsOperation.resolve(PrivPredicate.LOAD,
                HmsTestPlans.parse("truncate table hms.sales.orders"));
        Assertions.assertTrue(truncate.owner);
        Assertions.assertTrue(truncate.privileges.isEmpty());
        Assertions.assertFalse(truncate.ctas);
    }

    @Test
    void explainRetainsOverwriteRequirementsAndUnknownLoadFails() {
        HmsOperation explain = HmsOperation.resolve(PrivPredicate.LOAD,
                HmsTestPlans.parse("explain insert overwrite table hms.sales.orders select 1"));
        Assertions.assertTrue(explain.privileges.contains("DELETE"));
        Assertions.assertThrows(HmsAuthorizationException.class, () -> HmsOperation.resolve(PrivPredicate.LOAD,
                HmsTestPlans.parse("delete from hms.sales.orders where id = 1")));
    }

    @Test
    void executorCommandIsAvailableWhenStatementContextHasNoParsedStatement() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            LogicalPlan plan = HmsTestPlans.parse("insert overwrite table hms.sales.orders select 1");
            StmtExecutor executor = Mockito.mock(StmtExecutor.class);
            Mockito.when(executor.getParsedStmt()).thenReturn(new LogicalPlanAdapter(plan, new StatementContext()));
            context.setExecutor(executor);
            Assertions.assertSame(plan, HmsOperation.currentPlan());
            context.setExecutor(null);
            StatementContext statement = new StatementContext();
            statement.setParsedStatement(new LogicalPlanAdapter(plan, statement));
            context.setStatementContext(statement);
            Assertions.assertSame(plan, HmsOperation.currentPlan());
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

}
