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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.TruncateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Map concrete commands instead of guessing INSERT/ALTER subtypes from generic predicates. */
final class HmsOperation {
    final Set<String> privileges;
    final boolean owner;
    final boolean ctas;

    private HmsOperation(Set<String> privileges, boolean owner, boolean ctas) {
        this.privileges = Collections.unmodifiableSet(new HashSet<>(privileges));
        this.owner = owner;
        this.ctas = ctas;
    }

    static HmsOperation resolve(PrivPredicate wanted, LogicalPlan plan) {
        if (wanted == PrivPredicate.SELECT) {
            return new HmsOperation(Collections.singleton("SELECT"), false, false);
        }
        if (wanted == PrivPredicate.DROP) {
            // Hive SQL Standard has no DROP grant; DROPTABLE requires the owner.
            return new HmsOperation(Collections.emptySet(), true, false);
        }
        if (plan instanceof ExplainCommand) {
            plan = ((ExplainCommand) plan).getLogicalPlan();
        }
        if (wanted == PrivPredicate.LOAD) {
            Set<String> privileges = new HashSet<>();
            privileges.add("INSERT");
            if (plan instanceof InsertOverwriteTableCommand) {
                privileges.add("DELETE");
            } else if (plan instanceof CreateTableCommand) {
                // CTAS has a second LOAD check after creating its target. Verify the actual HMS owner.
                return new HmsOperation(Collections.emptySet(), true, true);
            } else if (plan instanceof TruncateTableCommand) {
                // Doris checks LOAD for TRUNCATE; Hive SQL Standard's TRUNCATETABLE requires the owner.
                return new HmsOperation(Collections.emptySet(), true, false);
            } else if (!(plan instanceof InsertIntoTableCommand)) {
                throw new HmsAuthorizationException("HMS authorization: unsupported LOAD command context");
            }
            return new HmsOperation(privileges, false, false);
        }
        if (wanted == PrivPredicate.ALTER && plan instanceof AlterTableCommand) {
            Set<String> privileges = new HashSet<>();
            boolean owner = false;
            for (AlterTableOp op : ((AlterTableCommand) plan).getNereidsOps()) {
                if (op instanceof AddPartitionOp) {
                    privileges.add("INSERT");
                } else if (op instanceof DropPartitionOp) {
                    privileges.add("DELETE");
                } else if (op instanceof AddColumnOp || op instanceof AddColumnsOp || op instanceof DropColumnOp
                        || op instanceof ModifyColumnOp || op instanceof RenameColumnOp || op instanceof RenameTableOp
                        || op instanceof ReorderColumnsOp || op instanceof ModifyTablePropertiesOp) {
                    owner = true;
                } else {
                    throw new HmsAuthorizationException("HMS authorization: unsupported ALTER clause "
                            + op.getClass().getSimpleName());
                }
            }
            if (!owner && privileges.isEmpty()) {
                throw new HmsAuthorizationException("HMS authorization: empty ALTER command");
            }
            return new HmsOperation(privileges, owner, false);
        }
        throw new HmsAuthorizationException("HMS authorization: unsupported privilege or command context");
    }

    static LogicalPlan currentPlan() {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            throw new HmsAuthorizationException("HMS authorization: write requires a Doris statement context");
        }
        StatementBase statement = context.getExecutor() == null ? null : context.getExecutor().getParsedStmt();
        if (statement == null && context.getStatementContext() != null) {
            statement = context.getStatementContext().getParsedStatement();
        }
        if (!(statement instanceof LogicalPlanAdapter)) {
            throw new HmsAuthorizationException("HMS authorization: write requires a Nereids command");
        }
        return ((LogicalPlanAdapter) statement).getLogicalPlan();
    }
}
