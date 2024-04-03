package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.BatchInsertJobInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

public class CreateBatchInsertJobCommand extends Command implements ForwardWithSync, NotAllowFallback{
    public CreateBatchInsertJobCommand(BatchInsertJobInfo batchInsertJobInfo) {
        super(PlanType.CREATE_BATCH_INSERT_JOB_COMMAND);
        
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

    }
}
