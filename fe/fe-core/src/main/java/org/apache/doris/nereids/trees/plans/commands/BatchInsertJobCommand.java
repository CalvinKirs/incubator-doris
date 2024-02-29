package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BatchInsertJobCommand extends Command implements ForwardWithSync, NotAllowFallback{
    
    private static final Logger LOG = LogManager.getLogger(BatchInsertJobCommand.class);
    protected BatchInsertJobCommand(PlanType type) {
        super(type);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

    }
}
