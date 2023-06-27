package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;

public class ShowEventSchedulerJobStmt extends ShowStmt {
    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Id")
                    .add("Name")
                    .add("CreateTime")
                    .add("PauseTime")
                    .add("EndTime")
                    .add("LastExecutionFinishTime")
                    .add("DbName")
                    .add("IsMultiTable")
                    .add("State")
                    .add("CurrentTaskNum")
                    .add("Statistic")
                    .add("Progress")
                    .add("ReasonOfStateChanged")
                    .add("ErrorLogUrls")
                    .add("OtherMsg")
                    .add("User")
                    .add("Comment")
                    .build();

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
