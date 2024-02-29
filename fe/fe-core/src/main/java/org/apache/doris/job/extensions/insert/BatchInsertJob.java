package org.apache.doris.job.extensions.insert;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Batch insert job,usually used for batch insert data
 */
public class BatchInsertJob extends InsertJob {


    @SerializedName(value = "sc")
    private int shardCount;

    @SerializedName(value = "sk")
    private String shardKey;

    @SerializedName(value = "ll")
    private long lowerLimit;

    @SerializedName(value = "ul")
    private long upperLimit;


    public BatchInsertJob(String jobName, JobStatus jobStatus, String dbName, String comment, UserIdentity createUser, JobExecutionConfiguration jobConfig, Long createTimeMs, String executeSql, int shardCount, String shardKey, long lowerLimit, long upperLimit) {
        super(jobName, jobStatus, dbName, comment, createUser, jobConfig, createTimeMs, executeSql);
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;
        this.shardCount = shardCount;
        this.shardKey = shardKey;

    }

    @Override
    protected void checkJobParamsInternal() {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shard count should be greater than 0");
        }
        if (lowerLimit >= upperLimit) {
            throw new IllegalArgumentException("lower limit should be less than upper limit");
        }
        if (StringUtils.isBlank(shardKey)) {
            throw new IllegalArgumentException("shard key should not be empty");
        }
        
        super.checkJobParamsInternal();
        if(getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)){
            return;
        }
        throw new IllegalArgumentException("BatchInsertJob only support INSTANT execute type");
    }

    @Override
    public List<InsertTask> createTasks(TaskType taskType, Map<Object, Object> taskContext,Long groupId) {
        String originalSQL = getExecuteSql();
        List<String> shardedSQLs = SQLSharder.generateShardedSQLs(originalSQL, shardKey, shardCount, lowerLimit, upperLimit);
        List<InsertTask> tasks = new ArrayList<>();
        for (String shardedSQL : shardedSQLs) {
            InsertTask task = new InsertTask(getCurrentDbName(), shardedSQL, getCreateUser());
            tasks.add(task);
        }
        super.initTasks(tasks, taskType,groupId);
        getTasks().addAll(tasks);
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
 
        return tasks;
    }
    //split task


    @Override
    public void onTaskSuccess(InsertTask task) throws JobException {
        //should trigger next task
        // we need to check if all task is finished
        super.onTaskSuccess(task);
    }
}
