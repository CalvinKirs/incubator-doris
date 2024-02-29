package org.apache.doris.job.scheduler;


import lombok.Data;
import org.apache.doris.job.common.JobType;

import java.util.List;

@Data
public class TaskDispatchEvent {
    
    private Long lastCompletedTaskId;
    
    private Long groupId;
    
    private JobType jobType;
    
    private TaskDispatchOperate taskDispatchOperate;
    
    private List<Long> groupIds;
    
    

    public TaskDispatchEvent(Long lastCompletedTaskId,Long groupId, TaskDispatchOperate taskDispatchOperate) {
        this.lastCompletedTaskId = lastCompletedTaskId;
        this.groupId = groupId;
        this.taskDispatchOperate = taskDispatchOperate;
    }
    
    public TaskDispatchEvent(Long lastCompletedTaskId, List<Long> groupIds, TaskDispatchOperate taskDispatchOperate) {
        this.lastCompletedTaskId = lastCompletedTaskId;
        this.taskDispatchOperate = taskDispatchOperate;
        this.groupIds = groupIds;
    }


}
