package org.apache.doris.job.extensions.insert;


import lombok.Data;
import org.apache.doris.job.common.TaskStatus;

@Data
public class InsertTaskInfo {
    
    private Long taskId;
    
    private TaskStatus taskStatus;
    
    private Long createTime;
    
    private String errorMsg;

    public InsertTaskInfo(Long taskId, TaskStatus taskStatus, Long createTime) {
        this.taskId = taskId;
        this.taskStatus = taskStatus;
        this.createTime = createTime;
    }
}
