package org.apache.doris.event;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.event.common.EventSchedulerJobConfiguration;

import java.util.Map;

public class EventSchedulerJob {
    
    private Long startsTimeStampMs;

    
    public void analyze(Map<String,String> params) {
        // nothing need to do at now
       
    }
    
    public void setStartTime(Map<String,String> params) {
        this.startsTimeStampMs = 0L;
        if(StringUtils.isNotBlank(params.get(EventSchedulerJobConfiguration.START_TIME.getName()))) {
            startsTimeStampMs = Long.parseLong(params.get(EventSchedulerJobConfiguration.START_TIME.getName()));
        } 
    }
        
    }
    
}
