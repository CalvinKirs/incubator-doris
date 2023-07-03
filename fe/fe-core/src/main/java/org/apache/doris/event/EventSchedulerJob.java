package org.apache.doris.event;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.UserException;
import org.apache.doris.event.common.IntervalUnit;
import org.apache.doris.event.common.JobConfiguration;

import java.util.Map;

public class EventSchedulerJob {

    private String jobName;

    private boolean isSchedulerCycle;
    private String schedulerTimeStamp;

    private long schedulerInterval;

    private IntervalUnit schedulerIntervalUnit;

    private String startsTimeStamp;


    private Long startsInterval;

    private IntervalUnit startsIntervalUnit;

    private String endsTimeStamp;
    private long endsInterval;

    private IntervalUnit endsIntervalUnit;

    private String executeSql;


    public void analyze() {
        // nothing need to do at now

    }

    public void setStartTime(Map<String, String> params) {
        if (params.containsKey(JobConfiguration.START_TIME.getName())) {
            if (StringUtils.isNotBlank(params.get(JobConfiguration.START_TIME.getName()))) {
                this.startsTimeStamp = params.get(JobConfiguration.START_TIME.getName());
                if (startsTimeStamp <= 0) {
                    throw new UserException("Starts time must be greater than 0");
                }
            }
        }
    }

    public void setJobName() throws UserException {
        // should check job name is valid
        if (StringUtils.isBlank(jobName)) {
            throw new UserException("Job name is blank");
        }
    }

    private void checkInterval(long interval) throws UserException {
        if (schedulerInterval <= 0) {
            throw new UserException("Scheduler interval must be greater than 0");
        }
        if (startsInterval <= 0) {
            throw new UserException("Starts interval must be greater than 0");
        }
        if (endsInterval <= 0) {
            throw new UserException("Ends interval must be greater than 0");
        }
    }


}
