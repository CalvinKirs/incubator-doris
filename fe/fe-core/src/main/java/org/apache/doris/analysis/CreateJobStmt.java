package org.apache.doris.analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.event.common.IntervalUnit;

@Slf4j
public class CreateJobStmt extends DdlStmt {


    private String jobName;

    private boolean isSchedulerCycle;
    private String schedulerTimeStamp;

    private long schedulerInterval;

    private IntervalUnit schedulerIntervalUnit;

    private String startsTimeStamp;

    private long startsInterval;

    private IntervalUnit startsIntervalUnit;

    private String endsTimeStamp;
    private long endsInterval;

    private String endsIntervalUnit;

    private String executeSql;

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
    }

    public CreateJobStmt(String jobName, boolean isSchedulerCycle, String schedulerTimeStamp, long schedulerInterval, IntervalUnit schedulerIntervalUnit, String startsTimeStamp, long startsInterval, IntervalUnit startsIntervalUnit, String endsTimeStamp, long endsInterval, IntervalUnit endsIntervalUnit, String executeSql) {
        this.jobName = jobName;
        //this.isSchedulerCycle = isSchedulerCycle;
        this.schedulerTimeStamp = schedulerTimeStamp;
        this.schedulerInterval = schedulerInterval;
        this.schedulerIntervalUnit = schedulerIntervalUnit;
        this.startsTimeStamp = startsTimeStamp;
        this.startsInterval = startsInterval;
        this.startsIntervalUnit = startsIntervalUnit;
        this.endsTimeStamp = endsTimeStamp;
        this.endsInterval = endsInterval;
        this.executeSql = executeSql;
    }


    private void checkStartTime() throws UserException {
        if (StringUtils.isNotBlank(startsTimeStamp)) {

        }
    }

    private void checkEndTime() throws UserException {
        if (StringUtils.isNotBlank(endsTimeStamp)) {
            TimeStamp timeStamp = new TimeStamp(endsTimeStamp);
            if (timeStamp.getTime() <= 0) {
                throw new UserException("Ends time must be greater than current time");
            }
            if (endsInterval > 0 && StringUtils.isNotBlank(endsIntervalUnit))){
                throw new UserException("Ends interval unit must be not null");
            }
        }


    }

    private void checkExecuteSql() throws UserException {
        if (StringUtils.isBlank(executeSql)) {
            throw new UserException("Execute sql must be not null");
        }
        // need to be check sql syntax
    }
}
