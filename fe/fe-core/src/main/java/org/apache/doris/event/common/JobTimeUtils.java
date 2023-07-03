package org.apache.doris.event.common;

import java.util.concurrent.TimeUnit;

public class JobTimeUtils {

    public boolean checkTimeUnit(String timeUnit) {
        return null != IntervalUnit.fromString(timeUnit);
    }
    
    
    public long getIntervalTime(String timeUnit, String timeValue) {
        this.checkTimeUnit(timeUnit);
        IntervalUnit intervalUnit = IntervalUnit.fromString(timeUnit);
        assert intervalUnit != null;
        return intervalUnit.getParameterValue(timeValue);
    }

}
