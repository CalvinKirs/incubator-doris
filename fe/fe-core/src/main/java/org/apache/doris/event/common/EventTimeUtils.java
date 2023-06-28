package org.apache.doris.event.common;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class EventTimeUtils {

    public boolean checkTimeUnit(String timeUnit) {
        return null != IntervalUnit.fromString(timeUnit);
    }

    public static long getIntervalValue(String intervalValue, String intervalUnit) {
        long intervalValueLong = Long.parseLong(intervalValue);
        switch (intervalUnit) {
            case "second":
                return TimeUnit.SECONDS.toMillis(intervalValueLong);
            case "minute":
                return TimeUnit.MINUTES.toMillis(intervalValueLong);
            case "minute_second":
                return TimeUnit.MINUTES.toMillis(intervalValueLong) + TimeUnit.SECONDS.toMillis(intervalValueLong);
            case "hour_second":
                return TimeUnit.HOURS.toMillis(intervalValueLong) + TimeUnit.SECONDS.toMillis(intervalValueLong);
            case "hour_minute":
                return TimeUnit.HOURS.toMillis(intervalValueLong) + TimeUnit.MINUTES.toMillis(intervalValueLong);
            case "hour":
                return TimeUnit.HOURS.toMillis(intervalValueLong);
            case "day_second":
                return TimeUnit.DAYS.toMillis(intervalValueLong) + TimeUnit.SECONDS.toMillis(intervalValueLong);
            case "day_minute":
                return TimeUnit.DAYS.toMillis(intervalValueLong) + TimeUnit.MINUTES.toMillis(intervalValueLong);
            case "day_hour":
                return TimeUnit.DAYS.toMillis(intervalValueLong) + TimeUnit.HOURS.toMillis(intervalValueLong);
            case "day":
                return TimeUnit.DAYS.toMillis(intervalValueLong);
            case "week":
                return TimeUnit.DAYS.toMillis(intervalValueLong * 7);
            case "month":
                return TimeUnit.DAYS.toMillis(intervalValueLong * 30);
            case "quarter":
                return TimeUnit.DAYS.toMillis(intervalValueLong * 90);
            case "year":
                return TimeUnit.DAYS.toMillis(intervalValueLong * 365);
            default:
                return -1;
        }
    }

}
