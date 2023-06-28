package org.apache.doris.event.common;

import java.util.Arrays;
import java.util.function.Function;

public enum JobConfiguration {

    EVENT_NAME("event_name", null, value -> value.replace(" ", "")),
    SCHEDULER_INTERVAL_VALUE("interval_value", -1, Integer::parseInt),
    SCHEDULER_INTERVAL_UNIT("interval_unit", null, value -> value.replace(" ", "")),
    START_TIME("starts", null, value -> value.replace(" ", "")),
    START_TIME_INTERVAL_VALUE("start_time_interval_value", -1, Integer::parseInt),
    START_TIME_INTERVAL_UNIT("start_time_interval_unit", null,
            value -> value.replace(" ", "")),
    END_TIME("ends", null, value -> value.replace(" ", "")),
    END_TIME_INTERVAL_VALUE("end_time_interval_value", -1, Integer::parseInt),
    END_TIME_INTERVAL_UNIT("end_time_interval_unit", null,
            value -> value.replace(" ", "")),

    EXECUTE_SQL("execute_sql", null, value -> value.replace(" ", ""));

    private final String name;

    public String getName() {
        return name;
    }

    private final Object defaultValue;

    private final Function<String, Object> converter;

    <T> JobConfiguration(String name, T defaultValue, Function<String, T> converter) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.converter = (Function<String, Object>) converter;
    }

    JobConfiguration getByName(String name) {
        return Arrays.stream(JobConfiguration.values())
                .filter(config -> config.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown configuration " + name));
    }


    public <T> T getParameterValue(String param) {
        Object value = param != null ? converter.apply(param) : defaultValue;
        return (T) value;
    }

}
