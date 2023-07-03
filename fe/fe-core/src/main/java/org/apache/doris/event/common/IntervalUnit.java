package org.apache.doris.event.common;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public enum IntervalUnit {
    SECOND("second", 0L, v -> TimeUnit.SECONDS.toMillis(Long.parseLong(v))),
    MINUTE("minute", 0L, v -> TimeUnit.MINUTES.toMillis(Long.parseLong(v))),
    HOUR("hour", 0L, v -> TimeUnit.HOURS.toMillis(Long.parseLong(v))),
    DAY("day", 0L, v -> TimeUnit.DAYS.toMillis(Long.parseLong(v))),
    WEEK("week", 0L, v -> TimeUnit.DAYS.toMillis(Long.parseLong(v) * 7));
    private final String unit;

    public String getUnit() {
        return unit;
    }

    public static IntervalUnit fromString(String unit) {
        for (IntervalUnit u : IntervalUnit.values()) {
            if (u.unit.equalsIgnoreCase(unit)) {
                return u;
            }
        }
        return null;
    }

    private final Object defaultValue;

    private final Function<String, Object> converter;

    <T> IntervalUnit(String unit, T defaultValue, Function<String, T> converter) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.converter = (Function<String, Object>) converter;
    }

    IntervalUnit getByName(String name) {
        return Arrays.stream(IntervalUnit.values())
                .filter(config -> config.getUnit().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown configuration " + name));
    }

    public <T> T getParameterValue(String param) {
        Object value = param != null ? converter.apply(param) : defaultValue;
        return (T) value;
    }
}
