package com.espro.flink.consul.utils;

import java.time.Duration;
import java.time.LocalDateTime;

public class TimeUtils {

    public static long getDurationTime(LocalDateTime startTime) {
        LocalDateTime currentTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, currentTime);
        return duration.toMillis();
    }
}
