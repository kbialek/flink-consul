package com.espro.flink.consul.metric;

import org.apache.flink.metrics.Metric;

public interface ConsulMetricService {

    void registerDefaultMetrics();
    void registerMetrics(Metric metric, String metricName);
    void setMetricValues(long requestDurationTime);
}
