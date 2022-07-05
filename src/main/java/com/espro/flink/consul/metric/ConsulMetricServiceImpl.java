package com.espro.flink.consul.metric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ConsulMetricServiceImpl implements ConsulMetricService {
    private static final Logger LOG = LoggerFactory.getLogger(ConsulMetricServiceImpl.class);

    private static final int HISTORY_SIZE = 100;

    private final MetricRegistry metricRegistry;
    private final ConsulMetricGroup consulMetricGroup;
    private final Counter counter;
    private final Histogram histogram;
    private final Map<String, Metric> metricMap = new HashMap<>();

    public ConsulMetricServiceImpl(MetricRegistry metricRegistry, ConsulMetricGroup consulMetricGroup) {
        this.metricRegistry = metricRegistry;
        this.consulMetricGroup = consulMetricGroup;
        this.counter = new SimpleCounter();
        this.histogram = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        initDefaultMetrics();
    }

    @Override
    public void registerDefaultMetrics() {
        LOG.info("Start registering default consul metrics.");
        this.metricMap.forEach((metricName, metric) -> registerMetrics(metric, metricName));
    }

    @Override
    public void registerMetrics(Metric metric, String metricName) {
        this.metricRegistry.register(metric, metricName, consulMetricGroup);
    }

    @Override
    public void setMetricValues(long requestDurationTime) {
        LOG.debug("Update metrics values.");
        synchronized (this) {
            this.counter.inc();
            this.histogram.update(requestDurationTime);
        }
    }

    private void initDefaultMetrics() {
        metricMap.put("request-count", counter);
        metricMap.put("request-time", histogram);
    }
}
