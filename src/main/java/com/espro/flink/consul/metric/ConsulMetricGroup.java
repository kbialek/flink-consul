package com.espro.flink.consul.metric;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.ComponentMetricGroup;

import java.util.Collections;

public class ConsulMetricGroup extends ComponentMetricGroup<ConsulMetricGroup> {

    public ConsulMetricGroup(MetricRegistry registry, String hostname) {
        super(registry, getScope(registry, hostname), null);
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "consul";
    }

    @Override
    protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        return new QueryScopeInfo.JobManagerQueryScopeInfo();
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup<ConsulMetricGroup>> subComponents() {
        return Collections.emptyList();
    }

    private static String[] getScope(MetricRegistry registry, String hostname) {
        // returning jobmanager scope in order to guarantee backwards compatibility
        // this can be changed once we introduce a proper scope for the process metric group
        return registry.getScopeFormats().getJobManagerFormat().formatScope(hostname);
    }
}
