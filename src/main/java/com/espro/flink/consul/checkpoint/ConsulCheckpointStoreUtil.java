/*
 * Copyright (c) SABIO GmbH, Hamburg 2021 - All rights reserved
 */
package com.espro.flink.consul.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.util.Preconditions;

import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

/**
 * {@link CheckpointStoreUtil} implementation for Consul.
 */
public class ConsulCheckpointStoreUtil implements CheckpointStoreUtil {

    private final String checkpointsPath;

    private final JobID jobID;

    /**
     * Creates a {@link ConsulCheckpointStoreUtil} with the given parameters.
     *
     * @param checkpointsPath path in consul where checkpoint information are stored
     * @param jobID identifier of the current job
     * 
     * @see ConsulHighAvailabilityOptions.HA_CONSUL_ROOT
     * @see ConsulHighAvailabilityOptions.HA_CONSUL_CHECKPOINTS_PATH
     */
    public ConsulCheckpointStoreUtil(String checkpointsPath, JobID jobID) {
        this.checkpointsPath = Preconditions.checkNotNull(checkpointsPath, "checkpointsPath");
        Preconditions.checkArgument(checkpointsPath.endsWith("/"), "checkpointsPath must end with /");
        this.jobID = Preconditions.checkNotNull(jobID, "jobID");
    }

    @Override
    public String checkpointIDToName(long checkpointId) {
        return jobPath() + checkpointId;
    }

    @Override
    public long nameToCheckpointID(String name) {
        return Long.parseLong(name.replace(jobPath(), ""));
    }

    private String jobPath() {
        return checkpointsPath + jobID.toString();
    }

}
