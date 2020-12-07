package com.espro.flink.consul.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

public final class ConsulCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

	private final ConsulClient client;
	private final Configuration configuration;

	public ConsulCheckpointRecoveryFactory(ConsulClient client, Configuration configuration) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.configuration = Preconditions.checkNotNull(configuration, "configuration");
	}

	@Override
	public CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader) throws Exception {
        RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage = new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "completedCheckpoint");

		return new ConsulCompletedCheckpointStore(client, checkpointsPath(), jobId, maxNumberOfCheckpointsToRetain, stateStorage);
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
		return new ConsulCheckpointIDCounter(client, checkpointCountersPath(), jobId);
	}

	private String checkpointCountersPath() {
		return configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT)
			+ configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_CHECKPOINT_COUNTER_PATH);
	}

	private String checkpointsPath() {
		return configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT)
			+ configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_CHECKPOINTS_PATH);
	}

}
