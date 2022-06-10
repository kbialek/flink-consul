package com.espro.flink.consul.checkpoint;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

public final class ConsulCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

    private final Supplier<ConsulClient> client;
	private final Configuration configuration;
	private final Executor executor;

	public ConsulCheckpointRecoveryFactory(Supplier<ConsulClient> client, Configuration configuration, Executor executor) {
		this.executor = executor;
		this.client = Preconditions.checkNotNull(client, "client");
		this.configuration = Preconditions.checkNotNull(configuration, "configuration");
	}

	private CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, SharedStateRegistryFactory sharedStateRegistryFactory, Executor ioExecutor, RestoreMode restoreMode) throws Exception {
        RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage = new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "completedCheckpoint");

        ConsulStateHandleStore<CompletedCheckpoint> consulStateHandleStore = new ConsulStateHandleStore<>(client, stateStorage,
                checkpointsPath());
        ConsulCheckpointStoreUtil consulCheckpointStoreUtil = new ConsulCheckpointStoreUtil(checkpointsPath(), jobId);

		Collection<CompletedCheckpoint> completedCheckpoints = DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
				consulStateHandleStore, consulCheckpointStoreUtil);
		SharedStateRegistry sharedStateRegistry = sharedStateRegistryFactory.create(ioExecutor, completedCheckpoints, restoreMode);

		return new DefaultCompletedCheckpointStore<>(maxNumberOfCheckpointsToRetain, consulStateHandleStore, consulCheckpointStoreUtil, completedCheckpoints, sharedStateRegistry,
                executor);
	}

	@Override
	public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, SharedStateRegistryFactory sharedStateRegistryFactory, Executor ioExecutor, RestoreMode restoreMode) throws Exception {
    	return createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, sharedStateRegistryFactory, ioExecutor, restoreMode);
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
