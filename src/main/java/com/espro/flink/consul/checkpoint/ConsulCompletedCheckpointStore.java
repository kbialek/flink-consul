package com.espro.flink.consul.checkpoint;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;

/**
 * Completed checkpoints are stored using the {@link RetrievableStateStorageHelper} and only a pointer to this checkpoint is stored in
 * Consul.
 */
final class ConsulCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulCompletedCheckpointStore.class);

	private final ConsulClient client;
	private final String checkpointsPath;
	private JobID jobID;
	private final int maxCheckpoints;
	private final RetrievableStateStorageHelper<CompletedCheckpoint> storage;

	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;
    private final Executor executor;

	public ConsulCompletedCheckpointStore(ConsulClient client, String checkpointsPath, JobID jobID, int maxCheckpoints,
            RetrievableStateStorageHelper<CompletedCheckpoint> storage, Executor executor) {
        this.client = Preconditions.checkNotNull(client, "client");
		this.checkpointsPath = Preconditions.checkNotNull(checkpointsPath, "checkpointsPath");
		Preconditions.checkArgument(checkpointsPath.endsWith("/"), "checkpointsPath must end with /");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
		this.storage = Preconditions.checkNotNull(storage, "storage");
		Preconditions.checkState(maxCheckpoints > 0, "maxCheckpoints must be > 0");
		this.maxCheckpoints = maxCheckpoints;
		this.completedCheckpoints = new ArrayDeque<>(maxCheckpoints + 1);
        this.executor = executor;
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		writeCheckpoint(checkpoint);
		completedCheckpoints.add(checkpoint);

		if (completedCheckpoints.size() > maxCheckpoints) {
			removeCheckpoint(completedCheckpoints.removeFirst());
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return Lists.newArrayList(completedCheckpoints);
	}

	@Override
	public void recover() throws Exception {
		List<String> checkpointPaths = client.getKVKeysOnly(jobPath()).getValue();
		if (checkpointPaths != null) {
			checkpointPaths.sort(Comparator.naturalOrder());
			List<RetrievableStateHandle<CompletedCheckpoint>> stateHandles = readCheckpointStateHandlesFromConsul(checkpointPaths);

			LOG.info("Trying to recover Job {} checkpoints from storage", jobID);

			List<CompletedCheckpoint> prevCheckpoints;
			List<CompletedCheckpoint> checkpoints = null;
			int attempts = 10;
			do {
				prevCheckpoints = checkpoints;
				try {
					checkpoints = readCheckpointsFromStorage(stateHandles);
				} catch (IllegalStateException e) {
					LOG.warn(String.format("Exception when reading Job %s checkpoints from storage", jobID.toString()), e.getCause());
				}
			} while (attempts-- > 0 && (
				prevCheckpoints == null
					|| checkpoints.size() != stateHandles.size()
                    /*|| !checkpoints.equals(prevCheckpoints)*/));

			if (attempts > 0 && checkpoints != null) {
				completedCheckpoints.clear();
				completedCheckpoints.addAll(checkpoints);
			} else {
				throw new FlinkException(String.format("Failed to recover Job %s checkpoints", jobID.toString()));
			}
		}
	}

	private List<RetrievableStateHandle<CompletedCheckpoint>> readCheckpointStateHandlesFromConsul(List<String> checkpointPaths) {
        return checkpointPaths.stream()
                .map(this::readCheckpointStateHandleFromConsul)
                .collect(Collectors.toList());
	}

    private RetrievableStateHandle<CompletedCheckpoint> readCheckpointStateHandleFromConsul(String path) {
        try {
        	GetBinaryValue binaryValue = client.getKVBinaryValue(path).getValue();

        	return InstantiationUtil.<RetrievableStateHandle<CompletedCheckpoint>>deserializeObject(
        		binaryValue.getValue(),
        		Thread.currentThread().getContextClassLoader()
        	);
        } catch (IOException | ClassNotFoundException e) {
        	throw new IllegalStateException(e);
        }
    }

    private List<CompletedCheckpoint> readCheckpointsFromStorage(List<RetrievableStateHandle<CompletedCheckpoint>> stateHandles) {
        return stateHandles.stream().map(sh -> {
            try {
                return sh.retrieveState();
            } catch (IOException | ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());
    }

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		completedCheckpoints.forEach(this::removeCheckpoint);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxCheckpoints;
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
	}

	private String jobPath() {
		return checkpointsPath + jobID.toString();
	}

	private void writeCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
        String key = createCheckpointKeyForConsul(checkpoint);

		RetrievableStateHandle<CompletedCheckpoint> storeHandle = storage.store(checkpoint);

		byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);
		boolean success = false;
		try {
			success = client.setKVBinaryValue(key, serializedStoreHandle).getValue();
        } catch (Exception e) {
            LOG.warn("Writing of checkpoint to Consul failed.", e);
		}
		if (!success) {
			// cleanup if data was not stored in Consul
			if (storeHandle != null) {
				storeHandle.discardState();
			}
		}
	}

	private void removeCheckpoint(CompletedCheckpoint checkpoint) {
        String key = createCheckpointKeyForConsul(checkpoint);
        RetrievableStateHandle<CompletedCheckpoint> stateHandle = readCheckpointStateHandleFromConsul(key);

        try {
            client.deleteKVValue(key);
            stateHandle.discardState();
        } catch (Exception e) {
            LOG.warn("Error while deleting state handle for checkpoint {}", key, e);
            return;
        }

        // Should only be executed if removal of state handle was successful
        executor.execute(() -> {
            try {
                checkpoint.discardOnSubsume();
            } catch (Exception e) {
                LOG.warn("Fail to subsume the old checkpoint.", e);
            }
        });
	}

    private String createCheckpointKeyForConsul(CompletedCheckpoint checkpoint) {
        return jobPath() + checkpoint.getCheckpointID();
    }
}
