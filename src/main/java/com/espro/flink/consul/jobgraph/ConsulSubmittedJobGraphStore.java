package com.espro.flink.consul.jobgraph;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;


/**
 * Stores the state of the job graph to the configured HA storage directory and only a pointer (RetrievableStateHandle) of the state to
 * Consul.
 *
 * @see RetrievableStateHandle
 * @see FileSystemStateStorageHelper
 * @see HighAvailabilityServicesUtils#getClusterHighAvailableStoragePath(Configuration)
 */
public final class ConsulSubmittedJobGraphStore implements JobGraphStore {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulSubmittedJobGraphStore.class);

    private final Supplier<ConsulClient> client;
	private final String jobgraphsPath;
    private final RetrievableStateStorageHelper<JobGraph> jobGraphStateStorage;
    private JobGraphListener listener;

    public ConsulSubmittedJobGraphStore(Configuration configuration, Supplier<ConsulClient> client, String jobgraphsPath)
            throws IOException {
		this.client = Preconditions.checkNotNull(client, "client");
		this.jobgraphsPath = Preconditions.checkNotNull(jobgraphsPath, "jobgraphsPath");
        Preconditions.checkArgument(jobgraphsPath.endsWith("/"), "jobgraphsPath must end with /");
        this.jobGraphStateStorage = new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "jobGraph");
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		this.listener = Preconditions.checkNotNull(jobGraphListener, "jobGraphListener");
	}

	@Override
	public void stop() throws Exception {
        // Nothing to do here
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
        RetrievableStateHandle<JobGraph> stateHandle = jobGraphStateStorage.store(jobGraph);

        boolean success = false;
        try {
            // Write state handle (not the actual state) to Consul. This is expected to be
            // smaller than the state itself.
            byte[] bytes = InstantiationUtil.serializeObject(stateHandle);
            LOG.debug("{} bytes will be written to Consul.", bytes.length);
            Boolean response = client.get().setKVBinaryValue(path(jobGraph.getJobID()), bytes).getValue();
            success = response == null ? false : response;
        } finally {
            // Cleanup the state handle if it was not written to Consul
            if (!success) {
                stateHandle.discardState();
            }
        }
        this.listener.onAddedJobGraph(jobGraph.getJobID());
	}

	@Override
    public JobGraph recoverJobGraph(JobID jobId) throws Exception {
        return getStateHandle(jobId).retrieveState();
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug("Releasing job graph {}.", jobId);
                    removeJobGraph(jobId);
                    LOG.info("Released job graph {} .", jobId);
                },
                executor);
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug("Releasing job graph {}.", jobId);
                    removeJobGraph(jobId);
                    LOG.info("Released job graph {} .", jobId);
                },
                executor);
    }

    private CompletableFuture<Void> runAsyncWithLockAssertRunning(
            ThrowingRunnable<Exception> runnable, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                        try {
                            runnable.run();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                },
                executor);
    }

    private RetrievableStateHandle<JobGraph> getStateHandle(JobID jobId) throws FlinkException {
        GetBinaryValue value = client.get().getKVBinaryValue(path(jobId)).getValue();
		if (value != null) {
			try {
                return InstantiationUtil.deserializeObject(value.getValue(),
                        Thread.currentThread().getContextClassLoader());
			} catch (Exception e) {
				throw new FlinkException("Could not deserialize SubmittedJobGraph for Job " + jobId.toString(), e);
			}
		} else {
			throw new FlinkException("Could not retrieve SubmittedJobGraph for Job " + jobId.toString());
		}
    }

    public void removeJobGraph(JobID jobId) throws Exception {
        RetrievableStateHandle<JobGraph> stateHandle = null;
        try {
            stateHandle = getStateHandle(jobId);
        } catch (FlinkException e) {
            LOG.warn("Could not retrieve the state handle from Consul {}.", path(jobId), e);
        }

        // First remove state from Consul (Independent of errors when reading the state handler)
        client.get().deleteKVValue(path(jobId));

        if (stateHandle != null) {
            stateHandle.discardState();
        }

		listener.onRemovedJobGraph(jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
        List<String> value = client.get().getKVKeysOnly(jobgraphsPath).getValue();
		if (value != null) {
			return value.stream()
				.map(id -> id.split("/"))
				.map(parts -> parts[parts.length - 1])
				.map(JobID::fromHexString).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private String path(JobID jobID) {
		return jobgraphsPath + jobID.toString();
	}
}
