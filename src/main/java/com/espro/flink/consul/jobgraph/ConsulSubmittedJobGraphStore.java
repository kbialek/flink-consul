package com.espro.flink.consul.jobgraph;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;

public final class ConsulSubmittedJobGraphStore implements JobGraphStore {

	private final ConsulClient client;
	private final String jobgraphsPath;
    private JobGraphListener listener;

	public ConsulSubmittedJobGraphStore(ConsulClient client, String jobgraphsPath) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.jobgraphsPath = Preconditions.checkNotNull(jobgraphsPath, "jobgraphsPath");
		Preconditions.checkArgument(jobgraphsPath.endsWith("/"), "jobgraphsPath must end with /");
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		this.listener = Preconditions.checkNotNull(jobGraphListener, "jobGraphListener");
	}

	@Override
	public void stop() throws Exception {

	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		byte[] bytes = InstantiationUtil.serializeObject(jobGraph);
        client.setKVBinaryValue(path(jobGraph.getJobID()), bytes);
        this.listener.onAddedJobGraph(jobGraph.getJobID());
	}

	@Override
    public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		GetBinaryValue value = client.getKVBinaryValue(path(jobId)).getValue();
		if (value != null) {
			try {
				return InstantiationUtil.deserializeObject(value.getValue(), Thread.currentThread().getContextClassLoader());
			} catch (Exception e) {
				throw new FlinkException("Could not deserialize SubmittedJobGraph for Job " + jobId.toString(), e);
			}
		} else {
			throw new FlinkException("Could not retrieve SubmittedJobGraph for Job " + jobId.toString());
		}
	}

	@Override
    public void removeJobGraph(JobID jobId) throws Exception {
		client.deleteKVValue(path(jobId));
		listener.onRemovedJobGraph(jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		List<String> value = client.getKVKeysOnly(jobgraphsPath).getValue();
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

    @Override
    public void releaseJobGraph(JobID jobId) throws Exception {
        // TODO Auto-generated method stub

    }
}
