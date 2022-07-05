package com.espro.flink.consul.checkpoint;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.espro.flink.consul.metric.ConsulMetricService;
import com.espro.flink.consul.utils.TimeUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import org.apache.flink.util.concurrent.FutureUtils;

final class ConsulCheckpointIDCounter implements CheckpointIDCounter {

    private final Supplier<ConsulClient> clientProvider;
	private final String countersPath;
	private final JobID jobID;
	private long index;
	private final ConsulMetricService consulMetricService;

    public ConsulCheckpointIDCounter(Supplier<ConsulClient> clientProvider, String countersPath, JobID jobID, ConsulMetricService consulMetricService) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
		this.countersPath = Preconditions.checkNotNull(countersPath, "countersPath");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
		Preconditions.checkArgument(countersPath.endsWith("/"), "countersPath must end with /");
		this.consulMetricService = consulMetricService;
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
		if (jobStatus.isGloballyTerminalState()) {
			removeCounter();
		}
		return FutureUtils.completedVoidFuture();
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (true) {
            long v = get();
			if (writeCounter(v + 1)) {
				return v;
			} else {
				Thread.sleep(100);
			}
		}
	}

    @Override
    public long get() {
		LocalDateTime startTime = LocalDateTime.now();
        GetValue gv = clientProvider.get().getKVValue(counterKey()).getValue();
		setMetricValues(startTime);
        if (gv == null) {
            index = 0;
            return 0;
        } else {
            index = gv.getModifyIndex();
            return Long.valueOf(gv.getDecodedValue());
        }
    }

	@Override
	public void setCount(long newId) throws Exception {
		writeCounter(newId);
	}

	private boolean writeCounter(long value) {
		PutParams params = new PutParams();
		params.setCas(index);
		LocalDateTime startTime = LocalDateTime.now();
		boolean result = clientProvider.get().setKVValue(counterKey(), String.valueOf(value), params).getValue();
		setMetricValues(startTime);
		return result;
	}

	private void removeCounter() {
        clientProvider.get().deleteKVValue(counterKey());
	}

	private String counterKey() {
		return countersPath + jobID.toString();
	}

	private void setMetricValues(LocalDateTime requestStartTime) {
		long durationTime = TimeUtils.getDurationTime(requestStartTime);
		if (consulMetricService != null) {
			this.consulMetricService.setMetricValues(durationTime);
		}
	}
}
