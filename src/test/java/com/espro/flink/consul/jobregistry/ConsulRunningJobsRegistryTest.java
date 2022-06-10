package com.espro.flink.consul.jobregistry;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;
import com.espro.flink.consul.ConsulSessionActivator;

import java.util.Set;
import java.util.stream.Collectors;

public class ConsulRunningJobsRegistryTest extends AbstractConsulTest {

	private ConsulClient client;
	private ConsulSessionActivator sessionActivator;
	private String jobRegistryPath = "test-jobregistry/";

	@Before
	public void setup() {
		client = new ConsulClient("localhost", consul.getHttpPort());
        sessionActivator = new ConsulSessionActivator(() -> client, 10);
	}

	@Test
	public void testSetJobRunning() throws Exception {
        ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(() -> client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();
		registry.createDirtyResult(createJobResult(jobID));
		assertEquals(true, registry.hasDirtyJobResultEntry(jobID));
	}

	@Test
	public void testSetJobFinished() throws Exception {
        ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(() -> client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();
		registry.markResultAsClean(jobID);
		assertEquals(true, registry.hasCleanJobResultEntry(jobID));
	}

	@Test
	public void testClearJob() throws Exception {
        ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(() -> client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();

		registry.createDirtyResult(createJobResult(jobID));
		assertEquals(true, registry.hasDirtyJobResultEntry(jobID));

		registry.markResultAsClean(jobID);
		assertEquals(true, registry.hasCleanJobResultEntry(jobID));

		JobID jobID2 = JobID.generate();
		registry.markResultAsClean(jobID2);
		assertEquals(true, registry.hasCleanJobResultEntry(jobID2));
		assertEquals(true, registry.hasCleanJobResultEntry(jobID));
	}

	@Test
	public void testGetDirtyJobs() throws Exception {
		ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(() -> client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();
		registry.createDirtyResult(createJobResult(jobID));

		JobID jobID2 = JobID.generate();
		registry.createDirtyResult(createJobResult(jobID2));

		JobID jobID3 = JobID.generate();
		registry.markResultAsClean(jobID3);

		Set<JobResult> dirtyResults = registry.getDirtyResults();
		Set<String> jobIds = dirtyResults.stream().map(jobResult -> jobResult.getJobId().toString()).collect(Collectors.toSet());

		assertEquals(2, dirtyResults.size());
		assertEquals(true, jobIds.contains(jobID.toString()));
		assertEquals(true, jobIds.contains(jobID2.toString()));
	}

	private JobResultEntry createJobResult(JobID jobID) {
		JobResult jobResult = new JobResult.Builder().jobId(jobID).netRuntime(1).build();
		return new JobResultEntry(jobResult);
	}
}
