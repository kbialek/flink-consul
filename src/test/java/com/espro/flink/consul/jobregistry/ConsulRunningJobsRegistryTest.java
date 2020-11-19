package com.espro.flink.consul.jobregistry;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;
import com.espro.flink.consul.ConsulSessionActivator;

public class ConsulRunningJobsRegistryTest extends AbstractConsulTest {

	private ConsulClient client;
	private ConsulSessionActivator sessionActivator;
	private String jobRegistryPath = "test-jobregistry/";

	@Before
	public void setup() {
		client = new ConsulClient("localhost", consul.getHttpPort());
		sessionActivator = new ConsulSessionActivator(client, Executors.newSingleThreadExecutor(), 10);
	}

	@Test
	public void testSetJobRunning() throws Exception {
		ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();

		registry.setJobRunning(jobID);
		assertEquals(RunningJobsRegistry.JobSchedulingStatus.RUNNING, registry.getJobSchedulingStatus(jobID));
	}

	@Test
	public void testSetJobFinished() throws Exception {
		ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();

		registry.setJobFinished(jobID);
		assertEquals(RunningJobsRegistry.JobSchedulingStatus.DONE, registry.getJobSchedulingStatus(jobID));
	}

	@Test
	public void testClearJob() throws Exception {
		ConsulRunningJobsRegistry registry = new ConsulRunningJobsRegistry(client, sessionActivator.getHolder(), jobRegistryPath);

		JobID jobID = JobID.generate();

		assertEquals(RunningJobsRegistry.JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jobID));

		registry.setJobRunning(jobID);
		assertEquals(RunningJobsRegistry.JobSchedulingStatus.RUNNING, registry.getJobSchedulingStatus(jobID));

		registry.clearJob(jobID);
		assertEquals(RunningJobsRegistry.JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jobID));
	}
}
