package org.apache.flink.runtime.consul.jobregistry;

import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.LogLevel;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.consul.ConsulSessionActivator;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class ConsulRunningJobsRegistryTest {

	private ConsulProcess consul;
	private ConsulClient client;
	private ConsulSessionActivator sessionActivator;
	private String jobRegistryPath = "test-jobregistry/";

	@Before
	public void setup() {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.withLogLevel(LogLevel.DEBUG)
			.build()
			.start();
		client = new ConsulClient("localhost", consul.getHttpPort());
		sessionActivator = new ConsulSessionActivator(client, Executors.newSingleThreadExecutor(), 10);
	}

	@After
	public void cleanup() {
		consul.close();
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
