package org.apache.flink.runtime.consul.checkpoint;

import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.LogLevel;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class ConsulCheckpointIDCounterTest {

	private ConsulProcess consul;
	private ConsulClient client;
	private String countersPath = "test-counters/";

	@Before
	public void setup() {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.withLogLevel(LogLevel.DEBUG)
			.build()
			.start();
		client = new ConsulClient("localhost", consul.getHttpPort());
	}

	@After
	public void cleanup() {
		consul.close();
	}

	@Test
	public void testGetAndIncrement() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(client, countersPath, jobID);
		counter.start();
		assertEquals(0, counter.getAndIncrement());
		assertEquals(1, counter.getAndIncrement());
		counter.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testSetCount() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(client, countersPath, jobID);
		counter.start();
		counter.setCount(3);
		assertEquals(3, counter.getAndIncrement());
		counter.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testSharedAccess() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCheckpointIDCounter counter1 = new ConsulCheckpointIDCounter(client, countersPath, jobID);
		ConsulCheckpointIDCounter counter2 = new ConsulCheckpointIDCounter(client, countersPath, jobID);

		counter1.start();
		counter2.start();

		assertEquals(0, counter1.getAndIncrement());
		assertEquals(1, counter1.getAndIncrement());
		assertEquals(2, counter2.getAndIncrement());

		counter1.shutdown(JobStatus.FINISHED);
		counter2.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testConcurrentAccess() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCheckpointIDCounter counter1 = new ConsulCheckpointIDCounter(client, countersPath, jobID);
		ConsulCheckpointIDCounter counter2 = new ConsulCheckpointIDCounter(client, countersPath, jobID);

		counter1.start();
		counter2.start();

		Runnable task1 = () -> {
			try {
				for (int i = 0; i < 10; i++) {
					counter1.getAndIncrement();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		Runnable task2 = () -> {
			try {
				for (int i = 0; i < 10; i++) {
					counter2.getAndIncrement();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		Executor executor = Executors.newFixedThreadPool(2);
		executor.execute(task1);
		executor.execute(task2);

		Thread.sleep(1000);

		assertEquals(20, counter1.getAndIncrement());
		assertEquals(21, counter2.getAndIncrement());

		counter1.shutdown(JobStatus.FINISHED);
		counter2.shutdown(JobStatus.FINISHED);
	}


	@Test
	public void testStop() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(client, countersPath, jobID);
		counter.start();
		counter.getAndIncrement();
		counter.getAndIncrement();
		counter.shutdown(JobStatus.FINISHED);
		assertEquals(0, counter.getAndIncrement());
	}
}
