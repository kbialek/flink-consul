package org.apache.flink.runtime.consul.jobgraph;

import akka.actor.ActorRef;
import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.LogLevel;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobInfo;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.util.FlinkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ConsulSubmittedJobGraphStoreTest {
	private ConsulProcess consul;
	private ConsulClient client;
	private String jobgraphsPath = "test-jobgraphs/";

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
	public void testPutAndRecoverJobGraph() throws Exception {
		ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(client, jobgraphsPath);
		ConsulSubmittedJobGraphStore graphStore2 = new ConsulSubmittedJobGraphStore(client, jobgraphsPath);

		SubmittedJobGraphStore.SubmittedJobGraphListener listener = mock(SubmittedJobGraphStore.SubmittedJobGraphListener.class);

		graphStore1.start(listener);
		graphStore2.start(listener);
		JobID jobID = JobID.generate();

		SubmittedJobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		verify(listener).onAddedJobGraph(jobID);
		graphStore1.stop();

		SubmittedJobGraph recoverJobGraph = graphStore2.recoverJobGraph(jobID);
		assertEquals(jobGraph.getJobId(), recoverJobGraph.getJobId());
		assertNotSame(jobGraph, recoverJobGraph);
	}

	@Test(expected = FlinkException.class)
	public void testPutAndRemoveJobGraph() throws Exception {
		ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(client, jobgraphsPath);

		SubmittedJobGraphStore.SubmittedJobGraphListener listener = mock(SubmittedJobGraphStore.SubmittedJobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

		SubmittedJobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		graphStore1.removeJobGraph(jobID);
		verify(listener).onRemovedJobGraph(jobID);

		graphStore1.recoverJobGraph(jobID);
	}

	@Test
	public void testGetJobIds() throws Exception {
		ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(client, jobgraphsPath);

		SubmittedJobGraphStore.SubmittedJobGraphListener listener = mock(SubmittedJobGraphStore.SubmittedJobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

		SubmittedJobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);

		Collection<JobID> jobIds = graphStore1.getJobIds();
		assertEquals(1, jobIds.size());
		assertEquals(jobID, jobIds.iterator().next());
	}

	private SubmittedJobGraph createJobGraph(JobID jobID) {
		JobGraph jobGraph = new JobGraph(jobID, "test-job");
		JobInfo jobInfo = new JobInfo(ActorRef.noSender(), ListeningBehaviour.DETACHED, 0, 0);
		return new SubmittedJobGraph(jobGraph, jobInfo);
	}
}
