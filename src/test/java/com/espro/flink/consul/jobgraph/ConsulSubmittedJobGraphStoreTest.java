package com.espro.flink.consul.jobgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.FlinkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;

public class ConsulSubmittedJobGraphStoreTest extends AbstractConsulTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ConsulClient client;
    private Configuration configuration;
	private String jobgraphsPath = "test-jobgraphs/";
	private ExecutorService executor;


	@Before
    public void setup() throws IOException {
		client = new ConsulClient("localhost", consul.getHttpPort());

        // Provide a HA_STORAGE_PATH
        configuration = new Configuration();
        configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpFolder.newFolder().getAbsolutePath());
        executor = Executors.newSingleThreadExecutor();
	}

	@Test
	public void testPutAndRecoverJobGraph() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);
        ConsulSubmittedJobGraphStore graphStore2 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		graphStore2.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		verify(listener).onAddedJobGraph(jobID);
		graphStore1.stop();

        JobGraph recoverJobGraph = graphStore2.recoverJobGraph(jobID);
        assertEquals(jobGraph.getJobID(), recoverJobGraph.getJobID());
		assertNotSame(jobGraph, recoverJobGraph);
	}

	@Test(expected = FlinkException.class)
	public void testPutAndRemoveJobGraph() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		graphStore1.removeJobGraph(jobID);
		verify(listener).onRemovedJobGraph(jobID);

		graphStore1.recoverJobGraph(jobID);
	}

	@Test(expected = FlinkException.class)
	public void testLocalCleanupAsync() throws Exception {
		ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);

		JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

		JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		graphStore1.localCleanupAsync(jobID, executor).join();

		verify(listener).onRemovedJobGraph(jobID);

		graphStore1.recoverJobGraph(jobID);
	}

	@Test(expected = FlinkException.class)
	public void testGlobalCleanupAsync() throws Exception {
		ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);

		JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

		JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		graphStore1.globalCleanupAsync(jobID, executor).join();

		verify(listener).onRemovedJobGraph(jobID);

		graphStore1.recoverJobGraph(jobID);
	}

	@Test
	public void testGetJobIds() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);

		Collection<JobID> jobIds = graphStore1.getJobIds();
		assertEquals(1, jobIds.size());
		assertEquals(jobID, jobIds.iterator().next());
	}

	@After
	public void destroy(){
		executor.shutdownNow();
	}
    private JobGraph createJobGraph(JobID jobID) {
        return new JobGraph(jobID, "test-job");
	}
}
