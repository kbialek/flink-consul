package com.espro.flink.consul.checkpoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.espro.flink.consul.AbstractConsulTest;

public class ConsulCompletedCheckpointStoreTest extends AbstractConsulTest {

    private ConsulClient client;
    private File tempDir;
    private RetrievableStateStorageHelper<CompletedCheckpoint> storage;
    private String checkpointsPath = "test-checkpoints/";

    @Before
    public void setup() throws IOException {
        client = new ConsulClient(String.format("localhost:%d", consul.getHttpPort()));
        tempDir = Files.createTempDir();
        storage = new FileSystemStateStorageHelper<>(new Path(tempDir.getPath()), "cp");
    }

    @Test
    public void testConfiguration() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 10, storage);
        assertEquals(10, store.getMaxNumberOfRetainedCheckpoints());
        assertTrue(store.requiresExternalizedCheckpoints());
    }

    @Test
    public void testAddCheckpoint() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);

        CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);

        assertEquals(0, store.getNumberOfRetainedCheckpoints());

        store.addCheckpoint(checkpoint);

        assertEquals(1, store.getNumberOfRetainedCheckpoints());

        CompletedCheckpoint latestCheckpoint = store.getLatestCheckpoint(false);
        assertNotNull(latestCheckpoint);
        assertEquals(checkpoint, latestCheckpoint);
        assertSame(checkpoint, latestCheckpoint);
    }

    @Test
    public void testAllCheckpoints() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 3, storage);

        CompletedCheckpoint checkpoint1 = createCheckpoint(jobID, 1l);
        CompletedCheckpoint checkpoint2 = createCheckpoint(jobID, 2l);

        store.addCheckpoint(checkpoint1);
        store.addCheckpoint(checkpoint2);

        List<CompletedCheckpoint> allCheckpoints = store.getAllCheckpoints();
        assertNotNull(allCheckpoints);
        assertEquals(2, allCheckpoints.size());
        assertEquals(checkpoint1, allCheckpoints.get(0));
        assertSame(checkpoint1, allCheckpoints.get(0));
        assertEquals(checkpoint2, allCheckpoints.get(1));
        assertSame(checkpoint2, allCheckpoints.get(1));
    }

    @Test
    public void testRecovery() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);

        CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);
        store.addCheckpoint(checkpoint);

        ConsulCompletedCheckpointStore newStore = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);
        newStore.recover();

        CompletedCheckpoint latestCheckpoint = newStore.getLatestCheckpoint(false);
        assertNotNull(latestCheckpoint);
        assertEquals(checkpoint.getExternalPointer(), latestCheckpoint.getExternalPointer());
        assertNotSame(checkpoint, latestCheckpoint);
    }

    @Test
    public void testRecovery_retryAfterException() throws Exception {
        JobID jobID = JobID.generate();

        CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);

        ConsulClient consulClient = mock(ConsulClient.class);
        when(consulClient.getKVKeysOnly(checkpointsPath + jobID.toString())).thenReturn(
                new Response<>(Lists.newArrayList("cp1"), 0L, false, 0L)
        );
        when(consulClient.getKVBinaryValue("cp1")).thenAnswer(i -> {
            GetBinaryValue binaryValue = new GetBinaryValue();
            binaryValue.setValue(InstantiationUtil.serializeObject(new TestStateHandle(
                    Lists.newArrayList(
                            new IOException(),
                            checkpoint,
                            checkpoint
                    )
            )));
            return new Response<>(binaryValue, 0L, false, 0L);
        });
        ConsulCompletedCheckpointStore newStore = new ConsulCompletedCheckpointStore(consulClient, checkpointsPath, jobID, 1, storage);

        newStore.recover();

        CompletedCheckpoint latestCheckpoint = newStore.getLatestCheckpoint(false);
        assertNotNull(latestCheckpoint);
        assertEquals(checkpoint.getExternalPointer(), latestCheckpoint.getExternalPointer());
        assertNotSame(checkpoint, latestCheckpoint);
    }

    @Test
    public void testShutdown() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);

        CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);
        store.addCheckpoint(checkpoint);
        store.shutdown(JobStatus.FINISHED);

        ConsulCompletedCheckpointStore newStore = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);
        newStore.recover();

        assertEquals(0, newStore.getAllCheckpoints().size());
    }

    @Test
    public void testRemoveSubsumedCheckpoint() throws Exception {
        JobID jobID = JobID.generate();
        ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, checkpointsPath, jobID, 1, storage);

        CompletedCheckpoint checkpoint1 = createCheckpoint(jobID, 1l);
        CompletedCheckpoint checkpoint2 = createCheckpoint(jobID, 2l);

        store.addCheckpoint(checkpoint1);
        store.addCheckpoint(checkpoint2);

        CompletedCheckpoint latestCheckpoint = store.getLatestCheckpoint(false);
        assertNotNull(latestCheckpoint);
        assertEquals(checkpoint2, latestCheckpoint);
        assertSame(checkpoint2, latestCheckpoint);

        assertEquals(1, store.getAllCheckpoints().size());
    }

    private CompletedCheckpoint createCheckpoint(JobID jobID, long checkpointId) {
        long timestamp = checkpointId;
        long completionTimestamp = checkpointId;
        CheckpointProperties chkProps = CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION);
        CompletedCheckpointStorageLocation chkLocation = new TestCompletedCheckpointStorageLocation();
        return new CompletedCheckpoint(jobID, checkpointId, timestamp, completionTimestamp, Maps.newHashMap(),
                Lists.newArrayList(), chkProps, chkLocation);
    }

    static class TestStateHandle implements RetrievableStateHandle<CompletedCheckpoint>, Serializable {

        private List<Object> behavior;
        private transient Iterator<Object> iter;

        public TestStateHandle(List<Object> behavior) {
            this.behavior = behavior;
            this.iter = behavior.iterator();
        }

        @Override
        public CompletedCheckpoint retrieveState() throws IOException, ClassNotFoundException {
            if (iter.hasNext()) {
                Object next = iter.next();
                if (next instanceof IOException) {
                    throw (IOException) next;
                } else {
                    return (CompletedCheckpoint) next;
                }
            } else {
                fail("Missing behavior");
                return null;
            }
        }

        @Override
        public void discardState() throws Exception {

        }

        @Override
        public long getStateSize() {
            return 0;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeObject(behavior);
        }

        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            behavior = (List<Object>) ois.readObject();
            iter = behavior.iterator();
        }
    }

    public static class TestCompletedCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

        private String externalPointer = UUID.randomUUID().toString();

        @Override
        public String getExternalPointer() {
            return externalPointer;
        }

        @Override
        public StreamStateHandle getMetadataHandle() {
            return new ByteStreamStateHandle("handle", new byte[]{});
        }

        @Override
        public void disposeStorageLocation() throws IOException {

        }

        @Override
        public int hashCode() {
            return externalPointer.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestCompletedCheckpointStorageLocation) {
                TestCompletedCheckpointStorageLocation other = (TestCompletedCheckpointStorageLocation) obj;
                return this.externalPointer.equals(other.externalPointer);
            }
            return false;
        }
    }
}
