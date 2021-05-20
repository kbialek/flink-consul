package com.espro.flink.consul.checkpoint;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.JobID;
import org.junit.Test;

public class ConsulCheckpointStoreUtilTest {

    private static final String CHECKPOINT_PATH = "flink/checkpoints/";

    @Test
    public void testCheckpointIDToName() {
        JobID jobID = JobID.generate();

        ConsulCheckpointStoreUtil storeUtil = new ConsulCheckpointStoreUtil(CHECKPOINT_PATH, jobID);

        long checkpointId = 5634L;
        assertEquals(CHECKPOINT_PATH + jobID.toString() + checkpointId, storeUtil.checkpointIDToName(checkpointId));
    }

    @Test
    public void testNameToCheckpointID() {
        // GIVEN job id and store util
        JobID jobID = JobID.generate();
        ConsulCheckpointStoreUtil storeUtil = new ConsulCheckpointStoreUtil(CHECKPOINT_PATH, jobID);

        // GIVEN expected checkpoint id and checkpoint name
        long expectedCheckpointId = 10L;
        String checkpointName = CHECKPOINT_PATH + jobID.toString() + expectedCheckpointId;

        // WHEN name is converted to checkpoint id, the resulting checkpoint id is equal to expected one
        assertEquals(expectedCheckpointId, storeUtil.nameToCheckpointID(checkpointName));
    }
}
