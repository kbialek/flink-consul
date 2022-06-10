package com.espro.flink.consul.jobregistry;

/**
* Hold running job status, JobResultStore introduces two status clean and dirty.
*
* @see org.apache.flink.runtime.highavailability.JobResultStore
*/
public enum JobStatus {
    CLEAN("clean"),
    DIRTY("dirty");

    JobStatus(String value) {
        this.value = value;
    }

    String value;

    public String getValue() {
        return this.value;
    }
}
