/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.consul.checkpoint.ConsulCheckpointRecoveryFactory;
import org.apache.flink.runtime.consul.configuration.ConsulHighAvailabilityOptions;
import org.apache.flink.runtime.consul.jobgraph.ConsulSubmittedJobGraphStore;
import org.apache.flink.runtime.consul.jobregistry.ConsulRunningJobsRegistry;
import org.apache.flink.runtime.consul.leader.ConsulLeaderElectionService;
import org.apache.flink.runtime.consul.leader.ConsulLeaderRetrievalService;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link HighAvailabilityServices} using Hashicorp Consul.
 */
public class ConsulHaServices implements HighAvailabilityServices {

	private static final String RESOURCE_MANAGER_LEADER_PATH = "/resource_manager_lock";

	private static final String DISPATCHER_LEADER_PATH = "/dispatcher_lock";

	private static final String JOB_MANAGER_LEADER_PATH = "/job_manager_lock";

	/**
	 * Consul client to use
	 */
	private final ConsulClient client;

	/**
	 * The executor to run Consul callbacks on
	 */
	private final Executor executor;

	/**
	 * The runtime configuration
	 */
	private final Configuration configuration;

	/**
	 * The Consul based running jobs registry
	 */
	private final RunningJobsRegistry runningJobsRegistry;

	/**
	 * Store for arbitrary blobs
	 */
//	private final BlobStoreService blobStoreService;
	private final BlobStoreService blobStore;

	private final ConsulSessionActivator consulSessionActivator;

	public ConsulHaServices(ConsulClient client,
							Executor executor,
							Configuration configuration,
							BlobStoreService blobStoreService) {
		this.client = checkNotNull(client);
		this.executor = Executors.newCachedThreadPool();
		this.configuration = checkNotNull(configuration);

		this.blobStore = checkNotNull(blobStoreService);

		this.consulSessionActivator = new ConsulSessionActivator(client, this.executor, 10);
		this.consulSessionActivator.start();

		this.runningJobsRegistry = new ConsulRunningJobsRegistry(client, consulSessionActivator.getHolder(), jobStatusPath());
	}


	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		String leaderPath = getJobManagerLeaderPath(jobID);
		return new ConsulLeaderRetrievalService(client, executor, leaderPath);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		String leaderPath = getJobManagerLeaderPath(jobID);
		return new ConsulLeaderElectionService(client, executor, consulSessionActivator.getHolder(), leaderPath);
	}


	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return new ConsulLeaderRetrievalService(client, executor, getLeaderPath() + RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return new ConsulLeaderElectionService(client, executor, consulSessionActivator.getHolder(),
			getLeaderPath() + RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return new ConsulLeaderRetrievalService(client, executor, getLeaderPath() + DISPATCHER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return new ConsulLeaderElectionService(client, executor, consulSessionActivator.getHolder(),
			getLeaderPath() + DISPATCHER_LEADER_PATH);
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new ConsulCheckpointRecoveryFactory(client, configuration);
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		return new ConsulSubmittedJobGraphStore(client, jobGraphsPath());
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return blobStore;
	}

	@Override
	public void close() throws Exception {
		consulSessionActivator.stop();
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		close();
	}

	private String getLeaderPath() {
		return configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT)
			+ configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_LEADER_PATH);
	}

	private String getJobManagerLeaderPath(final JobID jobID) {
		return getLeaderPath() + jobID + JOB_MANAGER_LEADER_PATH;
	}

	private String jobStatusPath() {
		return configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT)
			+ configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_JOBSTATUS_PATH);
	}

	private String jobGraphsPath() {
		return configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT)
			+ configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_JOBGRAPHS_PATH);
	}
}
