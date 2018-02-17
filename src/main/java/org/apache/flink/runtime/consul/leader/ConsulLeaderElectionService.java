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

package org.apache.flink.runtime.consul.leader;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.runtime.consul.ConsulSessionHolder;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * Consul. The current leader's address as well as its leader session ID is published via
 * Consul as well.
 */
public class ConsulLeaderElectionService implements LeaderElectionService {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderElectionService.class);

	private final Object lock = new Object();

	/**
	 * Client to the Consul quorum
	 */
	private final ConsulClient client;

	/**
	 * Consul path of the node which stores the current leader information
	 */
	private final String leaderPath;

	private ConsulLeaderLatch leaderLatch;

	/**
	 * Executor to run Consul client background tasks
	 */
	private final Executor executor;

	private volatile UUID confirmedLeaderSessionID;

	private final ConsulSessionHolder sessionHolder;

	/**
	 * The leader contender which applies for leadership
	 */
	private volatile LeaderContender leaderContender;

	private volatile boolean running;

	private final ConsulLeaderLatchListener listener = new ConsulLeaderLatchListener() {
		@Override
		public void onLeadershipAcquired(String address, UUID sessionId) {
			leaderContender.grantLeadership(sessionId);
		}

		@Override
		public void onLeadershipRevoked() {
			leaderContender.revokeLeadership();
		}
	};

	/**
	 * Creates a {@link ConsulLeaderElectionService} object.
	 *  @param client     Client which is connected to the Consul quorum
	 * @param leaderPath ZooKeeper node path for the node which stores the current leader information
	 * @param sessionHolder
	 */
	public ConsulLeaderElectionService(ConsulClient client,
									   Executor executor,
									   ConsulSessionHolder sessionHolder,
									   String leaderPath) {
		this.client = Preconditions.checkNotNull(client, "Consul client");
		this.leaderPath = Preconditions.checkNotNull(leaderPath, "leaderPath");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");

		confirmedLeaderSessionID = null;
		leaderContender = null;

		running = false;
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	@Override
	public void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		synchronized (lock) {
			LOG.info("Starting ConsulLeaderElectionService {}.", this);

			leaderContender = contender;

			leaderLatch = new ConsulLeaderLatch(client, executor, sessionHolder, leaderPath,
				leaderContender.getAddress(), listener, 10);
			leaderLatch.start();

			running = true;
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (!running) {
				return;
			}

			leaderLatch.stop();

			running = false;
			confirmedLeaderSessionID = null;
		}

		LOG.info("Stopping ZooKeeperLeaderElectionService {}.", this);

	}

	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {

	}

	@Override
	public boolean hasLeadership() {
		return leaderLatch.hasLeadership();
	}

}
