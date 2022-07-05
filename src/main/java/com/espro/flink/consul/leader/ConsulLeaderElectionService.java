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

package com.espro.flink.consul.leader;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.ConsulSessionHolder;

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
    private final Supplier<ConsulClient> clientProvider;

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

	private final ConsulMetricService consulMetricService;

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
     *
     * @param clientProvider provides a Client which is connected to the Consul quorum
     * @param leaderPath ZooKeeper node path for the node which stores the current leader information
     * @param sessionHolder
     */
    public ConsulLeaderElectionService(Supplier<ConsulClient> clientProvider,
									   Executor executor,
									   ConsulSessionHolder sessionHolder,
									   String leaderPath,
									   ConsulMetricService consulMetricService) {
		this.clientProvider = Preconditions.checkNotNull(clientProvider, "Consul client");
		this.leaderPath = Preconditions.checkNotNull(leaderPath, "leaderPath");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");
		this.consulMetricService = consulMetricService;

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

            leaderLatch = new ConsulLeaderLatch(clientProvider, executor, sessionHolder, leaderPath, listener, 10, consulMetricService);
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

        LOG.info("Stopping ConsulLeaderElectionService {}.", this);

	}

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (hasLeadership(leaderSessionID)) {
            LOG.info("Leadership confirmed for {} and session id {}", leaderAddress, leaderSessionID);
            leaderLatch.confirmLeadership(leaderSessionID, leaderAddress);
            this.confirmedLeaderSessionID = leaderSessionID;
        }
    }

    @Override
    public boolean hasLeadership(UUID leaderSessionId) {
        return leaderLatch.hasLeadership(leaderSessionId);
    }

}
