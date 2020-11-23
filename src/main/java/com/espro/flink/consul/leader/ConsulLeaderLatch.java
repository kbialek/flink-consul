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
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.espro.flink.consul.ConsulSessionHolder;

final class ConsulLeaderLatch {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderLatch.class);

	private final ConsulClient client;

	private final Executor executor;

	private final ConsulSessionHolder sessionHolder;

	private final String leaderKey;

	/**
	 * SessionID
	 */
	private UUID flinkSessionId;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private boolean hasLeadership;

	private final ConsulLeaderLatchListener listener;

    private final int waitTimeInSeconds;

	/**
	 * @param client      Consul client
	 * @param executor    Executor to run background tasks
	 * @param leaderKey   key in Consul KV store
	 * @param nodeAddress leadership changes are reported to this contender
	 * @param waitTime    Consul blocking read timeout (in seconds)
	 */
	public ConsulLeaderLatch(ConsulClient client,
							 Executor executor,
							 ConsulSessionHolder sessionHolder,
							 String leaderKey,
							 ConsulLeaderLatchListener listener,
							 int waitTime) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.listener = Preconditions.checkNotNull(listener, "listener");
        this.waitTimeInSeconds = waitTime;
	}

	public void start() {
		LOG.info("Starting Consul Leadership Latch");
		runnable = true;
		executor.execute(this::watch);
	}

	public void stop() {
		LOG.info("Stopping Consul Leadership Latch");
		runnable = false;
		hasLeadership = false;
	}

	private void watch() {
		flinkSessionId = UUID.randomUUID();
		while (runnable) {
			try {
				GetBinaryValue value = readLeaderKey();
				String leaderSessionId = null;
				if (value != null) {
					leaderKeyIndex = value.getModifyIndex();
					leaderSessionId = value.getSession();
				}

				if (runnable) {
					if (leaderSessionId == null) {
						LOG.info("No leader elected. Current node is trying to register");
                        Boolean success = writeLeaderKey(null);
						if (success) {
                            leadershipAcquired(ConsulLeaderData.from(null, flinkSessionId));
						} else {
							leadershipRevoked();
						}
					}
				}
			} catch (Exception e) {
				LOG.error("Exception during leadership election", e);
				// backoff
				try {
                    TimeUnit.SECONDS.sleep(waitTimeInSeconds);
				} catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
				}
			}
		}
		releaseLeaderKey();
	}

    UUID getFlinkSessionId() {
        return flinkSessionId;
    }

    public boolean hasLeadership(UUID leaderSessionId) {
        return hasLeadership && flinkSessionId.equals(leaderSessionId);
	}

	private GetBinaryValue readLeaderKey() {
		QueryParams queryParams = QueryParams.Builder.builder()
			.setIndex(leaderKeyIndex)
                .setWaitTime(waitTimeInSeconds)
			.build();
		Response<GetBinaryValue> leaderKeyValue = client.getKVBinaryValue(leaderKey, queryParams);
		return leaderKeyValue.getValue();
	}

    private boolean writeLeaderKey(String nodeAddress) {
		PutParams putParams = new PutParams();
		putParams.setAcquireSession(sessionHolder.getSessionId());
		try {
            ConsulLeaderData data = new ConsulLeaderData(nodeAddress, flinkSessionId);
            Boolean response = client.setKVBinaryValue(leaderKey, data.toBytes(), putParams).getValue();
            return response != null ? response : false;
		} catch (OperationException ex) {
            LOG.error("Error while writing leader key for {} with session id {} to Consul.", nodeAddress, flinkSessionId);
			return false;
		}
	}

	private Boolean releaseLeaderKey() {
		PutParams putParams = new PutParams();
		putParams.setReleaseSession(sessionHolder.getSessionId());
		try {
			return client.setKVBinaryValue(leaderKey, new byte[0], putParams).getValue();
		} catch (OperationException ex) {
            LOG.error("Error while releasing leader key for session {}.", sessionHolder.getSessionId());
			return false;
		}
	}

	private void leadershipAcquired(ConsulLeaderData data) {
		if (!hasLeadership) {
			hasLeadership = true;
			notifyOnLeadershipAcquired(data);
            LOG.info("Cluster leadership has been acquired by current node {}", data.getAddress());
		}
	}

	private void leadershipRevoked() {
		if (hasLeadership) {
			hasLeadership = false;
			notifyOnLeadershipRevoked();
			LOG.info("Cluster leadership has been revoked from current node");
		}
	}

	private void notifyOnLeadershipAcquired(ConsulLeaderData data) {
		try {
            listener.onLeadershipAcquired(data.getAddress(), data.getSessionId());
		} catch (Exception e) {
			LOG.error("Listener failed on leadership acquired notification", e);
		}
	}

	private void notifyOnLeadershipRevoked() {
		try {
			listener.onLeadershipRevoked();
		} catch (Exception e) {
			LOG.error("Listener failed on leadership revoked notification", e);
		}
	}

    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (hasLeadership(leaderSessionID)) {
            writeLeaderKey(leaderAddress);
        }
    }

}
