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
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

final class ConsulLeaderRetriever {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderRetriever.class);

	private final ConsulClient client;

	private final Executor executor;

	private final String leaderKey;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private ConsulLeaderData leaderData;

	private final LeaderRetrievalListener listener;

	private final int waitTime;

	/**
	 * @param client    Consul client
	 * @param executor  Executor to run background tasks
	 * @param leaderKey key in Consul KV store
	 * @param waitTime  Consul blocking read timeout (in seconds)
	 */
	public ConsulLeaderRetriever(ConsulClient client,
								 Executor executor,
								 String leaderKey,
								 LeaderRetrievalListener listener,
								 int waitTime) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.listener = Preconditions.checkNotNull(listener, "listener");
		this.waitTime = waitTime;
	}

	public void start() {
		LOG.info("Starting Consul Leader Retriever");
		runnable = true;
		executor.execute(this::watch);
	}

	public void stop() {
		LOG.info("Stopping Consul Leader Retriever");
		runnable = false;
	}

	private void watch() {
		while (runnable) {
			try {
				GetBinaryValue value = readLeaderKey();
				String leaderSessionId = null;
				if (value != null) {
					leaderKeyIndex = value.getModifyIndex();
					leaderSessionId = value.getSession();
				}

				if (runnable && leaderSessionId != null) {
					leaderRetrieved(ConsulLeaderData.from(value.getValue()));
				}
			} catch (Exception exception) {
				listener.handleError(exception);
				// backoff
				try {
					Thread.sleep(waitTime * 1000);
				} catch (InterruptedException ignored) {

				}
			}
		}
	}

	private GetBinaryValue readLeaderKey() {
		QueryParams queryParams = QueryParams.Builder.builder()
			.setIndex(leaderKeyIndex)
			.setWaitTime(waitTime)
			.build();
		Response<GetBinaryValue> leaderKeyValue = client.getKVBinaryValue(leaderKey, queryParams);
		return leaderKeyValue.getValue();
	}

	private void leaderRetrieved(ConsulLeaderData data) {
		if (!data.equals(leaderData)) {
			leaderData = data;
			notifyOnLeaderRetrieved(data);
			LOG.info("Cluster leader retrieved {}", data);
		}
	}

	private void notifyOnLeaderRetrieved(ConsulLeaderData data) {
		try {
			listener.notifyLeaderAddress(data.getAddress(), data.getSessionId());
		} catch (Exception e) {
			LOG.error("Listener failed on leader retrieved notification", e);
		}
	}

}
