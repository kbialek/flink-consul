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

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;

public final class ConsulLeaderRetrievalService implements LeaderRetrievalService {

	private final Object lock = new Object();

    private final Supplier<ConsulClient> clientProvider;
	private final Executor executor;
	private final String leaderKey;
	private final ConsulMetricService consulMetricService;

	private ConsulLeaderRetriever leaderRetriever;

    public ConsulLeaderRetrievalService(Supplier<ConsulClient> clientProvider,
										Executor executor,
										String leaderKey,
										ConsulMetricService consulMetricService) {
		this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.consulMetricService = consulMetricService;
	}

	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		Preconditions.checkState(leaderRetriever == null, "ConsulLeaderRetrievalService is already started");
		synchronized (lock) {
			this.leaderRetriever = new ConsulLeaderRetriever(clientProvider, executor, leaderKey, listener, 10, consulMetricService);
			this.leaderRetriever.start();
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (this.leaderRetriever != null) {
				this.leaderRetriever.stop();
			}
		}
	}
}
