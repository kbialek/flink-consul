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
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.LogLevel;
import org.apache.flink.runtime.consul.ConsulSessionActivator;
import org.apache.flink.runtime.consul.ConsulSessionHolder;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ConsulLeaderLatchTest {

	private ConsulProcess consul;
	private ConsulClient client;
	private Executor executor = Executors.newFixedThreadPool(8);
	private int waitTime = 1;
	private ConsulSessionActivator sessionActivator1;
	private ConsulSessionHolder sessionHolder1;
	private ConsulSessionActivator sessionActivator2;
	private ConsulSessionHolder sessionHolder2;

	@Before
	public void setup() {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.withLogLevel(LogLevel.DEBUG)
			.build()
			.start();
		client = new ConsulClient(String.format("localhost:%d", consul.getHttpPort()));
		sessionActivator1 = new ConsulSessionActivator(client, executor, 10);
		sessionHolder1 = sessionActivator1.start();
		sessionActivator2 = new ConsulSessionActivator(client, executor, 10);
		sessionHolder2 = sessionActivator2.start();
	}

	@After
	public void cleanup() {
		sessionActivator1.stop();
		sessionActivator2.stop();
		consul.close();
	}

	@Test
	public void testLeaderElection() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

		ConsulLeaderLatch latch = new ConsulLeaderLatch(client, executor, sessionHolder1, leaderKey, "leader-address", listener, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipAcquired(eq("leader-address"), any(UUID.class));

		assertTrue(latch.hasLeadership());

		latch.stop();
	}

	@Test
	public void testLeaderElectionTwoNodes() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener1 = mock(ConsulLeaderLatchListener.class);
		ConsulLeaderLatchListener listener2 = mock(ConsulLeaderLatchListener.class);

		LeaderRetrievalListener retrievalListener = mock(LeaderRetrievalListener.class);

		ConsulLeaderLatch latch1 = new ConsulLeaderLatch(client, executor, sessionHolder1, leaderKey, "leader-address1", listener1, waitTime);
		ConsulLeaderLatch latch2 = new ConsulLeaderLatch(client, executor, sessionHolder2, leaderKey, "leader-address2", listener2, waitTime);
		ConsulLeaderRetriever leaderResolver = new ConsulLeaderRetriever(client, executor, leaderKey, retrievalListener, 1);

		leaderResolver.start();
		latch1.start();
		Thread.sleep(100);
		latch2.start();

		Thread.sleep(2000 * waitTime);
		verify(listener1).onLeadershipAcquired(eq("leader-address1"), any(UUID.class));
		verify(retrievalListener).notifyLeaderAddress(eq("leader-address1"), any(UUID.class));
		assertTrue(latch1.hasLeadership());
		assertFalse(latch2.hasLeadership());

		latch1.stop();
		Thread.sleep(2000 * waitTime);
		verify(listener2).onLeadershipAcquired(eq("leader-address2"), any(UUID.class));
		assertFalse(latch1.hasLeadership());
		assertTrue(latch2.hasLeadership());

		latch2.stop();
		assertFalse(latch1.hasLeadership());
		assertFalse(latch2.hasLeadership());

	}

	@Test
	public void testConsulReset() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

		ConsulLeaderLatch latch = new ConsulLeaderLatch(client, executor, sessionHolder1, leaderKey, "leader-address", listener, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipAcquired(eq("leader-address"), any(UUID.class));
		assertTrue(latch.hasLeadership());

		consul.reset();
		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipRevoked();
		assertFalse(latch.hasLeadership());

		latch.stop();
	}


	@Test
	public void testWithConsulNotReachable() throws InterruptedException {
		consul.close();

		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);
		LeaderRetrievalListener retrievalListener = mock(LeaderRetrievalListener.class);

		ConsulLeaderLatch latch = new ConsulLeaderLatch(client, executor, sessionHolder1, leaderKey, "leader-address", listener, waitTime);
		ConsulLeaderRetriever leaderResolver = new ConsulLeaderRetriever(client, executor, leaderKey, retrievalListener, 1);

		leaderResolver.start();
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(retrievalListener, atLeastOnce()).handleError(any(Exception.class));

		latch.stop();
	}
}
