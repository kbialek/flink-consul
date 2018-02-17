package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class ConsulSessionActivatorTest {

	private ConsulProcess consul;
	private ConsulClient client;

	@Before
	public void setup() {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.build()
			.start();
		client = new ConsulClient("localhost", consul.getHttpPort());
	}

	@After
	public void cleanup() {
		consul.close();
	}

	@Test
	public void testSessionLifecycle() throws Exception {
		Executor executor = Executors.newFixedThreadPool(2);
		ConsulClient spiedClient = spy(client);
		ConsulSessionActivator cse = new ConsulSessionActivator(spiedClient, executor, 10);
		ConsulSessionHolder holder = cse.start();
		Thread.sleep(1000);

		verify(spiedClient).sessionCreate(any(NewSession.class), any(QueryParams.class));

		assertNotNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());
		Thread.sleep(11000);

		verify(spiedClient).renewSession(anyString(), any(QueryParams.class));
		assertNotNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());

		cse.stop();

		Thread.sleep(11000);
		verify(spiedClient).sessionDestroy(anyString(), any(QueryParams.class));
		assertNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());
	}

}
