package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Keeps Consul session active.
 */
public final class ConsulSessionActivator {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulSessionActivator.class);

	private final ConsulClient client;
	private final Executor executor;
	private final int sessionTtl;
	private volatile boolean running;
	private final ConsulSessionHolder holder = new ConsulSessionHolder();

	/**
	 * @param client     Consul client
	 * @param executor   runs session keep-alive background task
	 * @param sessionTtl session ttl in seconds
	 */
	public ConsulSessionActivator(ConsulClient client, Executor executor, int sessionTtl) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.sessionTtl = sessionTtl;
	}

	public ConsulSessionHolder start() {
		executor.execute(this::doRun);
		return holder;
	}

	public void stop() {
		running = false;
	}

	private void doRun() {
		LOG.info("Starting ConsulSessionActivator");
		running = true;
		while (running) {
			createOrRenewConsulSession();
			try {
				Thread.sleep(1000 * (sessionTtl - 1));
			} catch (InterruptedException ignored) {

			}
		}
		destroyConsulSession();
		LOG.info("Stopped ConsulSessionActivator");
	}

	private void createOrRenewConsulSession() {
		if (holder.getSessionId() == null) {
			createConsulSession();
		} else {
			renewConsulSession();
		}
	}

	private void createConsulSession() {
		NewSession newSession = new NewSession();
		newSession.setName("flink");
		newSession.setTtl(String.format("%ds", Math.max(10, sessionTtl)));
		holder.setSessionId(client.sessionCreate(newSession, QueryParams.DEFAULT).getValue());
	}

	private void renewConsulSession() {
		try {
			client.renewSession(holder.getSessionId(), QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session renew failed", e);
		}
	}

	private void destroyConsulSession() {
		try {
			client.sessionDestroy(holder.getSessionId(), QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session destroy failed", e);
		}
	}

	public ConsulSessionHolder getHolder() {
		return holder;
	}
}
