package com.espro.flink.consul;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.esotericsoftware.minlog.Log;

/**
 * Keeps Consul session active.
 */
public final class ConsulSessionActivator {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulSessionActivator.class);

    private final Supplier<ConsulClient> clientProvider;
    private final ScheduledExecutorService executorService;
    private final Duration sessionTtl;
	private volatile boolean running;
	private final ConsulSessionHolder holder = new ConsulSessionHolder();

	/**
     * @param clientProvider provides Consul client
     * @param executor runs session keep-alive background task
     * @param sessionTtl session ttl in seconds
     */
    public ConsulSessionActivator(Supplier<ConsulClient> clientProvider, int sessionTtl) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.sessionTtl = Duration.ofSeconds(sessionTtl);
	}

	public ConsulSessionHolder start() {
        LOG.info("Starting ConsulSessionActivator");
        running = true;
        executorService.execute(this::doRun);
		return holder;
	}

	public void stop() {
		running = false;

        // There is no need to wait for execution of scheduled tasks
        LOG.info("Shuting down now executorService...");
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        destroyConsulSession();
        LOG.info("Stopped ConsulSessionActivator");
	}

	private void doRun() {
        if (running) {
            createOrRenewConsulSession();

            // Consul session is renewed after 90% of the ttl is passed
            executorService.schedule(this::doRun, (long) (sessionTtl.toMillis() * 0.8), TimeUnit.MILLISECONDS);
        }
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
        newSession.setTtl(String.format("%ds", Math.max(10, sessionTtl.toMillis() / 1000)));
        holder.setSessionId(clientProvider.get().sessionCreate(newSession, QueryParams.DEFAULT).getValue());
        Log.info("New consul session is created {}", holder.getSessionId());
	}

	private void renewConsulSession() {
		try {
            clientProvider.get().renewSession(holder.getSessionId(), QueryParams.DEFAULT);
        } catch (OperationException e) {
            LOG.warn("Consul session renew failed, a new session is created.", e);
            createConsulSession();
        } catch (Exception e) {
			LOG.error("Consul session renew failed", e);
		}
	}

	private void destroyConsulSession() {
		try {
            clientProvider.get().sessionDestroy(holder.getSessionId(), QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session destroy failed", e);
		}
	}

	public ConsulSessionHolder getHolder() {
		return holder;
	}
}
