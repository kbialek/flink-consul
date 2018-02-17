package org.apache.flink.runtime.consul;

public final class ConsulSessionHolder {

	private volatile String sessionId;

	public String getSessionId() {
		return sessionId;
	}

	void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public boolean compareSessionId(String sessionId) {
		return (this.sessionId == null && sessionId == null) || (this.sessionId.equals(sessionId));
	}
}
