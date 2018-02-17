package org.apache.flink.runtime.consul.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public final class ConsulHighAvailabilityOptions {

	/**
	 * The root path under which Flink stores its entries in Consul
	 */
	public static final ConfigOption<String> HA_CONSUL_ROOT =
		key("high-availability.consul.path.root")
			.defaultValue("flink/");

	public static final ConfigOption<String> HA_CONSUL_LATCH_PATH =
		key("high-availability.consul.path.latch")
			.defaultValue("leaderlatch/");

	/**
	 * Consul root path for job graphs.
	 */
	public static final ConfigOption<String> HA_CONSUL_JOBGRAPHS_PATH =
		key("high-availability.consul.path.jobgraphs")
			.defaultValue("jobgraphs/");

	/**
	 * Consul root path for job status.
	 */
	public static final ConfigOption<String> HA_CONSUL_JOBSTATUS_PATH =
		key("high-availability.consul.path.jobstatus")
			.defaultValue("jobstatus/");

	public static final ConfigOption<String> HA_CONSUL_LEADER_PATH =
		key("high-availability.zookeeper.path.leader")
			.defaultValue("leader/");

	/**
	 * Consul root path for completed checkpoints.
	 */
	public static final ConfigOption<String> HA_CONSUL_CHECKPOINTS_PATH =
		key("high-availability.consul.path.checkpoints")
			.defaultValue("checkpoints/");

	/**
	 * Consul root path for checkpoint counters.
	 */
	public static final ConfigOption<String> HA_CONSUL_CHECKPOINT_COUNTER_PATH =
		key("high-availability.consul.path.checkpoint-counter")
			.defaultValue("checkpoint-counter/");
}
