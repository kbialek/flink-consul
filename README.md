[![Build Status](https://travis-ci.org/kbialek/flink-consul.svg?branch=master)](https://travis-ci.org/kbialek/flink-consul)

# flink-consul

Consul HA backend for Apache Flink

## Configuration

In order to start an HA-cluster with flink-consul add the following configuration keys to conf/flink-conf.yaml:

    high-availability: com.espro.flink.consul.ConsulHaServicesFactory
    high-availability.consul.host: https://my-consul-server
    high-availability.consul.port: 8550
    high-availability.storageDir: hdfs:///flink/recovery

### Mandatory Properties

| Property                      | Default          | Description                                  |
| ----------------------------- | ---------------- | -------------------------------------------- |
| high-availability.consul.host | localhost        | Address of the Consul server/agent           |
| high-availability.consul.port | 8550             | Port to use to reach the Consul server/agent |

### Optional Properties

#### Manage storage paths in Consul

| Property                                         | Default             | Description                                           |
| ------------------------------------------------ | ------------------- | ----------------------------------------------------- |
| high-availability.consul.path.root               | flink/              | Used to define the root/base path in Consul KV store. |
| high-availability.consul.path.jobstatus          | jobstatus/          | Consul path relative to the root path for storing job states |
| high-availability.consul.path.jobgraphs          | jobgraphs/          | Consul path relative to the root path for storing job graph |
| high-availability.consul.path.checkpoint-counter | checkpoint-counter/ | Consul path relative to the root path for storing checkpoint counter |
| high-availability.consul.path.checkpoints        | checkpoints/        | Consul path relative to the root path for storing information for completed checkpoints |
| high-availability.consul.path.leader             | leader/             | Consul path relative to the root path for storing leader information |

#### Secured connections to Consul

| Property                                         | Default          | Description     |
| ------------------------------------------------ | ---------------- | ----------------|
| high-availability.consul.tls.enabled             | false            | To enabled tls secured http communication |
| high-availability.consul.tls.keystore.path       | none             | Path to the keystore file, e.g. file:/path/to/keystore/consul.p12
| high-availability.consul.tls.keystore.password   | none             | The password to use to read the keystore
| high-availability.consul.tls.keystore.type       | PKCS12           | Type of the keystore
| high-availability.consul.tls.truststore.path     | none             | Path to the truststore file, e.g. file:/path/to/truststore/consul-ca.p12
| high-availability.consul.tls.truststore.password | none             | The password to use to read the truststore
| high-availability.consul.tls.truststore.type     | PKCS12           | Type of the truststore