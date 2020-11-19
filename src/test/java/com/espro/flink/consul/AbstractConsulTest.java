package com.espro.flink.consul;

import org.junit.After;
import org.junit.Before;

import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;

/**
 * Abstract test class to provide Consul Server to test against. This class is responsible for starting the consul process and shutting it
 * down afterwards.
 */
public abstract class AbstractConsulTest {

    /**
     * Consul version that is used to start the embedded consul process.
     */
    private static final String CONSUL_VERSION = "1.8.4";

    protected ConsulProcess consul;

    @Before
    public void startConsulProcess() {
        consul = ConsulStarterBuilder.consulStarter()
                .withConsulVersion(CONSUL_VERSION)
                .build()
                .start();
    }

    @After
    public void stopConsulProcess() {
        consul.close();
    }
}
