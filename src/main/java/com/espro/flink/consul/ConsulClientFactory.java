package com.espro.flink.consul;

import static org.apache.flink.util.Preconditions.checkNotNull;

import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_HOST;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_PORT;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_ENABLED;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_PASSWORD;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_PATH;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_TYPE;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_PASSWORD;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_PATH;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_TYPE;

import java.io.IOException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.flink.configuration.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

import com.ecwid.consul.transport.TransportException;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

/**
 * Factory class for creating a {@link ConsulClient} using the Flink {@link Configuration}.
 *
 * @see ConsulHighAvailabilityOptions
 */
final class ConsulClientFactory {

    private ConsulClientFactory() {
        // utility class for creating consul client
    }

    /**
     * Creates a {@link ConsulClient} using the configured consul properties out of the Flink {@link Configuration}.
     *
     * @param configuration flink's configuration
     * @return {@link ConsulClient}
     */
    public static ConsulClient createConsulClient(Configuration configuration) {

        String consulHost = configuration.getString(HA_CONSUL_HOST);
        int consulPort = configuration.getInteger(HA_CONSUL_PORT);

        if (configuration.getBoolean(HA_CONSUL_TLS_ENABLED)) {
            return new ConsulClient(new ConsulRawClient(consulHost, consulPort, createSecuredHttpClient(configuration)));
        }

        return new ConsulClient(consulHost, consulPort);
    }

    private static HttpClient createSecuredHttpClient(Configuration configuration) {
        String keyStorePath = checkNotNull(configuration.getString(HA_CONSUL_TLS_KEYSTORE_PATH), "No keystore path given!");
        String keyStorePassword = configuration.getString(HA_CONSUL_TLS_KEYSTORE_PASSWORD);
        String keyStoreType = checkNotNull(configuration.getString(HA_CONSUL_TLS_KEYSTORE_TYPE), "No keystore type given!");

        String trustStorePath = checkNotNull(configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_PATH), "No truststore path given!");
        String trustStorePassword = configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_PASSWORD);
        String trustStoreType = checkNotNull(configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_TYPE), "No truststore type given!");

        try {
            char[] keyStorePasswordCharArray = keyStorePassword != null ? keyStorePassword.toCharArray() : null;
            char[] trustStorePasswordCharArray = trustStorePassword != null ? trustStorePassword.toCharArray() : null;

            // Build ssl context using configured keystore and truststore
            SSLContext sslContext = SSLContexts.custom()
                    .setSecureRandom(new SecureRandom())
                    .setKeyStoreType(keyStoreType)
                    .loadKeyMaterial(new URL(keyStorePath), keyStorePasswordCharArray, keyStorePasswordCharArray)
                    .setKeyStoreType(trustStoreType)
                    .loadTrustMaterial(new URL(trustStorePath), trustStorePasswordCharArray, new TrustSelfSignedStrategy())
                    .build();
            SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslContext);

            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", factory)
                    .build();

            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
            connectionManager.setMaxTotal(20);
            connectionManager.setDefaultMaxPerRoute(10);

            // Took timeout settings from AbstractHttpTransport
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout((int) TimeUnit.SECONDS.toMillis(10))
                    .setConnectionRequestTimeout((int) TimeUnit.SECONDS.toMillis(10))
                    .setSocketTimeout((int) TimeUnit.MINUTES.toMillis(10))
                    .build();

            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig);

            return httpClientBuilder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new TransportException(e);
        }
    }
}
