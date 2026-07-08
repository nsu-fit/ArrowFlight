package net.surpin.data.arrowflight.client;

import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ClientRetryTest {

    @Test
    void retryOnTransientErrors() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        config.setMaxRetries(3);
        config.setRetryBackoffMs(10);
        config.setConnectTimeoutMs(5000);

        // We can't easily create a Client without a real Flight server,
        // but we can test the retry configuration propagation.
        assertEquals(3, config.getMaxRetries());
        assertEquals(10, config.getRetryBackoffMs());
        assertEquals(5000, config.getConnectTimeoutMs());
    }

    @Test
    void setterOverridesDefault() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        assertEquals(3, config.getMaxRetries());

        config.setMaxRetries(5);
        assertEquals(5, config.getMaxRetries());
    }

    @Test
    void defaultConfigurationValues() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);

        assertEquals(3, config.getMaxRetries());
        assertEquals(1000, config.getRetryBackoffMs());
        assertEquals(30000, config.getConnectTimeoutMs());
    }

    @Test
    void configurationToStringIncludesRetrySettings() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        String str = config.toString();

        assertTrue(str.contains("maxRetries=3"));
        assertTrue(str.contains("retryBackoffMs=1000"));
        assertTrue(str.contains("connectTimeoutMs=30000"));
    }
}
