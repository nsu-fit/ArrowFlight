package net.surpin.data.arrowflight.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationTest {

    @Test
    void basicConstructorGetters() {
        Configuration c = new Configuration("myhost", 9999, "myuser", "mypass", null);

        assertEquals("myhost", c.getFlightHost());
        assertEquals(9999, c.getFlightPort());
        assertFalse(c.getTlsEnabled());
        assertFalse(c.verifyServer());
        assertEquals("myuser", c.getUser());
        assertEquals("mypass", c.getPassword());
        assertNull(c.getBearerToken());
        assertNull(c.getTruststoreJks());
        assertNull(c.getTruststorePass());
    }

    @Test
    void bearerTokenConstructor() {
        Configuration c = new Configuration("host", 32010, "user", null, "abc-token");

        assertNull(c.getPassword());
        assertEquals("abc-token", c.getBearerToken());
    }

    @Test
    void tlsConstructor() {
        Configuration c = new Configuration("host", 32010, true, false, "u", "p", null);

        assertTrue(c.getTlsEnabled());
        assertFalse(c.verifyServer());
    }

    @Test
    void workloadManagementDefaultsEmpty() {
        Configuration c = new Configuration("h", 1, "u", "p", null);

        assertEquals("", c.getDefaultSchema());
        assertEquals("", c.getRoutingTag());
        assertEquals("", c.getRoutingQueue());
    }

    @Test
    void workloadManagementSetters() {
        Configuration c = new Configuration("h", 1, "u", "p", null);
        c.setDefaultSchema("my_schema");
        c.setRoutingTag("batch");
        c.setRoutingQueue("high");

        assertEquals("my_schema", c.getDefaultSchema());
        assertEquals("batch", c.getRoutingTag());
        assertEquals("high", c.getRoutingQueue());
    }

    @Test
    void allocationLimitDefaultsToZero() {
        Configuration c = new Configuration("h", 1, "u", "p", null);
        assertEquals(0, c.getAllocationLimit());
    }

    @Test
    void allocationLimitSetter() {
        Configuration c = new Configuration("h", 1, "u", "p", null);
        c.setAllocationLimit(1024 * 1024 * 1024L);
        assertEquals(1024 * 1024 * 1024L, c.getAllocationLimit());
    }

    @Test
    void nullPasswordAndToken() {
        Configuration c = new Configuration("h", 1, "user1", null, null);
        assertNull(c.getPassword());
        assertNull(c.getBearerToken());
    }
}
