package net.surpin.data.arrowflight.server;

import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class HadoopArrowFlightServerTest {

    private HadoopArrowFlightServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            HazelcastInstance hz = hazelcastInstance(server);
            if (hz != null) {
                hz.shutdown();
            }
        }
    }

    // ── single-node: skip join wait ─────────────────────────────────────────

    @Test
    void singleNodeSkipsClusterJoinWait() {
        int port = findFreePort();
        server = new HadoopArrowFlightServer();

        server.setupHazelcast(port, "127.0.0.1");

        HazelcastInstance hz = hazelcastInstance(server);
        assertNotNull(hz);
        assertEquals(1, hz.getCluster().getMembers().size(),
                "Single-node should have exactly 1 member");
    }

    @Test
    void singleNodeWithLocalhostWorks() {
        int port = findFreePort();
        server = new HadoopArrowFlightServer();

        server.setupHazelcast(port, "localhost");

        HazelcastInstance hz = hazelcastInstance(server);
        assertNotNull(hz);
        assertEquals(1, hz.getCluster().getMembers().size());
    }

    // ── timeout behaviour ───────────────────────────────────────────────────

    @Test
    void clusterJoinFailsWithClearErrorWhenTimeoutExceeded() {
        int port = findFreePort();
        server = new HadoopArrowFlightServer();

        // Set a short timeout via system property
        System.setProperty("hazelcastClusterJoinTimeoutSec", "2");

        try {
            // Request 3 nodes; only 1 will ever start. Must fail after timeout.
            long start = System.currentTimeMillis();
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    server.setupHazelcast(port, "127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703")
            );
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed >= 1900,
                    "Timeout should take at least ~2s, took " + elapsed + "ms");
            assertTrue(ex.getMessage().contains("timeout"),
                    "Error message must mention 'timeout': " + ex.getMessage());
            assertTrue(ex.getMessage().contains("1 of 3"),
                    "Error message must show member count: " + ex.getMessage());

        } finally {
            System.clearProperty("hazelcastClusterJoinTimeoutSec");
        }
    }

    @Test
    void clusterJoinTimeoutIsConfigurable() {
        int port = findFreePort();
        server = new HadoopArrowFlightServer();

        System.setProperty("hazelcastClusterJoinTimeoutSec", "1");

        try {
            long start = System.currentTimeMillis();
            assertThrows(IllegalStateException.class, () ->
                    server.setupHazelcast(port, "127.0.0.1:5701", "127.0.0.1:5702")
            );
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed < 5000,
                    "Short timeout (1s) should fail quickly, took " + elapsed + "ms");

        } finally {
            System.clearProperty("hazelcastClusterJoinTimeoutSec");
        }
    }

    // ── util ────────────────────────────────────────────────────────────────

    private static int findFreePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HazelcastInstance hazelcastInstance(HadoopArrowFlightServer server) {
        try {
            Field field = HadoopArrowFlightServer.class.getDeclaredField("hazelcastInstance");
            field.setAccessible(true);
            return (HazelcastInstance) field.get(server);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access hazelcastInstance via reflection", e);
        }
    }
}
