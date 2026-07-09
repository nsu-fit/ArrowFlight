package net.surpin.data.arrowflight.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import net.surpin.data.arrowflight.server.HadoopFlightSqlService;
import net.surpin.data.arrowflight.server.db.ParquetManager;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ClientAuthIntegrationTest {

    private static FlightServer flightServer;
    private static HazelcastInstance hz;
    private static RootAllocator allocator;
    private static String host;
    private static int port;

    @BeforeAll
    static void startServer() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        allocator = new RootAllocator(Long.MAX_VALUE);

        Config hzCfg = new Config();
        hzCfg.setClusterName("client-auth-" + UUID.randomUUID());
        hzCfg.getNetworkConfig().setPort(findFreePort());
        hzCfg.getNetworkConfig().setPortAutoIncrement(false);
        hzCfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hzCfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(hzCfg);

        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new org.apache.hadoop.conf.Configuration());
        ParquetManager parquetManager = new ParquetManager(localFs, dataDir, "localhost");

        host = "localhost";
        port = findFreePort();
        Location location = Location.forGrpcInsecure(host, port);
        HadoopFlightSqlService sqlService =
                new HadoopFlightSqlService(location, parquetManager, allocator, hz);
        flightServer = FlightServer.builder(allocator, location, sqlService).build();
        flightServer.start();
    }

    @AfterAll
    static void stopAll() {
        if (flightServer != null) flightServer.shutdown();
        if (hz != null) hz.shutdown();
        if (allocator != null) allocator.close();
    }

    // ── regression: bearer token + routing headers ─────────────────────────

    @Test
    void bearerTokenWithRoutingHeadersDoesNotThrowClassCastException() {
        // Before the fix, Client.authenticate() did (CredentialCallOption) callOptions.get(0),
        // but when clientProperties (HeaderCallOption) was inserted first, index 0 was not
        // a CredentialCallOption, causing a ClassCastException at runtime.
        Configuration config = new Configuration(host, port, "test-user", null, "test-token");
        config.setDefaultSchema("test_schema");
        config.setRoutingTag("batch");
        config.setRoutingQueue("high");

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Bearer-token auth with routing headers must not throw ClassCastException");

        assertNotNull(client);

        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0, "Query must return rows: got " + rows);
    }

    // ── sanity: other modes still work ─────────────────────────────────────

    @Test
    void bearerTokenWithoutRoutingHeadersStillWorks() {
        Configuration config = new Configuration(host, port, "test-user", null, "test-token-2");

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Plain bearer-token auth must still work");

        assertNotNull(client);
        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0);
    }

    @Test
    void passwordAuthStillWorks() {
        Configuration config = new Configuration(host, port, "test-user", "test-pass", null);

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Password-based auth must still work");

        assertNotNull(client);
        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0);
    }

    // ── util ────────────────────────────────────────────────────────────────

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
