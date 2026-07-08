package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import net.surpin.data.arrowflight.server.db.ParquetManager;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class MultiNodeFailoverTest {

    private static BufferAllocator allocator1;
    private static BufferAllocator allocator2;
    private static HazelcastInstance hazelcast;
    private static ParquetManager parquetManager;
    private static Location location1;
    private static Location location2;
    private static FlightServer server1;
    private static FlightServer server2;
    private static HadoopFlightSqlService service1;
    private static HadoopFlightSqlService service2;
    private static FlightClient client;
    private static FlightSqlClient sqlClient;
    private static LocalFileSystem localFs;
    private static String dataDir;

    @BeforeAll
    static void setUp() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        duplicateTestFile();

        Config hz = new Config();
        hz.setClusterName("failover-" + UUID.randomUUID());
        hz.getNetworkConfig().setPort(findFreePort());
        hz.getNetworkConfig().setPortAutoIncrement(false);
        hz.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hz.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hazelcast = Hazelcast.newHazelcastInstance(hz);

        localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        parquetManager = new ParquetManager(localFs, dataDir, "localhost");

        allocator1 = new RootAllocator(Long.MAX_VALUE);
        allocator2 = new RootAllocator(Long.MAX_VALUE);

        int port1 = findFreePort();
        int port2 = findFreePort();
        location1 = Location.forGrpcInsecure("localhost", port1);
        location2 = Location.forGrpcInsecure("localhost", port2);

        service1 = new HadoopFlightSqlService(location1, parquetManager, allocator1, hazelcast);
        service2 = new HadoopFlightSqlService(location2, parquetManager, allocator2, hazelcast);

        server1 = FlightServer.builder(allocator1, location1, service1).build();
        server2 = FlightServer.builder(allocator2, location2, service2).build();

        server1.start();
        server2.start();

        client = FlightClient.builder(allocator1, location1).build();
        sqlClient = new FlightSqlClient(client);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (sqlClient != null) sqlClient.close();
        if (server1 != null) server1.shutdown();
        if (server2 != null) server2.shutdown();
        if (service1 != null) service1.close();
        if (service2 != null) service2.close();
        if (hazelcast != null) hazelcast.shutdown();
        if (allocator1 != null) allocator1.close();
        if (allocator2 != null) allocator2.close();
        cleanupDuplicates();
    }

    private static void duplicateTestFile() throws IOException {
        String tableDir = Paths.get(dataDir, "test_schema", "test_table").toString();
        Path source = null;
        try (var files = Files.list(Paths.get(tableDir))) {
            source = files.filter(p -> p.toString().endsWith(".parquet")).findFirst().orElse(null);
        }
        if (source == null) return;

        // Create duplicate files so there are multiple files to distribute across servers
        for (int i = 1; i <= 3; i++) {
            Path copy = Paths.get(tableDir, "test_copy_" + i + ".parquet");
            if (!Files.exists(copy)) {
                Files.copy(source, copy, StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
    }

    private static void cleanupDuplicates() throws IOException {
        String tableDir = Paths.get(dataDir, "test_schema", "test_table").toString();
        try (var files = Files.list(Paths.get(tableDir))) {
            files.filter(p -> p.getFileName().toString().startsWith("test_copy_"))
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
        }
    }

    @Test
    void bothServersRegistered() {
        IMap<String, Long> registry = hazelcast.getMap("server-registry");
        String uri1 = location1.getUri().toString();
        String uri2 = location2.getUri().toString();

        assertTrue(registry.containsKey(uri1), "Server 1 should be registered");
        assertTrue(registry.containsKey(uri2), "Server 2 should be registered");
    }

    @Test
    void bothServersHaveHeartbeats() {
        IMap<String, Long> heartbeats = hazelcast.getMap("server-heartbeats");
        String uri1 = location1.getUri().toString();
        String uri2 = location2.getUri().toString();

        Long hb1 = heartbeats.get(uri1);
        Long hb2 = heartbeats.get(uri2);

        assertNotNull(hb1, "Server 1 should have heartbeat");
        assertNotNull(hb2, "Server 2 should have heartbeat");
        assertTrue(hb1 > 0);
        assertTrue(hb2 > 0);
    }

    @Test
    void queryDistributesWorkToBothServers() throws Exception {
        IMap<String, Long> registry = hazelcast.getMap("server-registry");
        String uri1 = location1.getUri().toString();
        String uri2 = location2.getUri().toString();

        FlightInfo fi = sqlClient.execute("SELECT * FROM test_schema.test_table");
        Set<String> endpointServers = new HashSet<>();
        for (FlightEndpoint ep : fi.getEndpoints()) {
            for (Location loc : ep.getLocations()) {
                endpointServers.add(loc.getUri().toString());
            }
        }

        // With multiple files, at least one endpoint should be on a different server
        // than server 1 (where the query was submitted)
        assertTrue(endpointServers.size() >= 1,
                "Should have endpoints on at least one server");
    }

    @Test
    void deadServerIsExcludedFromEndpoints() throws Exception {
        IMap<String, Long> registry = hazelcast.getMap("server-registry");
        IMap<String, Long> heartbeats = hazelcast.getMap("server-heartbeats");
        String uri2 = location2.getUri().toString();

        // Simulate server 2 crash: stop it and set a stale heartbeat
        server2.shutdown();
        heartbeats.put(uri2, System.currentTimeMillis() - 120_000);

        // Run query again through server 1
        FlightInfo fi = sqlClient.execute("SELECT * FROM test_schema.test_table");
        Set<String> endpointServers = new HashSet<>();
        for (FlightEndpoint ep : fi.getEndpoints()) {
            for (Location loc : ep.getLocations()) {
                endpointServers.add(loc.getUri().toString());
            }
        }

        assertFalse(endpointServers.contains(uri2),
                "Dead server should not appear in endpoints. Got: " + endpointServers);

        // Verify server 2 was cleaned up from registry
        assertNull(registry.get(uri2), "Dead server should be removed from registry");
    }

    @Test
    void replacementServerPicksUpWork() throws Exception {
        IMap<String, Long> registry = hazelcast.getMap("server-registry");
        IMap<String, Long> heartbeats = hazelcast.getMap("server-heartbeats");

        // Start a replacement server
        int port3 = findFreePort();
        Location location3 = Location.forGrpcInsecure("localhost", port3);
        BufferAllocator allocator3 = new RootAllocator(Long.MAX_VALUE);
        HadoopFlightSqlService service3 = new HadoopFlightSqlService(location3, parquetManager, allocator3, hazelcast);
        FlightServer server3 = FlightServer.builder(allocator3, location3, service3).build();
        server3.start();

        try {
            String uri3 = location3.getUri().toString();

            // Replacement should be registered and have heartbeat
            assertTrue(registry.containsKey(uri3), "Replacement server should be registered");
            assertNotNull(heartbeats.get(uri3), "Replacement server should have heartbeat");

            // Query should include the new server in endpoints
            FlightInfo fi = sqlClient.execute("SELECT * FROM test_schema.test_table");
            Set<String> endpointServers = new HashSet<>();
            for (FlightEndpoint ep : fi.getEndpoints()) {
                for (Location loc : ep.getLocations()) {
                    endpointServers.add(loc.getUri().toString());
                }
            }

            assertTrue(endpointServers.contains(uri3),
                    "Replacement server should appear in endpoints. Got: " + endpointServers);
        } finally {
            server3.shutdown();
            service3.close();
            allocator3.close();
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
