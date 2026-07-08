package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class HeartbeatAndRetryTest {

    private static BufferAllocator allocator;
    private static HazelcastInstance hazelcast;
    private static HadoopFlightSqlService sqlService;
    private IMap<String, Long> heartbeats;
    private IMap<String, Long> registry;

    @BeforeAll
    static void setUp() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        allocator = new RootAllocator(Long.MAX_VALUE);

        Config hz = new Config();
        hz.setClusterName("test-hb-" + UUID.randomUUID());
        hz.getNetworkConfig().setPort(5701);
        hz.getNetworkConfig().setPortAutoIncrement(true);
        hz.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hz.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hazelcast = Hazelcast.newHazelcastInstance(hz);

        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        ParquetManager parquetManager = new ParquetManager(localFs, dataDir, "localhost");

        Location loc = Location.forGrpcInsecure("localhost", 32011);
        sqlService = new HadoopFlightSqlService(loc, parquetManager, allocator, hazelcast);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (sqlService != null) sqlService.close();
        if (hazelcast != null) hazelcast.shutdown();
        if (allocator != null) allocator.close();
    }

    @BeforeEach
    void initMaps() {
        heartbeats = hazelcast.getMap("server-heartbeats");
        registry = hazelcast.getMap("server-registry");
        // Re-register self — may have been cleared by a previous test
        registry.put("grpc+tcp://localhost:32011", 0L);
        heartbeats.put("grpc+tcp://localhost:32011", System.currentTimeMillis());
    }

    @AfterEach
    void clearTestEntries() {
        String self = "grpc+tcp://localhost:32011";
        // Remove all entries except self-registration
        for (String key : registry.keySet().toArray(new String[0])) {
            if (!self.equals(key)) {
                registry.remove(key);
            }
        }
        for (String key : heartbeats.keySet().toArray(new String[0])) {
            if (!self.equals(key)) {
                heartbeats.remove(key);
            }
        }
    }

    @Test
    void selfRegisteredAndHasHeartbeat() {
        assertTrue(registry.size() >= 1, "At least self should be registered");
        Long hb = heartbeats.get("grpc+tcp://localhost:32011");
        assertNotNull(hb, "Self should have heartbeat");
        assertTrue(hb > 0, "Heartbeat timestamp should be positive");
    }

    @Test
    void liveServerPassesFilter() {
        String liveUri = "grpc+tcp://server1:32010";
        registry.put(liveUri, 0L);
        heartbeats.put(liveUri, System.currentTimeMillis());

        Set<String> input = Set.of(liveUri);
        Set<String> result = sqlService.filterLiveServers(input);

        assertEquals(1, result.size());
        assertTrue(result.contains(liveUri));
    }

    @Test
    void staleServerIsRemoved() {
        String staleUri = "grpc+tcp://dead-server:32010";
        registry.put(staleUri, 100L);
        // Heartbeat from 120 seconds ago — well past the 45s timeout
        heartbeats.put(staleUri, System.currentTimeMillis() - 120_000);

        Set<String> input = Set.of(staleUri);
        Set<String> result = sqlService.filterLiveServers(input);

        assertTrue(result.isEmpty(), "Stale server should be filtered out");
        assertNull(registry.get(staleUri), "Stale server should be removed from registry");
        assertNull(heartbeats.get(staleUri), "Stale server should be removed from heartbeats");
    }

    @Test
    void noHeartbeatYetIsConsideredLive() {
        String newUri = "grpc+tcp://just-joined:32010";
        registry.put(newUri, 0L);
        // No heartbeat at all — just registered, should be treated as live

        Set<String> input = Set.of(newUri);
        Set<String> result = sqlService.filterLiveServers(input);

        assertEquals(1, result.size());
        assertTrue(result.contains(newUri));
    }

    @Test
    void mixedLiveAndStaleServers() {
        String live1 = "grpc+tcp://live1:32010";
        String live2 = "grpc+tcp://live2:32010";
        String dead1 = "grpc+tcp://dead1:32010";
        String dead2 = "grpc+tcp://dead2:32010";

        registry.put(live1, 50L);
        registry.put(live2, 200L);
        registry.put(dead1, 100L);
        registry.put(dead2, 300L);

        heartbeats.put(live1, System.currentTimeMillis());
        heartbeats.put(live2, System.currentTimeMillis());
        heartbeats.put(dead1, System.currentTimeMillis() - 120_000);
        heartbeats.put(dead2, System.currentTimeMillis() - 60_000);

        Set<String> input = new LinkedHashSet<>();
        input.add(live1);
        input.add(dead1);
        input.add(live2);
        input.add(dead2);

        Set<String> result = sqlService.filterLiveServers(input);

        assertEquals(2, result.size());
        assertTrue(result.contains(live1));
        assertTrue(result.contains(live2));
        assertFalse(result.contains(dead1));
        assertFalse(result.contains(dead2));

        assertNull(registry.get(dead1), "Dead server should be removed from registry");
        assertNull(registry.get(dead2), "Dead server should be removed from registry");
    }

    @Test
    void emptyInputReturnsEmpty() {
        Set<String> result = sqlService.filterLiveServers(Set.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void heartbeatUpdatesPeriodically() throws Exception {
        String testUri = "grpc+tcp://localhost:32011";
        Long firstHb = heartbeats.get(testUri);
        assertNotNull(firstHb);

        // Wait for one heartbeat cycle (15s interval)
        Thread.sleep(16_000);

        Long secondHb = heartbeats.get(testUri);
        assertNotNull(secondHb);
        assertTrue(secondHb > firstHb,
                "Heartbeat should have been updated. First: " + firstHb + ", Second: " + secondHb);
    }
}
