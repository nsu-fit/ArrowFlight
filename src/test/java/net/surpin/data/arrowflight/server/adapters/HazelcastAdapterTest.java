package net.surpin.data.arrowflight.server.adapters;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import net.surpin.data.arrowflight.server.model.AppConfig;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.Serializable;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HazelcastAdapterTest {

    private static final AppConfig CFG = new AppConfig(
            3, 4096, 1, 131072, 1, 1, 1,
            null, false, null, null,
            "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
            false,
            1048576, 60000L, "/data/parquet", null, 32010, 5701, 60,
            3, 1000, 30000);

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K, V> IMap<K, V> stubMap(HazelcastInstance instance, String mapName) {
        IMap map = mock(IMap.class);
        when(instance.getMap(mapName)).thenReturn(map);
        return map;
    }

    @Test
    void constructorCreatesInstanceAndMaps() {
        try (MockedStatic<Hazelcast> hazelcastMock = mockStatic(Hazelcast.class)) {
            HazelcastInstance mockInstance = mock(HazelcastInstance.class);
            when(Hazelcast.newHazelcastInstance(any())).thenReturn(mockInstance);
            stubMap(mockInstance, "server-registry");
            stubMap(mockInstance, "statement-cache");
            stubMap(mockInstance, "server-heartbeats");
            stubMap(mockInstance, "server-files");

            HazelcastAdapter adapter = new HazelcastAdapter(CFG);

            assertNotNull(adapter.serverRegistry());
            assertNotNull(adapter.statementCache());
            assertNotNull(adapter.serverHeartbeats());
            assertNotNull(adapter.serverFiles());
            assertSame(mockInstance, adapter.instance());
        }
    }

    @Test
    void serverRegistryReturnsCorrectMap() {
        try (MockedStatic<Hazelcast> hazelcastMock = mockStatic(Hazelcast.class)) {
            HazelcastInstance mockInstance = mock(HazelcastInstance.class);
            when(Hazelcast.newHazelcastInstance(any())).thenReturn(mockInstance);
            IMap<String, Long> registry = stubMap(mockInstance, "server-registry");
            stubMap(mockInstance, "statement-cache");
            stubMap(mockInstance, "server-heartbeats");
            stubMap(mockInstance, "server-files");

            HazelcastAdapter adapter = new HazelcastAdapter(CFG);
            assertSame(registry, adapter.serverRegistry());
        }
    }

    @Test
    void closeShutsDownInstance() {
        try (MockedStatic<Hazelcast> hazelcastMock = mockStatic(Hazelcast.class)) {
            HazelcastInstance mockInstance = mock(HazelcastInstance.class);
            when(Hazelcast.newHazelcastInstance(any())).thenReturn(mockInstance);
            stubMap(mockInstance, "server-registry");
            stubMap(mockInstance, "statement-cache");
            stubMap(mockInstance, "server-heartbeats");
            stubMap(mockInstance, "server-files");

            HazelcastAdapter adapter = new HazelcastAdapter(CFG);
            adapter.close();
            verify(mockInstance).shutdown();
        }
    }
}
