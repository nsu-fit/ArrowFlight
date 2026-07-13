package net.surpin.data.arrowflight.server.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    void constructorAndAllAccessors() {
        AppConfig cfg = new AppConfig(
                4096, 32, 131072, 8, 8, 1,
                "/ext/hdfs.duckdb_extension", true,
                "hdfs://namenode:8020", "namenode1,namenode2",
                "true", "/var/lib/hadoop-hdfs/dn_socket",
                1048576, 60000L,
                "/data/parquet", 32010, 5701, 60,
                3, 1000, 30000
        );

        assertEquals(4096, cfg.batchSize());
        assertEquals(32, cfg.ioParallelism());
        assertEquals(131072, cfg.ioFileBufferSize());
        assertEquals(8, cfg.duckDbWarmConnections());
        assertEquals(8, cfg.duckDbGroups());
        assertEquals(1, cfg.duckDbThreads());
        assertEquals("/ext/hdfs.duckdb_extension", cfg.duckDbHdfsExtension());
        assertTrue(cfg.duckDbAllowUnsignedExtensions());
        assertEquals("hdfs://namenode:8020", cfg.duckDbHdfsDefaultNamenode());
        assertEquals("namenode1,namenode2", cfg.duckDbHdfsHaNamenodes());
        assertEquals("true", cfg.duckDbHdfsShortcircuit());
        assertEquals("/var/lib/hadoop-hdfs/dn_socket", cfg.duckDbHdfsDomainSocketPath());
        assertEquals(1048576, cfg.grpcMaxInboundMessageSize());
        assertEquals(60000L, cfg.flightListenerReadyTimeoutMillis());
        assertEquals("/data/parquet", cfg.dataDir());
        assertEquals(32010, cfg.port());
        assertEquals(5701, cfg.hazelcastPort());
        assertEquals(60, cfg.hazelcastClusterJoinTimeoutSec());
        assertEquals(3, cfg.clientMaxRetries());
        assertEquals(1000, cfg.clientRetryBackoffMs());
        assertEquals(30000, cfg.clientConnectTimeoutMs());
    }

    @Test
    void equalsSameValues() {
        AppConfig a = new AppConfig(1, 2, 3, 4, 5, 6, "a", false, "b", "c", "d", "e", 7, 8L, "f", 9, 10, 11, 12, 13, 14);
        AppConfig b = new AppConfig(1, 2, 3, 4, 5, 6, "a", false, "b", "c", "d", "e", 7, 8L, "f", 9, 10, 11, 12, 13, 14);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
