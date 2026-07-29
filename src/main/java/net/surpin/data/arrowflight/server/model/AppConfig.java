package net.surpin.data.arrowflight.server.model;

/**
 * Server configuration loaded at startup.
 * Values are resolved from arrowflight.properties, system properties, and environment variables.
 */
public record AppConfig(
    int numServers,
    int batchSize,
    int ioParallelism,
    int ioFileBufferSize,
    int duckDbWarmConnections,
    int duckDbGroups,
    int duckDbThreads,
    String duckDbHdfsExtension,
    boolean duckDbAllowUnsignedExtensions,
    String duckDbHdfsDefaultNamenode,
    String duckDbHdfsHaNamenodes,
    String duckDbHdfsShortcircuit,
    String duckDbHdfsDomainSocketPath,
    boolean metricsEnabled,
    int grpcMaxInboundMessageSize,
    int flightBackpressureThresholdBytes,
    long flightListenerReadyTimeoutMillis,
    String dataDir,
    String localDataDir,
    int port,
    int hazelcastPort,
    int hazelcastClusterJoinTimeoutSec,
    int clientMaxRetries,
    int clientRetryBackoffMs,
    int clientConnectTimeoutMs,
    SchedulerConfig scheduler
) {

    /**
     * Creates configuration with adaptive scheduling disabled for legacy callers.
     *
     * @param numServers expected Flight server count
     * @param batchSize Arrow batch size
     * @param ioParallelism I/O worker count
     * @param ioFileBufferSize Hadoop I/O buffer size
     * @param duckDbWarmConnections DuckDB warm connection count
     * @param duckDbGroups DuckDB aggregation group count
     * @param duckDbThreads threads per DuckDB query
     * @param duckDbHdfsExtension DuckDB HDFS extension path
     * @param duckDbAllowUnsignedExtensions whether unsigned extensions are allowed
     * @param duckDbHdfsDefaultNamenode default HDFS namenode
     * @param duckDbHdfsHaNamenodes HDFS HA namenodes
     * @param duckDbHdfsShortcircuit HDFS short-circuit setting
     * @param duckDbHdfsDomainSocketPath HDFS domain socket path
     * @param metricsEnabled whether Prometheus metrics are enabled
     * @param grpcMaxInboundMessageSize maximum inbound gRPC message size
     * @param flightBackpressureThresholdBytes Flight backpressure threshold
     * @param flightListenerReadyTimeoutMillis listener readiness timeout
     * @param dataDir Parquet data directory
     * @param localDataDir optional local data directory
     * @param port Flight server port
     * @param hazelcastPort Hazelcast port
     * @param hazelcastClusterJoinTimeoutSec cluster join timeout
     * @param clientMaxRetries client retry count
     * @param clientRetryBackoffMs client retry backoff
     * @param clientConnectTimeoutMs client connection timeout
     */
    public AppConfig(
            int numServers,
            int batchSize,
            int ioParallelism,
            int ioFileBufferSize,
            int duckDbWarmConnections,
            int duckDbGroups,
            int duckDbThreads,
            String duckDbHdfsExtension,
            boolean duckDbAllowUnsignedExtensions,
            String duckDbHdfsDefaultNamenode,
            String duckDbHdfsHaNamenodes,
            String duckDbHdfsShortcircuit,
            String duckDbHdfsDomainSocketPath,
            boolean metricsEnabled,
            int grpcMaxInboundMessageSize,
            int flightBackpressureThresholdBytes,
            long flightListenerReadyTimeoutMillis,
            String dataDir,
            String localDataDir,
            int port,
            int hazelcastPort,
            int hazelcastClusterJoinTimeoutSec,
            int clientMaxRetries,
            int clientRetryBackoffMs,
            int clientConnectTimeoutMs) {
        this(
                numServers, batchSize, ioParallelism, ioFileBufferSize,
                duckDbWarmConnections, duckDbGroups, duckDbThreads,
                duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes,
                duckDbHdfsShortcircuit, duckDbHdfsDomainSocketPath,
                metricsEnabled, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes,
                flightListenerReadyTimeoutMillis, dataDir, localDataDir,
                port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                SchedulerConfig.disabled());
    }
}
