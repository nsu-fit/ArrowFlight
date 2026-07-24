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
    long queryEngineMemoryLimitBytes,
    int maxConcurrentQueries,
    int duckDbMemorySharePercent,
    int maxQueuedQueries,
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
    int clientConnectTimeoutMs
) {
    /**
     * Returns the DuckDB portion of the native query-engine budget.
     *
     * @return DuckDB memory pool in bytes
     */
    public long duckDbMemoryPoolBytes() {
        return percentage(queryEngineMemoryLimitBytes, duckDbMemorySharePercent);
    }

    /**
     * Returns the per-query DuckDB memory limit.
     *
     * @return per-query DuckDB limit in bytes
     */
    public long duckDbQueryMemoryLimitBytes() {
        return duckDbMemoryPoolBytes() / maxConcurrentQueries;
    }

    /**
     * Returns the Arrow root allocator limit.
     *
     * @return Arrow root limit in bytes
     */
    public long arrowMemoryPoolBytes() {
        return queryEngineMemoryLimitBytes - duckDbMemoryPoolBytes();
    }

    /**
     * Returns the per-query Arrow child allocator limit.
     *
     * @return per-query Arrow limit in bytes
     */
    public long arrowQueryMemoryLimitBytes() {
        return percentage(arrowMemoryPoolBytes(), 90) / maxConcurrentQueries;
    }

    /**
     * Returns the Arrow memory retained for shared allocations.
     *
     * @return Arrow shared reserve in bytes
     */
    public long arrowSharedReserveBytes() {
        return arrowMemoryPoolBytes()
                - arrowQueryMemoryLimitBytes() * maxConcurrentQueries;
    }

    /**
     * Calculates an integer percentage without multiplication overflow.
     *
     * @param value source value
     * @param percent percentage
     * @return percentage of value
     */
    private static long percentage(long value, int percent) {
        return value / 100L * percent + value % 100L * percent / 100L;
    }
}
