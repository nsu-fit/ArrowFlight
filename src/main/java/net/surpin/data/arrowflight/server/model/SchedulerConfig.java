package net.surpin.data.arrowflight.server.model;

/**
 * Configures adaptive query admission and load-aware cluster scheduling.
 *
 * @param enabled whether adaptive admission and snapshot-aware scheduling are enabled
 * @param snapshotIntervalMillis node snapshot publication interval
 * @param snapshotStaleMillis maximum accepted node snapshot age
 * @param controlIntervalMillis minimum interval between concurrency-limit changes
 * @param minConcurrentQueries minimum number of simultaneously executing queries
 * @param maxConcurrentQueries maximum number of simultaneously executing queries
 * @param maxQueuedQueries maximum number of locally queued queries
 * @param maxQueueWaitMillis maximum time a query may wait for admission
 * @param cpuLowWatermark CPU utilization below which concurrency may increase
 * @param cpuHighWatermark CPU utilization above which concurrency decreases
 * @param memoryLowWatermark memory utilization below which concurrency may increase
 * @param memoryHighWatermark memory utilization above which concurrency decreases
 * @param remoteLocalityPenaltyMillis scheduling penalty for a non-local shared-storage read
 * @param redirectEnabled whether queued DoGet requests may move to another node
 * @param redirectAfterMillis queue wait before cross-node redirect is considered
 * @param maxRedirects maximum number of redirects for one endpoint
 * @param redirectMinScoreImprovement minimum fractional score improvement for redirect
 */
public record SchedulerConfig(
        boolean enabled,
        long snapshotIntervalMillis,
        long snapshotStaleMillis,
        long controlIntervalMillis,
        int minConcurrentQueries,
        int maxConcurrentQueries,
        int maxQueuedQueries,
        long maxQueueWaitMillis,
        double cpuLowWatermark,
        double cpuHighWatermark,
        double memoryLowWatermark,
        double memoryHighWatermark,
        long remoteLocalityPenaltyMillis,
        boolean redirectEnabled,
        long redirectAfterMillis,
        int maxRedirects,
        double redirectMinScoreImprovement) {

    /**
     * Creates configuration compatible with the original scheduler fields.
     *
     * @param enabled whether adaptive scheduling is enabled
     * @param snapshotIntervalMillis snapshot publication interval
     * @param snapshotStaleMillis maximum snapshot age
     * @param controlIntervalMillis concurrency control interval
     * @param minConcurrentQueries minimum concurrent queries
     * @param maxConcurrentQueries maximum concurrent queries
     * @param maxQueuedQueries maximum queued queries
     * @param maxQueueWaitMillis maximum queue wait
     * @param cpuLowWatermark CPU scale-up threshold
     * @param cpuHighWatermark CPU scale-down threshold
     * @param memoryLowWatermark memory scale-up threshold
     * @param memoryHighWatermark memory scale-down threshold
     * @param remoteLocalityPenaltyMillis remote-read scheduling penalty
     */
    public SchedulerConfig(
            boolean enabled,
            long snapshotIntervalMillis,
            long snapshotStaleMillis,
            long controlIntervalMillis,
            int minConcurrentQueries,
            int maxConcurrentQueries,
            int maxQueuedQueries,
            long maxQueueWaitMillis,
            double cpuLowWatermark,
            double cpuHighWatermark,
            double memoryLowWatermark,
            double memoryHighWatermark,
            long remoteLocalityPenaltyMillis) {
        this(
                enabled, snapshotIntervalMillis, snapshotStaleMillis,
                controlIntervalMillis, minConcurrentQueries,
                maxConcurrentQueries, maxQueuedQueries, maxQueueWaitMillis,
                cpuLowWatermark, cpuHighWatermark,
                memoryLowWatermark, memoryHighWatermark,
                remoteLocalityPenaltyMillis,
                enabled, 500L, 2, 0.30);
    }

    /**
     * Creates a permissive configuration for compatibility-only construction paths.
     *
     * @return disabled scheduler configuration
     */
    public static SchedulerConfig disabled() {
        return new SchedulerConfig(
                false, 1_000L, 5_000L, 5_000L,
                1, 1_024, 1_024, 300_000L,
                0.65, 0.90, 0.70, 0.85, 250L,
                false, 500L, 0, 0.30);
    }
}
