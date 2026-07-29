package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Describes one Flight node's recent execution capacity and resource pressure.
 *
 * @param observedAtMillis wall-clock time when the snapshot was collected
 * @param concurrencyLimit current local execution limit
 * @param activeQueries currently executing queries
 * @param queuedQueries queries waiting for local admission
 * @param processCpuLoad process CPU utilization in the range zero to one
 * @param systemCpuLoad total node or container CPU utilization in the range zero to one
 * @param memoryPressure maximum JVM, Arrow, system, or cgroup memory utilization
 * @param throughputBytesPerSecond moving-average successful query throughput
 * @param acceptingRequests whether the node can accept another planned endpoint
 */
public record NodeLoadSnapshot(
        long observedAtMillis,
        int concurrencyLimit,
        int activeQueries,
        int queuedQueries,
        double processCpuLoad,
        double systemCpuLoad,
        double memoryPressure,
        long throughputBytesPerSecond,
        boolean acceptingRequests) implements Serializable {

    /**
     * Creates a snapshot compatible with the original process-only CPU sample.
     *
     * @param observedAtMillis wall-clock observation time
     * @param concurrencyLimit local execution limit
     * @param activeQueries active query count
     * @param queuedQueries queued query count
     * @param processCpuLoad process CPU utilization
     * @param memoryPressure managed or Arrow memory utilization
     * @param throughputBytesPerSecond successful query throughput
     * @param acceptingRequests whether the node accepts planned work
     */
    public NodeLoadSnapshot(
            long observedAtMillis,
            int concurrencyLimit,
            int activeQueries,
            int queuedQueries,
            double processCpuLoad,
            double memoryPressure,
            long throughputBytesPerSecond,
            boolean acceptingRequests) {
        this(
                observedAtMillis, concurrencyLimit, activeQueries,
                queuedQueries, processCpuLoad, processCpuLoad,
                memoryPressure, throughputBytesPerSecond, acceptingRequests);
    }

    /**
     * Checks whether this snapshot is recent enough for scheduling.
     *
     * @param nowMillis current wall-clock time
     * @param staleAfterMillis maximum allowed age
     * @return whether the snapshot is fresh
     */
    public boolean isFresh(long nowMillis, long staleAfterMillis) {
        return observedAtMillis > 0L
                && observedAtMillis >= nowMillis - staleAfterMillis;
    }
}
