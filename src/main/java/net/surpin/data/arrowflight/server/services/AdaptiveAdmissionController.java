package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Bounds local query concurrency and adapts it to recent CPU and memory pressure.
 */
public final class AdaptiveAdmissionController {

    private static final long CANCELLATION_POLL_MILLIS = 100L;
    private static final double DECREASE_FACTOR = 0.70;
    private static final double THROUGHPUT_ALPHA = 0.20;

    private final SchedulerConfig config;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition capacityChanged = lock.newCondition();
    private final AtomicLong throughputBytesPerSecond = new AtomicLong();

    private int concurrencyLimit;
    private int activeQueries;
    private int queuedQueries;
    private long lastControlNanos;

    /**
     * Creates an admission controller using the supplied scheduler configuration.
     *
     * @param config scheduler configuration
     */
    public AdaptiveAdmissionController(SchedulerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.concurrencyLimit = config.maxConcurrentQueries();
        publishMetrics();
    }

    /**
     * Creates a controller that does not practically constrain compatibility paths.
     *
     * @return permissive admission controller
     */
    public static AdaptiveAdmissionController permissive() {
        return new AdaptiveAdmissionController(SchedulerConfig.disabled());
    }

    /**
     * Waits for local execution capacity and returns a permit.
     *
     * @param cancelled reports whether the Flight client cancelled the stream
     * @return execution permit that must be closed
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws AdmissionRejectedException if the queue is full or wait deadline expires
     * @throws AdmissionCancelledException if the client cancels while queued
     */
    public Permit acquire(BooleanSupplier cancelled) throws InterruptedException {
        return acquire(cancelled, false);
    }

    /**
     * Waits for capacity and optionally requests cross-node reassignment after a delay.
     *
     * @param cancelled reports whether the Flight client cancelled the stream
     * @param redirectEligible whether this endpoint may move to another node
     * @return execution permit that must be closed
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws AdmissionRejectedException if the queue is full or wait deadline expires
     * @throws AdmissionCancelledException if the client cancels while queued
     * @throws AdmissionRedirectException when another node should be evaluated
     */
    public Permit acquire(
            BooleanSupplier cancelled,
            boolean redirectEligible) throws InterruptedException {
        Objects.requireNonNull(cancelled, "cancelled");
        lock.lockInterruptibly();
        try {
            if (activeQueries < concurrencyLimit) {
                return admit();
            }
            if (queuedQueries >= config.maxQueuedQueries()) {
                throw new AdmissionRejectedException("Flight query admission queue is full");
            }

            queuedQueries++;
            publishMetrics();
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(
                    config.maxQueueWaitMillis());
            long redirectRemainingNanos = TimeUnit.MILLISECONDS.toNanos(
                    config.redirectAfterMillis());
            try {
                while (activeQueries >= concurrencyLimit) {
                    if (cancelled.getAsBoolean()) {
                        throw new AdmissionCancelledException(
                                "Flight client cancelled while waiting for query admission");
                    }
                    if (remainingNanos <= 0L) {
                        throw new AdmissionRejectedException(
                                "Flight query admission wait timed out");
                    }
                    if (redirectEligible
                            && config.redirectEnabled()
                            && redirectRemainingNanos <= 0L) {
                        throw new AdmissionRedirectException(
                                "Flight query should be evaluated for redirect");
                    }
                    long waitNanos = Math.min(
                            remainingNanos,
                            TimeUnit.MILLISECONDS.toNanos(CANCELLATION_POLL_MILLIS));
                    if (redirectEligible && config.redirectEnabled()) {
                        waitNanos = Math.min(
                                waitNanos, redirectRemainingNanos);
                    }
                    long unconsumedNanos = capacityChanged.awaitNanos(waitNanos);
                    long consumedNanos = waitNanos
                            - Math.max(0L, unconsumedNanos);
                    remainingNanos -= consumedNanos;
                    redirectRemainingNanos -= consumedNanos;
                }
            } finally {
                queuedQueries--;
            }
            return admit();
        } finally {
            try {
                publishMetrics();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Adjusts the concurrency limit when resource pressure remains outside watermarks.
     *
     * @param cpuLoad process CPU utilization in the range zero to one
     * @param memoryPressure memory utilization in the range zero to one
     */
    public void updatePressure(double cpuLoad, double memoryPressure) {
        if (!config.enabled()) {
            return;
        }
        long now = System.nanoTime();
        lock.lock();
        try {
            if (lastControlNanos != 0L
                    && now - lastControlNanos
                    < TimeUnit.MILLISECONDS.toNanos(config.controlIntervalMillis())) {
                return;
            }
            int previous = concurrencyLimit;
            boolean overloaded = above(cpuLoad, config.cpuHighWatermark())
                    || above(memoryPressure, config.memoryHighWatermark());
            boolean underloaded = below(cpuLoad, config.cpuLowWatermark())
                    && below(memoryPressure, config.memoryLowWatermark());
            if (overloaded) {
                int reduced = Math.min(
                        concurrencyLimit - 1,
                        (int) Math.floor(concurrencyLimit * DECREASE_FACTOR));
                concurrencyLimit = Math.max(config.minConcurrentQueries(), reduced);
            } else if (underloaded
                    && (queuedQueries > 0 || activeQueries >= concurrencyLimit)) {
                concurrencyLimit = Math.min(
                        config.maxConcurrentQueries(), concurrencyLimit + 1);
            }
            if (concurrencyLimit != previous) {
                capacityChanged.signalAll();
                lastControlNanos = now;
            }
        } finally {
            try {
                publishMetrics();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Returns the current execution-concurrency limit.
     *
     * @return current concurrency limit
     */
    public int concurrencyLimit() {
        lock.lock();
        try {
            return concurrencyLimit;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of executing queries.
     *
     * @return active query count
     */
    public int activeQueries() {
        lock.lock();
        try {
            return activeQueries;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of queries waiting for execution.
     *
     * @return queued query count
     */
    public int queuedQueries() {
        lock.lock();
        try {
            return queuedQueries;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the moving-average successful query throughput.
     *
     * @return bytes processed per second
     */
    public long throughputBytesPerSecond() {
        return throughputBytesPerSecond.get();
    }

    /**
     * Returns whether another endpoint can be planned for this node.
     *
     * @return whether the local admission queue has capacity
     */
    public boolean acceptingRequests() {
        lock.lock();
        try {
            return activeQueries < concurrencyLimit
                    || queuedQueries < config.maxQueuedQueries();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Converts one available local slot into an execution permit.
     *
     * @return newly acquired execution permit
     */
    private Permit admit() {
        activeQueries++;
        publishMetrics();
        return new Permit(this, System.nanoTime());
    }

    /**
     * Releases one local permit and records successful work.
     *
     * @param permit completed permit
     */
    private void release(Permit permit) {
        lock.lock();
        try {
            if (permit.successful && permit.logicalBytes > 0L) {
                updateThroughput(permit.logicalBytes,
                        Math.max(1L, System.nanoTime() - permit.startedNanos));
            }
            activeQueries--;
            capacityChanged.signalAll();
        } finally {
            try {
                publishMetrics();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Updates successful query throughput using an exponential moving average.
     *
     * @param logicalBytes processed logical bytes
     * @param elapsedNanos execution duration
     */
    private void updateThroughput(long logicalBytes, long elapsedNanos) {
        long sample = (long) (logicalBytes * 1_000_000_000.0 / elapsedNanos);
        throughputBytesPerSecond.updateAndGet(previous -> previous == 0L
                ? sample
                : Math.max(1L, Math.round(previous * (1.0 - THROUGHPUT_ALPHA)
                        + sample * THROUGHPUT_ALPHA)));
    }

    /**
     * Publishes current admission counters to Prometheus gauges.
     */
    private void publishMetrics() {
        MetricsService.updateAdmission(
                activeQueries, queuedQueries, concurrencyLimit,
                throughputBytesPerSecond.get());
    }

    /**
     * Checks a valid sampled resource value against a high watermark.
     *
     * @param value sampled utilization
     * @param watermark high watermark
     * @return whether the resource is overloaded
     */
    private static boolean above(double value, double watermark) {
        return value >= 0.0 && value >= watermark;
    }

    /**
     * Checks a sampled resource value against a low watermark.
     *
     * @param value sampled utilization
     * @param watermark low watermark
     * @return whether the resource is underloaded or unavailable
     */
    private static boolean below(double value, double watermark) {
        return value < 0.0 || value <= watermark;
    }

    /**
     * Represents one admitted local execution.
     */
    public static final class Permit implements AutoCloseable {

        private final AdaptiveAdmissionController owner;
        private final long startedNanos;
        private boolean successful;
        private long logicalBytes;
        private boolean closed;

        private Permit(AdaptiveAdmissionController owner, long startedNanos) {
            this.owner = owner;
            this.startedNanos = startedNanos;
        }

        /**
         * Records successful completion for throughput estimation.
         *
         * @param bytes logical input bytes processed by the query
         */
        public void markSuccessful(long bytes) {
            successful = true;
            logicalBytes = Math.max(0L, bytes);
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                owner.release(this);
            }
        }
    }

    /**
     * Signals that a query cannot enter the bounded local admission queue.
     */
    public static final class AdmissionRejectedException extends RuntimeException {

        /**
         * Creates a rejection with a client-safe description.
         *
         * @param message rejection description
         */
        public AdmissionRejectedException(String message) {
            super(message);
        }
    }

    /**
     * Signals that a queued Flight client cancelled its request.
     */
    public static final class AdmissionCancelledException extends RuntimeException {

        /**
         * Creates a cancellation with a client-safe description.
         *
         * @param message cancellation description
         */
        public AdmissionCancelledException(String message) {
            super(message);
        }
    }

    /**
     * Signals that a queued request reached its cross-node redirect threshold.
     */
    public static final class AdmissionRedirectException extends RuntimeException {

        /**
         * Creates a redirect suggestion with a diagnostic description.
         *
         * @param message redirect description
         */
        public AdmissionRedirectException(String message) {
            super(message);
        }
    }
}
