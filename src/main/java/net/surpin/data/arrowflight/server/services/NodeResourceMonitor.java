package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.model.NodeLoadSnapshot;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Samples local process resources and publishes scheduling snapshots through Hazelcast.
 */
public final class NodeResourceMonitor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeResourceMonitor.class);

    private final ClusterService clusterService;
    private final AdaptiveAdmissionController admissionController;
    private final BufferAllocator allocator;
    private final SchedulerConfig config;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * Creates a node resource monitor.
     *
     * @param clusterService cluster state service
     * @param admissionController local query admission controller
     * @param allocator server Arrow allocator
     * @param config scheduler configuration
     */
    public NodeResourceMonitor(
            ClusterService clusterService,
            AdaptiveAdmissionController admissionController,
            BufferAllocator allocator,
            SchedulerConfig config) {
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService");
        this.admissionController = Objects.requireNonNull(
                admissionController, "admissionController");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "flight-resource-monitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Publishes an initial snapshot and starts periodic resource sampling.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        publish();
        executor.scheduleAtFixedRate(
                this::publishSafely,
                config.snapshotIntervalMillis(),
                config.snapshotIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Collects and publishes one node snapshot immediately.
     */
    public void publish() {
        double cpuLoad = processCpuLoad();
        double systemCpuLoad = systemCpuLoad();
        double memoryPressure = memoryPressure();
        admissionController.updatePressure(
                Math.max(cpuLoad, systemCpuLoad), memoryPressure);
        NodeLoadSnapshot snapshot = new NodeLoadSnapshot(
                System.currentTimeMillis(),
                admissionController.concurrencyLimit(),
                admissionController.activeQueries(),
                admissionController.queuedQueries(),
                cpuLoad,
                systemCpuLoad,
                memoryPressure,
                admissionController.throughputBytesPerSecond(),
                admissionController.acceptingRequests()
                        && memoryPressure < 0.98);
        clusterService.publishNodeSnapshot(snapshot);
        MetricsService.updateResourcePressure(
                cpuLoad, systemCpuLoad, memoryPressure);
    }

    @Override
    public void close() {
        executor.shutdownNow();
        clusterService.removeNodeSnapshot();
    }

    /**
     * Publishes one snapshot without terminating periodic scheduling on failure.
     */
    private void publishSafely() {
        try {
            publish();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish Flight node load snapshot: {}", e.getMessage());
        }
    }

    /**
     * Returns the highest observable managed, Arrow, or container memory pressure.
     *
     * @return memory utilization in the range zero to one
     */
    private double memoryPressure() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        double heapPressure = ratio(heap.getUsed(), heap.getMax());
        double arrowPressure = ratio(
                allocator.getAllocatedMemory(), allocator.getLimit());
        return Math.max(
                Math.max(heapPressure, arrowPressure),
                Math.max(
                        physicalMemoryPressure(),
                        cgroupMemoryPressure()));
    }

    /**
     * Returns recent process CPU utilization.
     *
     * @return CPU utilization in the range zero to one, or negative when unavailable
     */
    private static double processCpuLoad() {
        java.lang.management.OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean extended) {
            return clamp(extended.getProcessCpuLoad());
        }
        double loadAverage = bean.getSystemLoadAverage();
        if (loadAverage < 0.0) {
            return -1.0;
        }
        return clamp(loadAverage / Math.max(1, bean.getAvailableProcessors()));
    }

    /**
     * Returns total CPU utilization so unrelated work on the node affects admission.
     *
     * @return system CPU utilization in the range zero to one, or negative when unavailable
     */
    private static double systemCpuLoad() {
        java.lang.management.OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean extended) {
            return clamp(extended.getCpuLoad());
        }
        double loadAverage = bean.getSystemLoadAverage();
        if (loadAverage < 0.0) {
            return -1.0;
        }
        return clamp(loadAverage / Math.max(1, bean.getAvailableProcessors()));
    }

    /**
     * Returns total physical memory pressure so unrelated processes affect admission.
     *
     * @return physical memory utilization, or zero when unavailable
     */
    private static double physicalMemoryPressure() {
        java.lang.management.OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean extended) {
            long total = extended.getTotalMemorySize();
            long free = extended.getFreeMemorySize();
            return ratio(Math.max(0L, total - free), total);
        }
        return 0.0;
    }

    /**
     * Reads Linux cgroup v1 or v2 memory utilization when available.
     *
     * @return container memory utilization, or zero outside a bounded cgroup
     */
    private static double cgroupMemoryPressure() {
        double v2 = cgroupRatio(
                Path.of("/sys/fs/cgroup/memory.current"),
                Path.of("/sys/fs/cgroup/memory.max"));
        if (v2 > 0.0) {
            return v2;
        }
        return cgroupRatio(
                Path.of("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
                Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    }

    /**
     * Reads a cgroup usage and limit pair.
     *
     * @param usagePath cgroup usage file
     * @param limitPath cgroup limit file
     * @return bounded utilization, or zero when unavailable
     */
    private static double cgroupRatio(Path usagePath, Path limitPath) {
        try {
            if (!Files.isRegularFile(usagePath) || !Files.isRegularFile(limitPath)) {
                return 0.0;
            }
            String maximum = Files.readString(limitPath).trim();
            if ("max".equals(maximum)) {
                return 0.0;
            }
            return ratio(
                    Long.parseLong(Files.readString(usagePath).trim()),
                    Long.parseLong(maximum));
        } catch (IOException | NumberFormatException | SecurityException e) {
            return 0.0;
        }
    }

    /**
     * Computes a bounded utilization ratio.
     *
     * @param used used bytes
     * @param maximum maximum bytes
     * @return utilization in the range zero to one
     */
    private static double ratio(long used, long maximum) {
        if (used < 0L || maximum <= 0L || maximum == Long.MAX_VALUE) {
            return 0.0;
        }
        return clamp((double) used / maximum);
    }

    /**
     * Clamps an observed utilization value.
     *
     * @param value sampled utilization
     * @return bounded utilization or negative one when invalid
     */
    private static double clamp(double value) {
        if (value < 0.0 || Double.isNaN(value)) {
            return -1.0;
        }
        return Math.min(1.0, value);
    }
}
