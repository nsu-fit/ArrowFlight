package net.surpin.data.arrowflight.server.adapters;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;

/**
 * Loads configuration from arrowflight.properties, system properties, and environment variables.
 * Produces an immutable AppConfig record.
 */
public class ConfigAdapter {

    private static final String CONFIG_RESOURCE = "arrowflight.properties";

    /**
     * Utility class, no instantiation.
     */
    private ConfigAdapter() {
    }

    /**
     * Loads and returns the complete server configuration.
     *
     * @return resolved AppConfig
     */
    public static AppConfig getConfig() {
        Properties props = loadProperties();
        int ioParallelism = computeIoParallelism(props);
        int numServers = getInt("numServers", "arrowflight.cluster.numServers", 3, props);
        int batchSize = getInt("batchSize", "arrowflight.duckdb.batchSize", 65536, props);
        int ioFileBufferSize = getInt("ioFileBufferSize", null, 131072, props);
        int duckDbWarmConnections = getInt("duckDbWarmConnections",
                "arrowflight.duckdb.warmConnections", Math.min(8, ioParallelism), props);
        int duckDbGroups = getInt("duckDbGroups",
                "arrowflight.duckdb.groups", Math.min(8, ioParallelism), props);
        int duckDbThreads = getInt("duckDbThreads", "arrowflight.duckdb.threads", 1, props);
        String duckDbHdfsExtension = getStringWithEnv("duckDbHdfsExtension",
                "arrowflight.duckdb.hdfs.extension", "DUCKDB_HDFS_EXTENSION", null, props);
        boolean duckDbAllowUnsignedExtensions = getBooleanWithEnv("duckDbAllowUnsignedExtensions",
                "arrowflight.duckdb.allowUnsignedExtensions", "DUCKDB_ALLOW_UNSIGNED_EXTENSIONS",
                false, props);
        String duckDbHdfsDefaultNamenode = getStringWithEnv("duckDbHdfsDefaultNamenode",
                "arrowflight.duckdb.hdfs.defaultNamenode", "HDFS_DEFAULT_NAMENODE", null, props);
        String duckDbHdfsHaNamenodes = getStringWithEnv("duckDbHdfsHaNamenodes",
                "arrowflight.duckdb.hdfs.haNamenodes", "HDFS_HA_NAMENODES", null, props);
        String duckDbHdfsShortcircuit = getStringWithEnv("duckDbHdfsShortcircuit",
                "arrowflight.duckdb.hdfs.shortcircuit", "HDFS_SHORTCIRCUIT", null, props);
        String duckDbHdfsDomainSocketPath = getStringWithEnv("duckDbHdfsDomainSocketPath",
                "arrowflight.duckdb.hdfs.domainSocketPath", "HDFS_DOMAIN_SOCKET_PATH", null, props);
        boolean metricsEnabled = getBooleanWithEnv("metricsEnabled",
                "arrowflight.metrics.enabled", "FLIGHT_METRICS_ENABLED", false, props);
        int grpcMaxInboundMessageSize = getInt("grpcMaxInboundMessageSize",
                "arrowflight.grpc.maxInboundMessageSize", Integer.MAX_VALUE, props);
        int flightBackpressureThresholdBytes = getInt("flightBackpressureThresholdBytes",
                "arrowflight.flight.backpressureThresholdBytes", 67_108_864, props);
        if (flightBackpressureThresholdBytes <= 0) {
            throw new IllegalArgumentException(
                    "flightBackpressureThresholdBytes must be positive: "
                            + flightBackpressureThresholdBytes);
        }
        long flightListenerReadyTimeoutMillis = getLong("flightListenerReadyTimeoutMs",
                "arrowflight.flight.listenerReadyTimeoutMs", 300_000L, props);
        if (flightListenerReadyTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "flightListenerReadyTimeoutMs must be positive: "
                            + flightListenerReadyTimeoutMillis);
        }
        String dataDir = getString("dataDir", null, "/data/parquet", props);
        String localDataDir = getStringWithEnv("localDataDir", "arrowflight.localDataDir",
                "FLIGHT_LOCAL_DATA_DIR", null, props);
        int port = getInt("port", null, 32010, props);
        int hazelcastPort = getInt("hazelcastPort", null, 5701, props);
        int hazelcastClusterJoinTimeoutSec = getInt("hazelcastClusterJoinTimeoutSec",
                "arrowflight.hazelcast.clusterJoinTimeoutSec", 60, props);
        int clientMaxRetries = getInt("client.maxRetries", null, 3, props);
        int clientRetryBackoffMs = getInt("client.retryBackoffMs", null, 1000, props);
        int clientConnectTimeoutMs = getInt("client.connectTimeoutMs", null, 0, props);
        SchedulerConfig schedulerConfig = schedulerConfig(props, duckDbThreads);

        return new AppConfig(
                numServers, batchSize, ioParallelism, ioFileBufferSize,
                duckDbWarmConnections, duckDbGroups, duckDbThreads,
                duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes,
                duckDbHdfsShortcircuit, duckDbHdfsDomainSocketPath,
                metricsEnabled,
                grpcMaxInboundMessageSize, flightBackpressureThresholdBytes,
                flightListenerReadyTimeoutMillis,
                dataDir, localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                schedulerConfig);
    }

    /**
     * Loads adaptive scheduling configuration and resolves automatic concurrency.
     *
     * @param props loaded properties
     * @param duckDbThreads DuckDB threads consumed by one query
     * @return validated scheduler configuration
     */
    private static SchedulerConfig schedulerConfig(
            Properties props, int duckDbThreads) {
        boolean enabled = getBoolean("adaptiveSchedulingEnabled",
                "arrowflight.scheduler.enabled", true, props);
        long snapshotIntervalMillis = getLong("schedulerSnapshotIntervalMs",
                "arrowflight.scheduler.snapshotIntervalMs", 1_000L, props);
        long snapshotStaleMillis = getLong("schedulerSnapshotStaleMs",
                "arrowflight.scheduler.snapshotStaleMs", 5_000L, props);
        long controlIntervalMillis = getLong("schedulerControlIntervalMs",
                "arrowflight.scheduler.controlIntervalMs", 5_000L, props);
        int minConcurrentQueries = getInt("admissionMinConcurrentQueries",
                "arrowflight.admission.minConcurrentQueries", 1, props);
        int configuredMaximum = getInt("admissionMaxConcurrentQueries",
                "arrowflight.admission.maxConcurrentQueries", 0, props);
        int automaticMaximum = Math.max(1, Math.min(8,
                Runtime.getRuntime().availableProcessors()
                        / Math.max(1, duckDbThreads)));
        int maxConcurrentQueries = configuredMaximum > 0
                ? configuredMaximum : automaticMaximum;
        int maxQueuedQueries = getInt("admissionMaxQueuedQueries",
                "arrowflight.admission.maxQueuedQueries", 64, props);
        long maxQueueWaitMillis = getLong("admissionMaxQueueWaitMs",
                "arrowflight.admission.maxQueueWaitMs", 30_000L, props);
        double cpuLowWatermark = getDouble("schedulerCpuLowWatermark",
                "arrowflight.scheduler.cpuLowWatermark", 0.65, props);
        double cpuHighWatermark = getDouble("schedulerCpuHighWatermark",
                "arrowflight.scheduler.cpuHighWatermark", 0.90, props);
        double memoryLowWatermark = getDouble("schedulerMemoryLowWatermark",
                "arrowflight.scheduler.memoryLowWatermark", 0.70, props);
        double memoryHighWatermark = getDouble("schedulerMemoryHighWatermark",
                "arrowflight.scheduler.memoryHighWatermark", 0.85, props);
        long remotePenaltyMillis = getLong("schedulerRemoteLocalityPenaltyMs",
                "arrowflight.scheduler.remoteLocalityPenaltyMs", 250L, props);
        boolean redirectEnabled = getBoolean("admissionRedirectEnabled",
                "arrowflight.admission.redirectEnabled", true, props);
        long redirectAfterMillis = getLong("admissionRedirectAfterMs",
                "arrowflight.admission.redirectAfterMs", 500L, props);
        int maxRedirects = getInt("admissionMaxRedirects",
                "arrowflight.admission.maxRedirects", 2, props);
        double redirectMinScoreImprovement = getDouble(
                "admissionRedirectMinScoreImprovement",
                "arrowflight.admission.redirectMinScoreImprovement",
                0.30, props);

        if (snapshotIntervalMillis <= 0L
                || snapshotStaleMillis < snapshotIntervalMillis
                || controlIntervalMillis <= 0L
                || minConcurrentQueries <= 0
                || maxConcurrentQueries < minConcurrentQueries
                || maxQueuedQueries < 0
                || maxQueueWaitMillis <= 0L
                || remotePenaltyMillis < 0L
                || redirectAfterMillis < 0L
                || maxRedirects < 0
                || redirectEnabled && maxRedirects == 0
                || redirectMinScoreImprovement < 0.0
                || redirectMinScoreImprovement >= 1.0) {
            throw new IllegalArgumentException("Invalid adaptive scheduler limits");
        }
        validateWatermarks(
                cpuLowWatermark, cpuHighWatermark, "CPU");
        validateWatermarks(
                memoryLowWatermark, memoryHighWatermark, "memory");
        return new SchedulerConfig(
                enabled,
                snapshotIntervalMillis,
                snapshotStaleMillis,
                controlIntervalMillis,
                minConcurrentQueries,
                maxConcurrentQueries,
                maxQueuedQueries,
                maxQueueWaitMillis,
                cpuLowWatermark,
                cpuHighWatermark,
                memoryLowWatermark,
                memoryHighWatermark,
                remotePenaltyMillis,
                redirectEnabled,
                redirectAfterMillis,
                maxRedirects,
                redirectMinScoreImprovement);
    }

    /**
     * Computes I/O thread pool parallelism from config, cores, and constraints.
     *
     * @param props loaded properties
     * @return thread count (clamped to 64)
     */
    private static int computeIoParallelism(Properties props) {
        Integer explicit = getOptionalInt("ioParallelism", "arrowflight.io.parallelism", props);
        if (explicit != null && explicit > 0) {
            return Math.min(explicit, 64);
        }
        int availableCores = Runtime.getRuntime().availableProcessors();
        int maxCores = getInt("ioParallelismMaxCores", "arrowflight.io.maxCores", 8, props);
        int effectiveCores = maxCores > 0 ? Math.min(availableCores, maxCores) : availableCores;
        int multiplier = getInt("ioParallelismMultiplier", "arrowflight.io.parallelismMultiplier", 8, props);
        int minThreads = getInt("ioParallelismMinThreads", "arrowflight.io.minParallelism", 32, props);
        return Math.min(64, Math.max(minThreads, effectiveCores * multiplier));
    }

    /**
     * Reads a string config from system property or properties file.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static String getString(String key, String sysAlias, String fallback, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = System.getProperty(sysAlias);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = props.getProperty(sysAlias);
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /**
     * Reads a string config with environment variable fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static String getStringWithEnv(String key, String sysAlias, String envName,
            String fallback, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = System.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            value = props.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            String env = System.getenv(envName);
            if (env != null && !env.isBlank()) {
                value = env;
            }
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /**
     * Reads an integer config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static int getInt(String key, String sysAlias, int fallback, Properties props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + raw, e);
        }
    }

    /**
     * Reads an optional integer config, returns null if not set.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param props    loaded properties
     * @return integer value or null
     */
    private static Integer getOptionalInt(String key, String sysAlias, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = System.getProperty(sysAlias);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = props.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + value, e);
        }
    }

    /**
     * Reads a boolean config with environment variable fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static boolean getBooleanWithEnv(String key, String sysAlias, String envName,
            boolean fallback, Properties props) {
        String value = getStringWithEnv(key, sysAlias, envName, null, props);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    /**
     * Reads a boolean configuration value.
     *
     * @param key primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props loaded properties
     * @return resolved boolean value
     */
    private static boolean getBoolean(
            String key, String sysAlias, boolean fallback, Properties props) {
        String value = getString(key, sysAlias, null, props);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    /**
     * Reads a long config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static long getLong(String key, String sysAlias, long fallback, Properties props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + key + ": " + raw, e);
        }
    }

    /**
     * Reads a double configuration value with fallback.
     *
     * @param key primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props loaded properties
     * @return resolved double value
     */
    private static double getDouble(
            String key, String sysAlias, double fallback, Properties props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid double for " + key + ": " + raw, e);
        }
    }

    /**
     * Validates an adaptive controller watermark pair.
     *
     * @param low low utilization threshold
     * @param high high utilization threshold
     * @param name resource name
     */
    private static void validateWatermarks(double low, double high, String name) {
        if (low < 0.0 || high > 1.0 || low >= high) {
            throw new IllegalArgumentException(
                    "Invalid " + name + " scheduler watermarks");
        }
    }

    /**
     * Loads properties from classpath resource.
     *
     * @return loaded Properties
     */
    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = ConfigAdapter.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        return properties;
    }
}
