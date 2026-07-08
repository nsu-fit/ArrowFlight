package net.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class RuntimeSettings {
    private static final String CONFIG_RESOURCE = "arrowflight.properties";
    private static final Properties PROPERTIES = loadProperties();
    private static final int DEFAULT_GRPC_MAX_INBOUND_MESSAGE_SIZE = Integer.MAX_VALUE;

    private RuntimeSettings() {
    }

    public static int batchSize() {
        return getInt("batchSize", "arrowflight.duckdb.batchSize", 4096);
    }

    public static int duckDbBatchSize() {
        return batchSize();
    }

    public static int ioParallelism() {
        Integer explicit = getOptionalInt("ioParallelism", "arrowflight.io.parallelism");
        if (explicit != null && explicit > 0) {
            return explicit;
        }

        int availableCores = Runtime.getRuntime().availableProcessors();
        int maxCores = getInt("ioParallelismMaxCores", "arrowflight.io.maxCores", 0);
        int effectiveCores = maxCores > 0 ? Math.min(availableCores, maxCores) : availableCores;
        int multiplier = getInt("ioParallelismMultiplier", "arrowflight.io.parallelismMultiplier", 8);
        int minThreads = getInt("ioParallelismMinThreads", "arrowflight.io.minParallelism", 32);

        return Math.max(minThreads, effectiveCores * multiplier);
    }

    public static int duckDbWarmConnections(int ioParallelism) {
        return getInt("duckDbWarmConnections",
                "arrowflight.duckdb.warmConnections",
                Math.min(8, ioParallelism));
    }

    public static int duckDbGroups(int ioParallelism) {
        return getInt("duckDbGroups",
                "arrowflight.duckdb.groups",
                Math.min(8, ioParallelism));
    }

    public static int duckDbThreads() {
        return getInt("duckDbThreads", "arrowflight.duckdb.threads", 1);
    }

    public static String duckDbHdfsExtension() {
        return getString("duckDbHdfsExtension",
                "arrowflight.duckdb.hdfs.extension",
                "DUCKDB_HDFS_EXTENSION",
                null);
    }

    public static boolean duckDbAllowUnsignedExtensions(boolean fallback) {
        return getBoolean("duckDbAllowUnsignedExtensions",
                "arrowflight.duckdb.allowUnsignedExtensions",
                "DUCKDB_ALLOW_UNSIGNED_EXTENSIONS",
                fallback);
    }

    public static String duckDbHdfsDefaultNamenode() {
        return getString("duckDbHdfsDefaultNamenode",
                "arrowflight.duckdb.hdfs.defaultNamenode",
                "HDFS_DEFAULT_NAMENODE",
                null);
    }

    public static String duckDbHdfsHaNamenodes() {
        return getString("duckDbHdfsHaNamenodes",
                "arrowflight.duckdb.hdfs.haNamenodes",
                "HDFS_HA_NAMENODES",
                null);
    }

    public static String duckDbHdfsShortcircuit() {
        return getString("duckDbHdfsShortcircuit",
                "arrowflight.duckdb.hdfs.shortcircuit",
                "HDFS_SHORTCIRCUIT",
                null);
    }

    public static String duckDbHdfsDomainSocketPath() {
        return getString("duckDbHdfsDomainSocketPath",
                "arrowflight.duckdb.hdfs.domainSocketPath",
                "HDFS_DOMAIN_SOCKET_PATH",
                null);
    }

    public static int grpcMaxInboundMessageSize() {
        return getInt("grpcMaxInboundMessageSize",
                "arrowflight.grpc.maxInboundMessageSize",
                DEFAULT_GRPC_MAX_INBOUND_MESSAGE_SIZE);
    }

    public static long flightListenerReadyTimeoutMillis() {
        return getLong("flightListenerReadyTimeoutMs",
                "arrowflight.flight.listenerReadyTimeoutMs",
                60_000L);
    }

    public static String defaultDataDir() {
        return getString("dataDir", "/data/parquet");
    }

    public static int defaultPort() {
        return getInt("port", 32010);
    }

    public static int defaultHazelcastPort() {
        return getInt("hazelcastPort", 5701);
    }

    public static int ioFileBufferSize() {
        return getInt("ioFileBufferSize", 131072);
    }

    private static String getString(String key, String fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(key);
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static String getString(String key, String systemPropertyAlias, String envName, String fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getProperty(systemPropertyAlias);
        }
        if (value == null || value.isBlank()) {
            value = System.getenv(envName);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(systemPropertyAlias);
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int getInt(String key, int fallback) {
        return parseInt(key, getString(key, String.valueOf(fallback)));
    }

    private static int getInt(String key, String systemPropertyAlias, int fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getProperty(systemPropertyAlias);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(systemPropertyAlias);
        }
        return parseInt(key, value == null || value.isBlank() ? String.valueOf(fallback) : value.trim());
    }

    private static Integer getOptionalInt(String key, String systemPropertyAlias) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getProperty(systemPropertyAlias);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(systemPropertyAlias);
        }
        return value == null || value.isBlank() ? null : parseInt(key, value.trim());
    }

    private static boolean getBoolean(String key, String systemPropertyAlias, String envName,
            boolean fallback) {
        String value = getString(key, systemPropertyAlias, envName, null);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    private static long getLong(String key, String systemPropertyAlias, long fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getProperty(systemPropertyAlias);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = PROPERTIES.getProperty(systemPropertyAlias);
        }
        return parseLong(key, value == null || value.isBlank() ? String.valueOf(fallback) : value.trim());
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for " + key + ": " + value, e);
        }
    }

    private static long parseLong(String key, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for " + key + ": " + value, e);
        }
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = RuntimeSettings.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        return properties;
    }
}
