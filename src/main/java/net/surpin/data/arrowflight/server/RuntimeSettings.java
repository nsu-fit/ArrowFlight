package net.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class RuntimeSettings {
    private static final String CONFIG_RESOURCE = "arrowflight.properties";
    private static final Properties PROPERTIES = loadProperties();

    private RuntimeSettings() {
    }

    public static int batchSize() {
        return getInt("batchSize", "arrowflight.duckdb.batchSize", 4096);
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

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for " + key + ": " + value, e);
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
