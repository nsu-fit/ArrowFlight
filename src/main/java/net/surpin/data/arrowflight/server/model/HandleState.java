package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * State associated with a query handle, including query text, file paths, server URI, and byte count.
 */
public record HandleState(String query, String[] filePaths, String serverUri, long bytes) implements Serializable {

    /**
     * @param query SQL query text
     * @return handle state with no file paths or server URI
     */
    public static HandleState forQuery(String query) {
        return new HandleState(query, null, null, 0L);
    }

    /**
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @return handle state with all fields populated
     */
    public static HandleState forServerFiles(String query, String[] filePaths, String serverUri, long bytes) {
        return new HandleState(query, filePaths, serverUri, bytes);
    }
}
