package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * State associated with a query handle, including execution and load-accounting data.
 *
 * @param query SQL query text
 * @param filePaths paths to Parquet files
 * @param serverUri URI of the server holding the files
 * @param bytes total file size
 * @param reservationId distributed execution reservation, when admission controls the handle
 * @param loadTracked whether the bytes were added to distributed server load
 */
public record HandleState(String query, String[] filePaths, String serverUri,
        long bytes, String reservationId, boolean loadTracked) implements Serializable {

    /**
     * Creates state compatible with the original four-field representation.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     */
    public HandleState(String query, String[] filePaths, String serverUri, long bytes) {
        this(query, filePaths, serverUri, bytes, null,
                serverUri != null && bytes > 0);
    }

    /**
     * @param query SQL query text
     * @return handle state with no file paths or server URI
     */
    public static HandleState forQuery(String query) {
        return new HandleState(query, null, null, 0L, null, false);
    }

    /**
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @return handle state with all fields populated
     */
    public static HandleState forServerFiles(String query, String[] filePaths,
            String serverUri, long bytes) {
        return new HandleState(query, filePaths, serverUri, bytes, null, true);
    }

    /**
     * Creates endpoint state with explicit distributed load-accounting behavior.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @param loadTracked whether the bytes were added to distributed server load
     * @return handle state with all fields populated
     */
    public static HandleState forServerFiles(String query, String[] filePaths,
            String serverUri, long bytes, boolean loadTracked) {
        return new HandleState(query, filePaths, serverUri, bytes, null, loadTracked);
    }
}
