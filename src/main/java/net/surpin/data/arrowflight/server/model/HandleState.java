package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

public record HandleState(String query, String[] filePaths, String serverUri, long bytes) implements Serializable {

    public static HandleState forQuery(String query) {
        return new HandleState(query, null, null, 0L);
    }

    public static HandleState forServerFiles(String query, String[] filePaths, String serverUri, long bytes) {
        return new HandleState(query, filePaths, serverUri, bytes);
    }
}
