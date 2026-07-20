package net.surpin.data.arrowflight.server.services;

import java.net.URI;
import java.util.List;

/**
 * Static helpers for URI/path resolution between Hadoop, DuckDB, and Acero engines.
 */
public final class PathUtils {

    private PathUtils() {
    }

    /**
     * Strips file: scheme from URIs for DuckDB consumption.
     *
     * @param uris fully-qualified URIs
     * @return DuckDB-compatible paths
     */
    public static List<String> ducksDbPaths(List<String> uris) {
        return uris.stream().map(u -> {
            if (u.startsWith("file:")) {
                int colon = u.indexOf(':');
                String stripped = u.substring(colon + 1);
                while (stripped.startsWith("/")) {
                    stripped = stripped.substring(1);
                }
                return "/" + stripped;
            }
            return u;
        }).toList();
    }

    /**
     * Converts Hadoop path to DuckDB-quoted path string.
     *
     * @param p Hadoop path
     * @return quoted DuckDB path
     */
    public static String duckDbPath(org.apache.hadoop.fs.Path p) {
        URI uri = p.toUri();
        String path = "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
        return "'" + path.replace("'", "''") + "'";
    }

    /**
     * Converts Hadoop path to unquoted DuckDB path string.
     *
     * @param p Hadoop path
     * @return unquoted DuckDB path
     */
    public static String plainDuckDbPath(org.apache.hadoop.fs.Path p) {
        URI uri = p.toUri();
        return "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
    }

    /**
     * Builds FROM clause for Arrow-exported streams with UNION ALL.
     *
     * @param count number of streams
     * @return FROM clause string
     */
    public static String arrowStreamsFromClause(int count) {
        if (count == 1) {
            return "\"t0\"";
        }
        StringBuilder result = new StringBuilder("(");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                result.append(" UNION ALL ");
            }
            result.append("SELECT * FROM \"t").append(i).append("\"");
        }
        return result.append(')').toString();
    }
}
