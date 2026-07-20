package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Executes JOIN queries by registering temp views in DuckDB and streaming the result.
 */
public final class JoinService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JoinService.class);

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final AceroFileResolver aceroFileResolver;

    /**
     * Creates JoinService.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param duckDbAdapter  DuckDB adapter
     * @param appConfig      server configuration
     */
    public JoinService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            AppConfig appConfig) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.aceroFileResolver = new AceroFileResolver(
                parquetAdapter.fileSystem(), new Path(parquetAdapter.dataDirectory()),
                appConfig.localDataDir(), appConfig.ioFileBufferSize());
    }

    /**
     * Executes JOIN queries by registering temp views in DuckDB and streaming the result.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query with join tables
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void executeJoin(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
        try {
            Map<String, List<String>> tableFiles = new LinkedHashMap<>();
            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                tableFiles.computeIfAbsent(key, k -> {
                    try {
                        return resolveTableFiles(k);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                List<String> duckDbPaths = tableFiles.get(key);
                if (duckDbPaths.isEmpty()) {
                    throw new IOException("No Parquet files found for table: " + key);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE OR REPLACE TEMP VIEW "
                            + DuckDbAdapter.quoteIdentifier(jt.alias())
                            + " AS SELECT * FROM "
                            + DuckDbAdapter.readParquetFromClause(duckDbPaths));
                }
                registeredAliases.add(jt.alias());
            }

            duckDbAdapter.streamSql(allocator, pq.duckDbSql, listener, startListener);
        } finally {
            try (Statement stmt = conn.createStatement()) {
                for (String alias : registeredAliases) {
                    try {
                        stmt.execute("DROP VIEW IF EXISTS "
                                + DuckDbAdapter.quoteIdentifier(alias));
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private boolean isHdfsData() {
        return "hdfs".equalsIgnoreCase(parquetAdapter.fileSystem().getUri().getScheme());
    }

    private List<String> resolveTableFiles(String key) throws IOException {
        int dot = key.indexOf('.');
        String schema = dot > 0 ? key.substring(0, dot) : null;
        String table = dot > 0 ? key.substring(dot + 1) : key;
        Path dir = schema != null
                ? new Path(parquetAdapter.dataDirectory(), schema + "/" + table)
                : new Path(parquetAdapter.dataDirectory(), table);
        List<String> uris = new ArrayList<>();
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
                parquetAdapter.fileSystem().listFiles(dir, true);
        while (it.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus f = it.next();
            if (f.isFile() && f.getPath().getName().endsWith(".parquet")) {
                uris.add(isHdfsData()
                        ? PathUtils.ducksDbPaths(List.of(aceroFileResolver.resolve(f))).get(0)
                        : PathUtils.plainDuckDbPath(f.getPath()));
            }
        }
        return uris;
    }
}
