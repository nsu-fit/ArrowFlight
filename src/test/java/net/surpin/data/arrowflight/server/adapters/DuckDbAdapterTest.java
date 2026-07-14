package net.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.surpin.data.arrowflight.server.model.AppConfig;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbAdapterTest {

    @Test
    void sqlStringLiteralNormal() {
        assertEquals("'hello'", DuckDbAdapter.sqlStringLiteral("hello"));
    }

    @Test
    void sqlStringLiteralEscapesSingleQuote() {
        assertEquals("'it''s'", DuckDbAdapter.sqlStringLiteral("it's"));
    }

    @Test
    void sqlStringLiteralEmpty() {
        assertEquals("''", DuckDbAdapter.sqlStringLiteral(""));
    }

    @Test
    void quoteIdentifierNormal() {
        assertEquals("\"table_name\"", DuckDbAdapter.quoteIdentifier("table_name"));
    }

    @Test
    void quoteIdentifierEscapesDoubleQuote() {
        assertEquals("\"col\"\"name\"", DuckDbAdapter.quoteIdentifier("col\"name"));
    }

    @Test
    void readParquetFromClauseSingleFile() {
        String result = DuckDbAdapter.readParquetFromClause(List.of("/data/file.parquet"));
        assertEquals("read_parquet(['/data/file.parquet'])", result);
    }

    @Test
    void readParquetFromClauseMultipleFiles() {
        String result = DuckDbAdapter.readParquetFromClause(
                List.of("/data/a.parquet", "/data/b.parquet"));
        assertEquals("read_parquet(['/data/a.parquet', '/data/b.parquet'])", result);
    }

    @Test
    void readParquetFromClauseEmpty() {
        String result = DuckDbAdapter.readParquetFromClause(List.of());
        assertEquals("read_parquet([])", result);
    }

    @Test
    void ignoresHdfsOptionsWhenExtensionIsNotConfigured() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                4096, 1, 131072, 1, 1, 1,
                null, false, null, null,
                "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
                1048576, 60000L, "/data/parquet", 32010, 5701, 60,
                3, 1000, 30000);

        try {
            DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool);
            try (Connection connection = adapter.connection()) {
                assertFalse(connection.isClosed());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }
}
