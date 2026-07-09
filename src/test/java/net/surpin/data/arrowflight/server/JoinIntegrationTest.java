package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.server.db.ParquetManager;
import net.surpin.data.arrowflight.server.db.ParquetQueryParser;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class JoinIntegrationTest {

    private static ParquetManager manager;
    private static String fullTable;

    @BeforeAll
    static void setUp() throws Exception {
        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        manager = new ParquetManager(localFs, dataDir, "localhost");
        fullTable = "test_schema.test_table";
    }

    @Test
    void selfJoinDifferentAliases() throws Exception {
        String query = "SELECT a.id, b.id "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");
        assertEquals(2, pq.joinTables.size(), "Should have 2 table references");
        assertNotNull(pq.duckDbSql, "Should produce DuckDB SQL");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            assertTrue(files.length > 0, "Should find files for join query");
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertTrue(listener.totalRows > 0, "Self-join should return rows");
    }

    @Test
    void innerJoinOnMatchingColumn() throws Exception {
        String query = "SELECT a.id, a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.tinyint_col";

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertTrue(listener.totalRows > 0, "Inner join should return matching rows");
    }

    @Test
    void joinSchemaExtraction() {
        String query = "SELECT a.id, a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.tinyint_col";

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(3, schema.getFields().size(), "Should have 3 output columns");
    }

    @Test
    void joinQueryParserExtractsAliases() {
        String query = "SELECT t1.id, t2.bool_col "
                + "FROM " + fullTable + " t1 "
                + "JOIN " + fullTable + " t2 "
                + "ON t1.id = t2.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertEquals("t1", pq.joinTables.get(0).alias());
        assertEquals("t2", pq.joinTables.get(1).alias());
    }

    @Test
    void joinQueryParserProducesDuckDbSql() {
        String query = "SELECT a.id, b.id "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        String duckSql = pq.duckDbSql;
        assertNotNull(duckSql);
        assertTrue(duckSql.contains("JOIN b"), "DuckDB SQL should use alias 'b': " + duckSql);
        assertFalse(duckSql.contains("test_schema.test_table"),
                "DuckDB SQL should not contain schema-qualified names: " + duckSql);
    }

    // ── cross-type join: INT32 = INT16 ──────────────────────────────────────

    @Test
    void innerJoinIntToSmallint() throws Exception {
        String query = "SELECT a.id, b.smallint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.smallint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertEquals(1000, listener.totalRows,
                "Join id(INT32) = smallint_col(INT16) must return 1000 pairs");
    }

    // ── cross-type join: INT32 = INT64 ──────────────────────────────────────

    @Test
    void innerJoinIntToBigint() throws Exception {
        String query = "SELECT a.id, b.bigint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.bigint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertEquals(1000, listener.totalRows,
                "Join id(INT32) = bigint_col(INT64) must return 1000 pairs");
    }

    // ── cross-type join: INT32 = FLOAT ─────────────────────────────────────

    @Test
    void innerJoinIntToFloat() throws Exception {
        String query = "SELECT a.id, b.float_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.float_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertTrue(listener.totalRows >= 0,
                "Join id(INT32) = float_col(FLOAT) must not throw ClassCastException");
    }

    // ── cross-type join: INT32 = DOUBLE ────────────────────────────────────

    @Test
    void innerJoinIntToDouble() throws Exception {
        String query = "SELECT a.id, b.double_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.double_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertTrue(listener.totalRows >= 0,
                "Join id(INT32) = double_col(DOUBLE) must not throw ClassCastException");
    }

    // ── cross-type join: FLOAT = DOUBLE ────────────────────────────────────

    @Test
    void innerJoinFloatToDouble() throws Exception {
        String query = "SELECT a.float_col, b.double_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.float_col = b.double_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertTrue(listener.totalRows >= 0,
                "Join float_col(FLOAT) = double_col(DOUBLE) must not throw ClassCastException");
    }

    // ── cross-type join: BOOL = INT8 ───────────────────────────────────────

    @Test
    void innerJoinBoolToTinyint() throws Exception {
        String query = "SELECT a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.bool_col = b.tinyint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertEquals(100000, listener.totalRows,
                "Join bool_col(BOOL) = tinyint_col(INT8) must return 100000 pairs without ClassCastException");
    }

    // ── cross-type join: STRING = STRING ───────────────────────────────────

    @Test
    void innerJoinStringColumns() throws Exception {
        String query = "SELECT a.date_string_col, b.string_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.date_string_col = b.string_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = manager.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String[] files = manager.locationsForQuery(query).keySet().toArray(new String[0]);
            manager.readParquet(allocator, query, files, listener, true);
        }
        assertEquals(0, listener.totalRows,
                "Join date_string_col(STRING) = string_col(STRING) must return 0 rows without ClassCastException");
    }

    static class CountingListener implements ServerStreamListener {
        int totalRows;
        VectorSchemaRoot root;

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {
            this.root = root;
        }

        @Override public void putNext() { totalRows += root.getRowCount(); }
        @Override public void putNext(ArrowBuf metadata) { totalRows += root.getRowCount(); }
        @Override public void putMetadata(ArrowBuf metadata) {}
        @Override public boolean isReady() { return true; }
        @Override public boolean isCancelled() { return false; }
        @Override public void setOnReadyHandler(Runnable handler) {}
        @Override public void setOnCancelHandler(Runnable handler) {}
        @Override public void error(Throwable ex) { throw new RuntimeException(ex); }
        @Override public void completed() {}
    }
}
