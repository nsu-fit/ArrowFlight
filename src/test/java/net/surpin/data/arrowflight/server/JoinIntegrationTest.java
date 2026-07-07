package net.surpin.data.arrowflight.server;

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
