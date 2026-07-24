package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ParquetManagerIntegrationTest {

    private static ParquetAdapter parquetAdapter;
    private static MetadataService metadataService;
    private static ExecutionService executionService;
    private static BufferAllocator allocator;

    @BeforeAll
    static void setUp() throws Exception {
        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        System.setProperty("dataDir", dataDir);
        System.setProperty("duckDbWarmConnections", "1");
        System.setProperty("duckDbGroups", "1");
        System.setProperty("duckDbThreads", "1");
        System.setProperty("ioParallelism", "2");
        AppConfig appConfig = ConfigAdapter.getConfig();

        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(new java.net.URI("file:///"), new Configuration());

        allocator = new RootAllocator(Long.MAX_VALUE);
        parquetAdapter = new ParquetAdapter(appConfig, localFs, "localhost");
        metadataService = new MetadataService(parquetAdapter);
        DuckDbAdapter duckDbAdapter = new DuckDbAdapter(appConfig,
                Executors.newCachedThreadPool());

        executionService = new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig,
                Executors.newCachedThreadPool());
    }

    @Test
    void getTableSchemaReturnsNonEmptySchema() {
        Schema schema = parquetAdapter.getTableSchema("test_schema", "test_table");
        assertNotNull(schema);
        assertFalse(schema.getFields().isEmpty());
    }

    @Test
    void getTableSchemaColumnNamesArePresent() {
        Schema schema = parquetAdapter.getTableSchema("test_schema", "test_table");
        List<String> names = schema.getFields().stream().map(f -> f.getName()).toList();
        assertTrue(names.contains("id"));
        assertTrue(names.contains("bool_col"));
        assertTrue(names.contains("tinyint_col"));
    }

    @Test
    void getTableSchemaWithColumnFilterReturnsSubset() {
        List<String> requested = List.of("id", "bool_col");
        Schema schema = parquetAdapter.getTableSchema("test_schema", "test_table", requested);
        assertEquals(2, schema.getFields().size());
        List<String> names = schema.getFields().stream().map(f -> f.getName()).toList();
        assertTrue(names.contains("id") && names.contains("bool_col"));
    }

    @Test
    void getTableSchemaEmptyColumnListReturnsAll() {
        Schema full = parquetAdapter.getTableSchema("test_schema", "test_table");
        Schema withEmpty = parquetAdapter.getTableSchema(
                "test_schema", "test_table", List.of());
        assertEquals(full.getFields().size(), withEmpty.getFields().size());
    }

    @Test
    void getQuerySchemaSelectStarReturnsAllColumns() {
        Schema full = parquetAdapter.getTableSchema("test_schema", "test_table");
        Schema fromQuery = metadataService.getQuerySchema(
                "SELECT * FROM test_schema.test_table");
        assertEquals(full.getFields().size(), fromQuery.getFields().size());
    }

    @Test
    void getQuerySchemaWithProjection() {
        Schema schema = metadataService.getQuerySchema(
                "SELECT id, bool_col FROM test_schema.test_table");
        assertEquals(2, schema.getFields().size());
    }

    @Test
    void locationsForQueryReturnsRelativePaths() throws Exception {
        Map<String, FileAssignment> locations = parquetAdapter.locationsForQuery(
                "SELECT * FROM test_schema.test_table");
        assertFalse(locations.isEmpty());
        for (String path : locations.keySet()) {
            assertFalse(path.startsWith("/"), "Paths should be relative: " + path);
        }
    }

    @Test
    void getSchemasReturnsTestSchema() throws Exception {
        Map<String, ?> schemas = parquetAdapter.getSchemas(null);
        assertTrue(schemas.containsKey("test_schema"));
    }

    @Test
    void initCatalogReaderPopulatesDdlCache() {
        parquetAdapter.initCatalogReader();
        Map<String, Map<String, String>> ddlCache = parquetAdapter.tableDdlCache();
        assertFalse(ddlCache.isEmpty(), "DDL cache must not be empty after initCatalogReader");
        Map<String, String> schemaTables = ddlCache.get("test_schema");
        assertNotNull(schemaTables, "test_schema must be in DDL cache");
        String tableDdl = schemaTables.get("test_table");
        assertNotNull(tableDdl, "test_table must have DDL in cache");
        assertTrue(tableDdl.startsWith("CREATE TABLE"),
                "DDL must start with CREATE TABLE");
    }

    @Test
    void readParquetFullScanReturnsRows() throws Exception {
        CountingListener listener = new CountingListener();
        executionService.readParquet(allocator,
                "SELECT * FROM test_schema.test_table", null, listener, true);
        assertTrue(listener.totalRows > 0);
    }

    @Test
    void readParquetWithFilterReturnsFewer() throws Exception {
        CountingListener all = new CountingListener();
        CountingListener filtered = new CountingListener();

        executionService.readParquet(allocator,
                "SELECT * FROM test_schema.test_table", null, all, true);
        executionService.readParquet(allocator,
                "SELECT * FROM test_schema.test_table WHERE \"tinyint_col\" = 0",
                null, filtered, true);

        assertTrue(filtered.totalRows > 0);
        assertTrue(filtered.totalRows < all.totalRows,
                "Filtered should return fewer rows than full scan");
    }

    static class CountingListener implements FlightProducer.ServerStreamListener {
        int totalRows;
        VectorSchemaRoot root;

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {
            this.root = root;
        }

        @Override
        public void putNext() {
            totalRows += root.getRowCount();
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            totalRows += root.getRowCount();
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {
            // Metadata frames do not contribute to the row count asserted by this listener.
        }

        @Override
        public boolean isReady() { return true; }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            // The test listener is always ready and never needs a readiness callback.
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            // The test listener is never cancelled and does not register a callback.
        }

        @Override
        public void error(Throwable ex) { throw new RuntimeException(ex); }

        @Override
        public void completed() {
            // Completion is observed through the accumulated row count.
        }
    }
}
