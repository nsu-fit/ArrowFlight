package net.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tests Hadoop Parquet decoding and Arrow C Data registration. */
@Tag("unit")
class HadoopParquetArrowReaderTest {

    private static final java.nio.file.Path PARQUET_FILE = Paths.get(
            "src/test/resources/lineitem_tiny/tpch/lineitem/lineitem_tiny.parquet")
            .toAbsolutePath();

    /** Verifies bounded batches preserve all rows and schema. */
    @Test
    void readsMultipleArrowBatches() throws Exception {
        Configuration configuration = new Configuration(false);
        try (RootAllocator allocator = new RootAllocator();
                FileSystem fileSystem = localFileSystem(configuration);
                HadoopParquetArrowReader reader = new HadoopParquetArrowReader(
                        allocator, fileSystem, List.of(new Path(PARQUET_FILE.toUri())), 17)) {
            int rows = 0;
            int batches = 0;
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
                batches++;
            }
            assertEquals(100, rows);
            assertEquals(6, batches);
            assertEquals(16, reader.getVectorSchemaRoot().getFieldVectors().size());
        }
    }

    /** Verifies DuckDB consumes the exported Arrow stream through C Data. */
    @Test
    void duckDbQueriesRegisteredArrowStream() throws Exception {
        Configuration configuration = new Configuration(false);
        try (RootAllocator allocator = new RootAllocator();
                FileSystem fileSystem = localFileSystem(configuration);
                HadoopParquetArrowReader reader = new HadoopParquetArrowReader(
                        allocator, fileSystem, List.of(new Path(PARQUET_FILE.toUri())), 19);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Data.exportArrayStream(allocator, reader, stream);
            connection.unwrap(DuckDBConnection.class).registerArrowStream("lineitem", stream);
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery(
                            "SELECT count(*), min(l_orderkey) FROM lineitem")) {
                assertTrue(result.next());
                assertEquals(100, result.getLong(1));
                assertTrue(result.getLong(2) > 0);
            }
        }
    }

    /** Verifies Parquet reader decodes only requested top-level columns. */
    @Test
    void projectsRequestedColumns() throws Exception {
        Configuration configuration = new Configuration(false);
        try (RootAllocator allocator = new RootAllocator();
                FileSystem fileSystem = localFileSystem(configuration);
                HadoopParquetArrowReader reader = new HadoopParquetArrowReader(
                        allocator, fileSystem, List.of(new Path(PARQUET_FILE.toUri())), 32,
                        Set.of("l_orderkey"))) {
            assertTrue(reader.loadNextBatch());
            assertEquals(1, reader.getVectorSchemaRoot().getFieldVectors().size());
            assertEquals(32, reader.getVectorSchemaRoot().getRowCount());
        }
    }

    /** Verifies invalid batch sizes cannot create non-progressing streams. */
    @Test
    void rejectsNonPositiveBatchSize() throws Exception {
        Configuration configuration = new Configuration(false);
        try (RootAllocator allocator = new RootAllocator();
                FileSystem fileSystem = localFileSystem(configuration)) {
            assertThrows(IllegalArgumentException.class,
                    () -> new HadoopParquetArrowReader(allocator, fileSystem,
                            List.of(new Path(PARQUET_FILE.toUri())), 0));
        }
    }

    /**
     * Creates local Hadoop filesystem without the JDK-incompatible UGI cache.
     *
     * @param configuration Hadoop configuration
     * @return local filesystem
     */
    private static FileSystem localFileSystem(Configuration configuration) throws Exception {
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(URI.create("file:///"), configuration);
        return fileSystem;
    }
}
