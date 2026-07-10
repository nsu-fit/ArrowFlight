package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Wraps Arrow Acero (Dataset/Sanner) for Parquet scanning with optional Substrait filter pushdown.
 */
public final class AceroAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AceroAdapter.class);

    private final int batchSize;

    /**
     * Creates an AceroAdapter.
     *
     * @param appConfig server configuration
     */
    public AceroAdapter(AppConfig appConfig) {
        this.batchSize = appConfig.batchSize();
    }

    /**
     * Scans Parquet files with Acero directly (no DuckDB), streaming Arrow batches to a listener.
     *
     * @param allocator       Arrow buffer allocator
     * @param query           original SQL query for logging
     * @param parsedQuery     parsed query (schema, table, columns, filter)
     * @param parquetUris     list of Parquet file URIs
     * @param listener        Flight stream listener
     * @param startListener   whether to call listener.start()
     * @throws Exception on scan failure
     */
    public void scanBatches(BufferAllocator allocator, String query,
            ParquetQueryParser parsedQuery, List<String> parquetUris,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        List<String> selectedColumns = parsedQuery.columns;

        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                parquetUris.toArray(new String[0]));
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(new ScanOptions.Builder(batchSize)
                     .columns(selectedColumns.isEmpty() ? Optional.empty()
                             : Optional.of(selectedColumns.toArray(new String[0])))
                     .build());
             ArrowReader reader = scanner.scanBatches()) {

            Schema aceroSchema = scanner.schema();
            LOGGER.info("Executing Acero scan for query: {} with schema: {}", query, aceroSchema);

            VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
            if (startListener) {
                listener.start(vsr);
            }

            while (true) {
                if (!reader.loadNextBatch()) {
                    break;
                }
                int rowCount = vsr.getRowCount();
                if (rowCount == 0) {
                    vsr.clear();
                    continue;
                }
                if (!DuckDbAdapter.awaitListenerReady(listener)) {
                    LOGGER.warn("Flight listener cancelled during Acero scan");
                    vsr.clear();
                    break;
                }
                listener.putNext();
                vsr.clear();
            }
        }
    }

    /**
     * Scans a single Parquet file with Acero, counting rows (COUNT(*)-only path).
     *
     * @param allocator       Arrow buffer allocator
     * @param fileUri         Parquet file URI
     * @param filterBytes     optional Substrait filter bytes
     * @param cols            optional column projection
     * @param numCountStarCols number of COUNT(*) columns in output
     * @return list of result rows (single row with count)
     * @throws Exception on scan failure
     */
    public List<Object[]> aggregateFile(BufferAllocator allocator, String fileUri,
            byte[] filterBytes, Optional<String[]> cols,
            int numCountStarCols) throws Exception {

        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                new String[]{fileUri});
             Dataset dataset = factory.finish()) {

            ScanOptions.Builder optBuilder = new ScanOptions.Builder(batchSize).columns(cols);
            if (filterBytes != null) {
                ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
                direct.put(filterBytes).flip();
                optBuilder.substraitFilter(direct);
            }
            long count = 0;
            try (ArrowReader reader = dataset.newScan(optBuilder.build()).scanBatches()) {
                while (reader.loadNextBatch()) {
                    count += reader.getVectorSchemaRoot().getRowCount();
                }
            }
            Object[] row = new Object[numCountStarCols];
            java.util.Arrays.fill(row, count);
            return Collections.singletonList(row);
        }
    }

    /**
     * Scans a group of Parquet files with Acero and exports each as Arrow C streams.
     * Returns a list of ArrowArrayStreams registered with DuckDB for aggregation.
     *
     * @param allocator   Arrow buffer allocator
     * @param fileUris    Parquet file URIs
     * @param filterBytes optional Substrait filter bytes
     * @param cols        optional column projection
     * @param duckConn    DuckDB connection for stream registration
     * @return list of registered stream aliases used in queries
     * @throws Exception on scan or registration failure
     */
    public List<String> exportToDuckDb(BufferAllocator allocator, List<String> fileUris,
            byte[] filterBytes, Optional<String[]> cols, DuckDBConnection duckConn)
            throws Exception {

        int n = fileUris.size();
        List<FileSystemDatasetFactory> factories = new ArrayList<>(n);
        List<Dataset> datasets = new ArrayList<>(n);
        List<Scanner> scanners = new ArrayList<>(n);
        List<ArrowReader> readers = new ArrayList<>(n);
        List<ArrowArrayStream> cStreams = new ArrayList<>(n);
        List<String> aliases = new ArrayList<>(n);

        try {
            for (int i = 0; i < n; i++) {
                FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                        new String[]{fileUris.get(i)});
                factories.add(factory);
                Dataset dataset = factory.finish();
                datasets.add(dataset);

                ScanOptions.Builder optBuilder = new ScanOptions.Builder(batchSize).columns(cols);
                if (filterBytes != null) {
                    ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
                    direct.put(filterBytes).flip();
                    optBuilder.substraitFilter(direct);
                }
                Scanner scanner = dataset.newScan(optBuilder.build());
                scanners.add(scanner);
                ArrowReader reader = scanner.scanBatches();
                readers.add(reader);
                ArrowArrayStream cStream = ArrowArrayStream.allocateNew(allocator);
                cStreams.add(cStream);
                Data.exportArrayStream(allocator, reader, cStream);
                String alias = "t" + i;
                duckConn.registerArrowStream(alias, cStream);
                aliases.add(alias);
            }
            return aliases;
        } finally {
            // On failure, close resources in reverse-dependency order.
            for (int i = cStreams.size() - 1; i >= 0; i--) {
                try { cStreams.get(i).close(); } catch (Exception ignored) { }
            }
            for (int i = readers.size() - 1; i >= 0; i--) {
                try { readers.get(i).close(); } catch (Exception ignored) { }
            }
            for (int i = scanners.size() - 1; i >= 0; i--) {
                try { scanners.get(i).close(); } catch (Exception ignored) { }
            }
            for (int i = datasets.size() - 1; i >= 0; i--) {
                try { datasets.get(i).close(); } catch (Exception ignored) { }
            }
            for (int i = factories.size() - 1; i >= 0; i--) {
                try { factories.get(i).close(); } catch (Exception ignored) { }
            }
        }
    }

    /**
     * Concatenates all batches from an ArrowReader into a single VectorSchemaRoot.
     *
     * @param allocator Arrow buffer allocator
     * @param reader    Arrow reader
     * @return concatenated VectorSchemaRoot
     * @throws IOException on read failure
     */
    public static VectorSchemaRoot concatBatches(BufferAllocator allocator, ArrowReader reader)
            throws java.io.IOException {
        VectorSchemaRoot src = reader.getVectorSchemaRoot();
        int numCols = src.getSchema().getFields().size();
        List<FieldVector> outVecs = new ArrayList<>(numCols);
        for (Field f : src.getSchema().getFields()) {
            FieldVector v = f.createVector(allocator);
            v.allocateNew();
            outVecs.add(v);
        }
        int outRow = 0;
        while (reader.loadNextBatch()) {
            for (int r = 0; r < src.getRowCount(); r++) {
                for (int c = 0; c < numCols; c++) {
                    outVecs.get(c).copyFromSafe(r, outRow, src.getVector(c));
                }
                outRow++;
            }
        }
        for (FieldVector v : outVecs) {
            v.setValueCount(outRow);
        }
        VectorSchemaRoot result = new VectorSchemaRoot(src.getSchema().getFields(), outVecs);
        result.setRowCount(outRow);
        return result;
    }
}
