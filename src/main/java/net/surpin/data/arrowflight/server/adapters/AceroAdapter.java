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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.LogUtil;

/**
 * Wraps Arrow Acero (Dataset/Sanner) for Parquet scanning with optional Substrait filter pushdown.
 */
public final class AceroAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AceroAdapter.class);

    private final int batchSize;
    private final long listenerReadyTimeoutMillis;

    /**
     * Creates an AceroAdapter.
     *
     * @param appConfig server configuration
     */
    public AceroAdapter(AppConfig appConfig) {
        this.batchSize = appConfig.batchSize();
        this.listenerReadyTimeoutMillis = appConfig.flightListenerReadyTimeoutMillis();
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
        scanBatches(allocator, query, parsedQuery, parquetUris, null, listener, startListener);
    }

    /**
     * Scans Parquet files with an optional Substrait filter.
     *
     * @param allocator       Arrow allocator
     * @param query           original SQL query for logging
     * @param parsedQuery     parsed query
     * @param parquetUris     Parquet file URIs
     * @param filterBytes     serialized Substrait filter, or null
     * @param listener        Flight stream listener
     * @param startListener   whether to start the listener
     * @throws Exception on scan failure
     */
    public void scanBatches(BufferAllocator allocator, String query,
            ParquetQueryParser parsedQuery, List<String> parquetUris, byte[] filterBytes,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        List<String> selectedColumns = parsedQuery.columns;
        String qid = LogUtil.qid();
        long startNanos = System.nanoTime();
        int numFiles = parquetUris.size();

        LOGGER.info("qid={} node={} acero=start files={} columns={} hasFilter={} query='{}'",
                qid, LogUtil.node(), numFiles,
                selectedColumns.isEmpty() ? "*" : String.join(",", selectedColumns),
                filterBytes != null, query);

        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                parquetUris.toArray(new String[0]));
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(scanOptions(selectedColumns, filterBytes));
             ArrowReader reader = scanner.scanBatches()) {

            Schema aceroSchema = scanner.schema();
            LOGGER.info("qid={} node={} acero=schema schema={} files={}",
                    qid, LogUtil.node(), aceroSchema, numFiles);

            VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
            if (startListener) {
                listener.start(vsr);
            }

            int batchesSent = 0;
            long rowsSent = 0;
            long backpressureNanos = 0;
            boolean cancelled = false;
            while (true) {
                if (!reader.loadNextBatch()) {
                    break;
                }
                int rowCount = vsr.getRowCount();
                if (rowCount == 0) {
                    vsr.clear();
                    continue;
                }
                long bpStart = System.nanoTime();
                if (!DuckDbAdapter.awaitListenerReady(
                        listener, listenerReadyTimeoutMillis)) {
                    backpressureNanos += System.nanoTime() - bpStart;
                    LOGGER.warn("qid={} node={} acero=cancelled batch={} rows={}",
                            qid, LogUtil.node(), batchesSent, rowsSent);
                    vsr.clear();
                    cancelled = true;
                    break;
                }
                backpressureNanos += System.nanoTime() - bpStart;
                listener.putNext();
                batchesSent++;
                rowsSent += rowCount;
                if (batchesSent % 10 == 0) {
                    LOGGER.debug("qid={} node={} acero=progress batches={} rows={} elapsed={} throughput={}rows/s",
                            qid, LogUtil.node(), batchesSent, rowsSent,
                            LogUtil.elapsedNanos(startNanos),
                            rowsSent * 1_000_000_000L / Math.max(1, System.nanoTime() - startNanos));
                }
                vsr.clear();
            }
            LOGGER.info("qid={} node={} acero=completed files={} batches={} rows={} backpressureMs={} elapsed={} cancelled={}",
                    qid, LogUtil.node(), numFiles, batchesSent, rowsSent,
                    backpressureNanos / 1_000_000,
                    LogUtil.elapsedNanos(startNanos), cancelled);
        }
    }

    private ScanOptions scanOptions(List<String> selectedColumns, byte[] filterBytes) {
        ScanOptions.Builder builder = new ScanOptions.Builder(batchSize)
                .columns(selectedColumns.isEmpty() ? Optional.empty()
                        : Optional.of(selectedColumns.toArray(new String[0])));
        if (filterBytes != null) {
            ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
            direct.put(filterBytes).flip();
            builder.substraitFilter(direct);
        }
        return builder.build();
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
        long startNanos = System.nanoTime();
        String qid = LogUtil.qid();
        LOGGER.debug("qid={} node={} acero=aggregateFile start file={} hasFilter={}",
                qid, LogUtil.node(), fileUri, filterBytes != null);

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
            LOGGER.debug("qid={} node={} acero=aggregateFile completed file={} count={} elapsed={}",
                    qid, LogUtil.node(), fileUri, count, LogUtil.elapsedNanos(startNanos));
            Object[] row = new Object[numCountStarCols];
            java.util.Arrays.fill(row, count);
            return Collections.singletonList(row);
        }
    }

    /**
     * Scans a group of Parquet files with Acero and exports each as Arrow C streams.
     * Returns registered Arrow streams that must stay open while DuckDB consumes them.
     *
     * @param allocator   Arrow buffer allocator
     * @param fileUris    Parquet file URIs
     * @param filterBytes optional Substrait filter bytes
     * @param cols        optional column projection
     * @param duckConn    DuckDB connection for stream registration
     * @return registered streams and their aliases
     * @throws Exception on scan or registration failure
     */
    public RegisteredArrowStreams exportToDuckDb(BufferAllocator allocator, List<String> fileUris,
            byte[] filterBytes, Optional<String[]> cols, DuckDBConnection duckConn)
            throws Exception {
        long startNanos = System.nanoTime();
        String qid = LogUtil.qid();
        LOGGER.debug("qid={} node={} acero=exportToDuckDb start files={}",
                qid, LogUtil.node(), fileUris.size());

        int n = fileUris.size();
        List<FileSystemDatasetFactory> factories = new ArrayList<>(n);
        List<Dataset> datasets = new ArrayList<>(n);
        List<Scanner> scanners = new ArrayList<>(n);
        List<ArrowReader> unexportedReaders = new ArrayList<>(n);
        List<ArrowArrayStream> cStreams = new ArrayList<>(n);
        List<String> aliases = new ArrayList<>(n);
        RegisteredArrowStreams registered = new RegisteredArrowStreams(
                aliases, cStreams, unexportedReaders, scanners, datasets, factories);
        boolean success = false;

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
                unexportedReaders.add(reader);
                ArrowArrayStream cStream = ArrowArrayStream.allocateNew(allocator);
                cStreams.add(cStream);
                // The exporter takes ownership of the reader, including on export failure.
                unexportedReaders.remove(reader);
                Data.exportArrayStream(allocator, reader, cStream);
                String alias = "t" + i;
                duckConn.registerArrowStream(alias, cStream);
                aliases.add(alias);
            }
            LOGGER.debug("qid={} node={} acero=exportToDuckDb completed files={} aliases={} elapsed={}",
                    qid, LogUtil.node(), fileUris.size(), aliases,
                    LogUtil.elapsedNanos(startNanos));
            success = true;
            return registered;
        } finally {
            if (!success) {
                try {
                    registered.close();
                } catch (Exception closeException) {
                    LOGGER.warn("qid={} Failed to close Acero resources during cleanup",
                            qid, closeException);
                }
            }
        }
    }

    /**
     * Owns the native and Java resources behind Arrow streams registered in DuckDB.
     * DuckDB pulls from registered streams lazily, so this object must remain open
     * until the query using its aliases has finished.
     */
    public static final class RegisteredArrowStreams implements AutoCloseable {
        private final List<String> aliases;
        private final List<ArrowArrayStream> streams;
        private final List<ArrowReader> unexportedReaders;
        private final List<Scanner> scanners;
        private final List<Dataset> datasets;
        private final List<FileSystemDatasetFactory> factories;
        private boolean closed;

        RegisteredArrowStreams(List<String> aliases, List<ArrowArrayStream> streams,
                List<ArrowReader> unexportedReaders, List<Scanner> scanners,
                List<Dataset> datasets, List<FileSystemDatasetFactory> factories) {
            this.aliases = aliases;
            this.streams = streams;
            this.unexportedReaders = unexportedReaders;
            this.scanners = scanners;
            this.datasets = datasets;
            this.factories = factories;
        }

        /**
         * Returns aliases registered in DuckDB.
         *
         * @return immutable alias list
         */
        public List<String> aliases() {
            return Collections.unmodifiableList(aliases);
        }

        @Override
        public void close() throws Exception {
            if (closed) {
                return;
            }
            closed = true;

            Exception failure = null;
            for (int i = streams.size() - 1; i >= 0; i--) {
                ArrowArrayStream stream = streams.get(i);
                try {
                    stream.release();
                } catch (Exception e) {
                    failure = appendFailure(failure, e);
                }
                try {
                    stream.close();
                } catch (Exception e) {
                    failure = appendFailure(failure, e);
                }
            }
            failure = closeAll(unexportedReaders, failure);
            failure = closeAll(scanners, failure);
            failure = closeAll(datasets, failure);
            failure = closeAll(factories, failure);
            if (failure != null) {
                throw failure;
            }
        }

        private static Exception closeAll(List<? extends AutoCloseable> resources,
                Exception failure) {
            for (int i = resources.size() - 1; i >= 0; i--) {
                try {
                    resources.get(i).close();
                } catch (Exception e) {
                    failure = appendFailure(failure, e);
                }
            }
            return failure;
        }

        private static Exception appendFailure(Exception failure, Exception next) {
            if (failure == null) {
                return next;
            }
            failure.addSuppressed(next);
            return failure;
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
