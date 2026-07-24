package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.vector.FieldVector;

import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.LogUtil;

/**
 * Manages DuckDB connection pool and SQL execution.
 * Each Java worker thread gets its own DuckDB in-memory connection.
 */
public final class DuckDbAdapter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DuckDbAdapter.class);
    private static final long LISTENER_STATE_POLL_NANOS =
            TimeUnit.MILLISECONDS.toNanos(5);
    private static final long LISTENER_TIMING_LOG_THRESHOLD_NANOS =
            TimeUnit.MILLISECONDS.toNanos(100);
    private static final long PRODUCER_STOP_TIMEOUT_SECONDS = 5;
    private final ThreadLocal<Connection> threadConn;
    private final Set<Connection> allConnections = ConcurrentHashMap.newKeySet();
    private final ExecutorService ioPool;

    private final int batchSize;
    private final int duckDbGroups;
    private final AppConfig appConfig;

    /**
     * Creates DuckDB adapter with thread-local connection pool.
     *
     * @param appConfig server configuration
     * @param ioPool    shared I/O thread pool for parallel execution
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool) {
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.batchSize = appConfig.batchSize();
        this.duckDbGroups = appConfig.duckDbGroups();

        String jdbcUrl = "jdbc:duckdb:";
        Properties connProps = new Properties();
        if (appConfig.duckDbAllowUnsignedExtensions()) {
            connProps.setProperty("allow_unsigned_extensions", "true");
        }
        this.threadConn = ThreadLocal.withInitial(() -> {
            try {
                Properties props = new Properties();
                if (appConfig.duckDbAllowUnsignedExtensions()) {
                    props.setProperty("allow_unsigned_extensions", "true");
                }
                Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
                configureConnection(conn);
                allConnections.add(conn);
                return conn;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create thread-local DuckDB connection", e);
            }
        });

        int warmCount = appConfig.duckDbWarmConnections();
        if (warmCount > 0) {
            LOGGER.info("node={} duckdb=warmup connections={}", LogUtil.node(), warmCount);
            List<Future<Connection>> futs = new ArrayList<>(warmCount);
            for (int i = 0; i < warmCount; i++) {
                futs.add(ioPool.submit(threadConn::get));
            }
            for (Future<Connection> f : futs) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOGGER.warn("node={} duckdb=warmupFailed", LogUtil.node(), e);
                }
            }
            LOGGER.info("node={} duckdb=warmupDone", LogUtil.node());
        }
    }

    /**
     * Configure DuckDB connection settings
     * @param conn JDBC connection
     * @throws Exception on configuration failure
     */
    private void configureConnection(Connection conn) throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET threads = " + appConfig.duckDbThreads());
            String dataDir = appConfig.dataDir();
            if (dataDir != null) {
                setArrayOptionIfPresent(s, dataDir);
            } else {
                s.execute("SET allowed_paths = ARRAY[]");
            }
            String hdfsExtension = appConfig.duckDbHdfsExtension();
            if (hdfsExtension != null) {
                s.execute("LOAD " + sqlStringLiteral(hdfsExtension));
                LOGGER.info("node={} duckdb=hdfsExtensionLoaded path={}",
                        LogUtil.node(), hdfsExtension);
                setOptionIfPresent(s, "hdfs_default_namenode",
                        appConfig.duckDbHdfsDefaultNamenode());
                setOptionIfPresent(s, "hdfs_ha_namenodes",
                        appConfig.duckDbHdfsHaNamenodes());
                setOptionIfPresent(s, "hdfs_shortcircuit",
                        appConfig.duckDbHdfsShortcircuit());
                setOptionIfPresent(s, "hdfs_domain_socket_path",
                        appConfig.duckDbHdfsDomainSocketPath());
            }
        }
    }

    /**
     * Set DuckDB option if value is non-null
     * @param statement JDBC statement
     * @param optionName option name
     * @param value option value
     * @throws Exception on SQL failure
     */
    private static void setOptionIfPresent(Statement statement, String optionName, String value)
            throws Exception {
        if (value == null) {
            return;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            statement.execute("SET " + optionName + " = " + value.toLowerCase());
        } else {
            statement.execute("SET " + optionName + " = " + sqlStringLiteral(value));
        }
    }

    /**
     * Set DuckDB array option if value is non-null
     *
     * @param statement JDBC statement
     * @param value     option value
     * @throws Exception on SQL failure
     */
    private static void setArrayOptionIfPresent(Statement statement, String value)
            throws Exception {
        if (value == null) {
            return;
        }
        // DuckDB expects VARCHAR[] e.g. SET allowed_paths = ['/path']
        String escaped = sqlStringLiteral(value);
        statement.execute("SET " + "allowed_paths" + " = " + "ARRAY[" + escaped + "]");
    }

    /**
     * Returns this thread's DuckDB connection.
     *
     * @return JDBC connection
     */
    public Connection connection() {
        return threadConn.get();
    }

    /**
     * Returns the shared I/O executor service.
     *
     * @return executor service
     */
    public ExecutorService ioPool() {
        return ioPool;
    }

    /**
     * Returns the number of DuckDB groups for parallel aggregation.
     *
     * @return group count
     */
    public int duckDbGroups() {
        return duckDbGroups;
    }

    /**
     * Returns the Arrow batch size for DuckDB exports.
     *
     * @return batch size
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Streams DuckDB SQL query results as Arrow batches to a Flight listener.
     * Uses a producer-consumer pattern to decouple DuckDB reads from gRPC sends,
     * preventing DuckDB from blocking on backpressure from a slow consumer.
     *
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL query
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    @SuppressWarnings("java:S3776") // Streaming lifecycle is kept in one scope for deterministic resource cleanup.
    public void streamSql(BufferAllocator allocator, String duckSql,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        String qid = LogUtil.qid();
        long t = LogUtil.mark();
        long streamStartNanos = System.nanoTime();
        LOGGER.debug("qid={} node={} thread={} duckdb=start batchSize={} sql='{}'",
                qid, LogUtil.node(), Thread.currentThread().getName(), batchSize, duckSql);
        Connection conn = threadConn.get();
        AtomicLong firstBatchNanos = new AtomicLong(-1);
        long backpressureNanos = 0;
        try (Statement stmt = conn.createStatement();
                org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) stmt.executeQuery(duckSql);
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            VectorSchemaRoot duckRoot = reader.getVectorSchemaRoot();

            int poolCapacity = 4;
            ArrayBlockingQueue<StreamChunk> readyQueue =
                    new ArrayBlockingQueue<>(poolCapacity);
            ArrayBlockingQueue<VectorSchemaRoot> freeQueue =
                    new ArrayBlockingQueue<>(poolCapacity);
            for (int i = 0; i < poolCapacity; i++) {
                freeQueue.put(VectorSchemaRoot.create(duckRoot.getSchema(), allocator));
            }

            VectorSchemaRoot sendRoot = VectorSchemaRoot.create(duckRoot.getSchema(), allocator);
            try {
            if (startListener) {
                listener.start(sendRoot);
            }
            ListenerReadiness listenerReadiness = new ListenerReadiness(
                    listener, appConfig.flightListenerReadyTimeoutMillis());

            AtomicBoolean producerStarted = new AtomicBoolean(false);
            AtomicBoolean cancelled = new AtomicBoolean(false);
            CountDownLatch producerStopped = new CountDownLatch(1);

            Future<?> producerFuture = ioPool.submit(() -> {
                producerStarted.set(true);
                VectorSchemaRoot held = null;
                Exception terminalError = null;
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        if (cancelled.get()) {
                            break;
                        }
                        held = freeQueue.take();
                        if (cancelled.get()) {
                            freeQueue.put(held);
                            held = null;
                            break;
                        }
                        if (!reader.loadNextBatch()) {
                            freeQueue.put(held);
                            held = null;
                            break;
                        }
                        int rows = duckRoot.getRowCount();
                        if (rows == 0) {
                            duckRoot.clear();
                            freeQueue.put(held);
                            held = null;
                            continue;
                        }
                        if (firstBatchNanos.get() < 0) {
                            firstBatchNanos.set(System.nanoTime() - streamStartNanos);
                        }
                        transferRoot(duckRoot, held);
                        duckRoot.clear();
                        readyQueue.put(new StreamChunk(held, null, false));
                        held = null;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (!cancelled.get()) {
                        terminalError = e;
                    }
                } catch (Exception e) {
                    terminalError = e;
                } finally {
                    if (held != null) {
                        held.close();
                    }
                    if (!cancelled.get()) {
                        try {
                            readyQueue.put(new StreamChunk(null, terminalError, true));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    producerStopped.countDown();
                }
            });

            int batchesSent = 0;
            long rowsSent = 0;
            try {
                while (true) {
                    if (cancelled.get()) {
                        break;
                    }
                    StreamChunk chunk;
                    try {
                        chunk = readyQueue.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    if (chunk.end()) {
                        if (chunk.error() != null) {
                            throw new RuntimeException(chunk.error());
                        }
                        break;
                    }
                    VectorSchemaRoot buf = chunk.root();

                    transferRoot(buf, sendRoot);
                    buf.clear();
                    freeQueue.put(buf);

                    if (firstBatchNanos.get() > 0 && batchesSent == 0) {
                        LogUtil.logTiming(t, "duckdb.firstBatch",
                                "sql='" + duckSql.substring(0, Math.min(100, duckSql.length())) + "'");
                    }

                    int rows = sendRoot.getRowCount();
                    long bpStart = System.nanoTime();
                    listenerReadiness.await();
                    backpressureNanos += System.nanoTime() - bpStart;
                    listener.putNext();
                    batchesSent++;
                    rowsSent += rows;
                    if (batchesSent % 10 == 0) {
                        LOGGER.debug("qid={} node={} duckdb=progress batches={} rows={} elapsed={} throughput={}rows/s",
                                qid, LogUtil.node(), batchesSent, rowsSent,
                                LogUtil.elapsedNanos(streamStartNanos),
                                rowsSent * 1_000_000_000L
                                        / Math.max(1, System.nanoTime() - streamStartNanos));
                    }
                }
            } catch (Exception e) {
                cancelled.set(true);
                throw e;
            } finally {
                if (cancelled.get()) {
                    producerFuture.cancel(true);
                    if (producerStarted.get() && !producerStopped.await(
                            PRODUCER_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        LOGGER.warn("qid={} node={} duckdb=producerStopTimeout timeoutSec={}",
                                qid, LogUtil.node(), PRODUCER_STOP_TIMEOUT_SECONDS);
                    }
                }
            }

            LogUtil.logTiming(t, "duckdb.streamSql",
                    "batches=" + batchesSent + " rows=" + rowsSent
                    + " backpressureMs=" + backpressureNanos / 1_000_000);
            LOGGER.debug("qid={} node={} duckdb=completed batches={} rows={} ttfB={} backpressureMs={} elapsed={} cancelled={}",
                    qid, LogUtil.node(), batchesSent, rowsSent,
                    firstBatchNanos.get() >= 0 ? formatDuration(firstBatchNanos.get()) : "N/A",
                    backpressureNanos / 1_000_000,
                    LogUtil.elapsedNanos(streamStartNanos), cancelled.get());
            } finally {
                sendRoot.close();
                List<VectorSchemaRoot> remaining = new ArrayList<>();
                freeQueue.drainTo(remaining);
                List<StreamChunk> ready = new ArrayList<>();
                readyQueue.drainTo(ready);
                ready.stream()
                        .map(StreamChunk::root)
                        .filter(java.util.Objects::nonNull)
                        .forEach(remaining::add);
                for (VectorSchemaRoot root : remaining) {
                    root.close();
                }
            }
        }
    }

    /**
     * Transfers buffer ownership from source VectorSchemaRoot to destination
     * via FieldVector.makeTransferPair. Source buffers are moved to destination,
     * source gets destination's old buffers (which are then cleared by the caller).
     *
     * @param src source root (data will be moved out)
     * @param dst destination root (receives source data)
     */
    private static void transferRoot(VectorSchemaRoot src, VectorSchemaRoot dst) {
        List<FieldVector> srcVecs = src.getFieldVectors();
        List<FieldVector> dstVecs = dst.getFieldVectors();
        for (int i = 0; i < srcVecs.size(); i++) {
            srcVecs.get(i).makeTransferPair(dstVecs.get(i)).transfer();
        }
        dst.setRowCount(src.getRowCount());
    }

    /**
     * Carries either a ready Arrow batch or the producer's terminal state.
     *
     * @param root Arrow batch, or null for the terminal state
     * @param error producer failure, or null for successful completion
     * @param end whether this chunk terminates the stream
     */
    private record StreamChunk(VectorSchemaRoot root, Exception error, boolean end) {
    }

    /**
     * Formats an elapsed nanosecond duration for structured logging.
     *
     * @param nanos elapsed nanoseconds
     * @return human-readable duration
     */
    private static String formatDuration(long nanos) {
        if (nanos < 1_000_000L) {
            return nanos / 1_000L + "us";
        }
        return nanos / 1_000_000L + "ms";
    }

    /**
     * Reads only the Parquet footer to get the total row count. Zero column I/O.
     *
     * @param fileSystem Hadoop FileSystem
     * @param full       absolute file path
     * @return total row count across all row groups
     * @throws IOException on read failure
     */
    public static long footerRowCount(org.apache.hadoop.fs.FileSystem fileSystem,
            org.apache.hadoop.fs.Path full) throws IOException {
        long t = LogUtil.mark();
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new InputFile() {
            @Override
            public long getLength() {
                return fileLen;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            long count = 0;
            for (BlockMetaData b : pfr.getFooter().getBlocks()) {
                count += b.getRowCount();
            }
            LogUtil.logTiming(t, "duckdb.footerRowCount", "file=" + full.getName() + " count=" + count);
            return count;
        }
    }

    /**
     * Reads MIN/MAX/COUNT statistics from Parquet footer. Zero column I/O.
     *
     * @param fileSystem Hadoop FileSystem
     * @param full       absolute file path
     * @param pq         parsed query with select expressions
     * @return optional array of aggregated stats values, empty if any stats are missing
     * @throws IOException on read failure
     */
    @SuppressWarnings("java:S3776") // Footer aggregation handles every supported statistic in one pass.
    public static Optional<Object[]> footerStats(org.apache.hadoop.fs.FileSystem fileSystem,
            org.apache.hadoop.fs.Path full, ParquetQueryParser pq) throws IOException {
        long t = LogUtil.mark();
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new InputFile() {
            @Override
            public long getLength() {
                return fileLen;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
            int n = exprs.size();
            Object[] result = new Object[n];
            long totalRows = 0;

            for (BlockMetaData block : pfr.getFooter().getBlocks()) {
                totalRows += block.getRowCount();

                for (ColumnChunkMetaData cc : block.getColumns()) {
                    String colName = cc.getPath().toDotString();

                    for (int i = 0; i < n; i++) {
                        ParquetQueryParser.SelectExpr expr = exprs.get(i);
                        if (!colName.equals(expr.inputColumn)) {
                            continue;
                        }

                        Statistics<?> stats = cc.getStatistics();
                        if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MIN) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
                                return Optional.empty();
                            }
                            result[i] = minOf(result[i], stats.genericGetMin());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MAX) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
                                return Optional.empty();
                            }
                            result[i] = maxOf(result[i], stats.genericGetMax());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT) {
                            if (stats == null || stats.getNumNulls() < 0) {
                                return Optional.empty();
                            }
                            long nonNulls = block.getRowCount() - stats.getNumNulls();
                            result[i] = (result[i] == null ? 0L
                                    : ((Number) result[i]).longValue()) + nonNulls;
                        }
                    }
                }
            }

            for (int i = 0; i < n; i++) {
                if (exprs.get(i).func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR) {
                    result[i] = totalRows;
                }
            }
            LogUtil.logTiming(t, "duckdb.footerStats", "file=" + full.getName());
            return Optional.of(result);
        }
    }

    /**
     * Builds a DuckDB SQL SELECT query from parsed query.
     *
     * @param pq         parsed query
     * @param fromClause FROM clause (e.g. read_parquet([...]) or table reference)
     * @return DuckDB SQL string
     */
    public static String buildDuckSql(ParquetQueryParser pq, String fromClause) {
        return buildDuckSqlWithFilter(pq, fromClause, false);
    }

    /**
     * Builds a DuckDB SQL SELECT query after filter has been applied upstream.
     *
     * @param pq              parsed query
     * @param fromClause      FROM clause
     * @param filterApplied   whether filter was already applied upstream
     * @return DuckDB SQL string
     */
    @SuppressWarnings("java:S3776") // SQL rendering branches directly over the supported expression model.
    public static String buildDuckSqlWithFilter(ParquetQueryParser pq, String fromClause,
            boolean filterApplied) {
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;

        for (String gbCol : pq.groupByColumnNames) {
            if (!first) {
                sql.append(", ");
            }
            first = false;
            sql.append('"').append(gbCol).append('"');
        }

        Set<String> gbSet = new LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && gbSet.contains(expr.inputColumn)) {
                continue;
            }
            if (!first) {
                sql.append(", ");
            }
            first = false;
            switch (expr.func) {
                case COUNT_STAR -> sql.append("count(*)");
                case COUNT -> sql.append("count(").append(aggregateInput(expr)).append(")");
                case SUM -> sql.append("sum(").append(aggregateInput(expr)).append(")");
                case MIN -> sql.append("min(").append(aggregateInput(expr)).append(")");
                case MAX -> sql.append("max(").append(aggregateInput(expr)).append(")");
                case COLUMN -> sql.append('"').append(expr.inputColumn).append('"');
                default -> { }
            }
            if (expr.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && expr.outputName != null && !expr.outputName.isBlank()) {
                sql.append(" AS ").append(quoteIdentifier(expr.outputName));
            }
        }

        sql.append(" FROM ").append(fromClause);
        if (!filterApplied && pq.filter != null && !pq.filter.isBlank()) {
            sql.append(" WHERE ").append(pq.filter);
        }
        if (!pq.groupByColumnNames.isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < pq.groupByColumnNames.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append('"').append(pq.groupByColumnNames.get(i)).append('"');
            }
        }
        return sql.toString();
    }

    /**
     * Builds a grouped DuckDB SQL for parallel aggregation with multiple file references.
     *
     * @param pq                parsed query
     * @param numFiles          number of files
     * @param filterApplied     whether filter was already applied
     * @return DuckDB SQL string
     */
    public static String buildGroupedDuckSql(ParquetQueryParser pq, int numFiles,
            boolean filterApplied) {
        if (numFiles == 1) {
            return buildDuckSqlWithFilter(pq, "\"t0\"", filterApplied);
        }
        StringBuilder from = new StringBuilder("(");
        for (int i = 0; i < numFiles; i++) {
            if (i > 0) {
                from.append(" UNION ALL ");
            }
            from.append("SELECT * FROM \"t").append(i).append('"');
        }
        from.append(')');
        return buildDuckSqlWithFilter(pq, from.toString(), filterApplied);
    }

    /**
     * Creates a DuckDB read_parquet FROM clause from file paths.
     *
     * @param duckDbPaths list of file paths
     * @return FROM clause string
     */
    public static String readParquetFromClause(List<String> duckDbPaths) {
        StringBuilder sb = new StringBuilder("read_parquet([");
        for (int i = 0; i < duckDbPaths.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(sqlStringLiteral(duckDbPaths.get(i)));
        }
        sb.append("])");
        return sb.toString();
    }

    /**
     * Builds a DuckDB select SQL with read_parquet FROM clause.
     *
     * @param pq    parsed query
     * @param from  FROM clause
     * @return DuckDB SQL
     */
    public static String buildSelectSql(ParquetQueryParser pq, String from) {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (pq.selectExprs.isEmpty()) {
            sql.append('*');
        } else {
            boolean first = true;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                appendSelectExpr(sql, expr);
            }
        }

        sql.append(" FROM ").append(from);

        if (pq.filter != null && !pq.filter.isBlank()) {
            sql.append(" WHERE ").append(pq.filter);
        }
        return sql.toString();
    }

    /**
     * Appends a DuckDB select expression to SQL string builder.
     *
     * @param sql  SQL builder
     * @param expr select expression
     */
    public static void appendSelectExpr(StringBuilder sql, ParquetQueryParser.SelectExpr expr) {
        switch (expr.func) {
            case COUNT_STAR -> sql.append("count(*)");
            case COUNT -> sql.append("count(").append(aggregateInput(expr)).append(")");
            case SUM -> sql.append("sum(").append(aggregateInput(expr)).append(")");
            case MIN -> sql.append("min(").append(aggregateInput(expr)).append(")");
            case MAX -> sql.append("max(").append(aggregateInput(expr)).append(")");
            case COLUMN -> sql.append(quoteIdentifier(expr.inputColumn));
            default -> { }
        }
        if (expr.outputName != null && !expr.outputName.isBlank()) {
            sql.append(" AS ").append(quoteIdentifier(expr.outputName));
        }
    }

    /**
     * Returns parser-validated aggregate input SQL with a simple-column fallback.
     *
     * @param expr parsed aggregate expression
     * @return executable aggregate input SQL
     */
    private static String aggregateInput(ParquetQueryParser.SelectExpr expr) {
        return expr.inputExpression != null
                ? expr.inputExpression : quoteIdentifier(expr.inputColumn);
    }

    /**
     * Quotes an identifier for DuckDB SQL.
     *
     * @param identifier identifier to quote
     * @return quoted identifier
     */
    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Converts a string value to SQL string literal.
     *
     * @param value string value
     * @return SQL string literal
     */
    public static String sqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Checks whether a Flight listener is ready to receive data.
     *
     * @param listener      Flight stream listener
     * @param timeoutMillis maximum wait time in milliseconds
     * @return true when the listener is ready
     * @throws org.apache.arrow.flight.FlightRuntimeException if cancelled or timed out
     * @throws InterruptedException if waiting is interrupted
     * @throws IllegalArgumentException if timeout is not positive
     */
    public static boolean awaitListenerReady(
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            long timeoutMillis)
            throws InterruptedException {
        return new ListenerReadiness(listener, timeoutMillis).await();
    }

    /** Maintains one readiness signal and callback registration for a Flight stream. */
    private static final class ListenerReadiness {
        private final org.apache.arrow.flight.FlightProducer.ServerStreamListener listener;
        private final long timeoutMillis;
        private final Semaphore stateChanged = new Semaphore(0);

        /**
         * Registers callbacks used by every batch in one Flight stream.
         *
         * @param listener Flight stream listener
         * @param timeoutMillis maximum wait time in milliseconds
         */
        private ListenerReadiness(
                org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
                long timeoutMillis) {
            if (timeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "Flight listener readiness timeout must be positive: " + timeoutMillis);
            }
            this.listener = listener;
            this.timeoutMillis = timeoutMillis;
            Runnable stateChangeHandler = stateChanged::release;
            listener.setOnReadyHandler(stateChangeHandler);
            listener.setOnCancelHandler(stateChangeHandler);
        }

        /**
         * Waits until the listener can accept the next Arrow batch.
         *
         * @return true when the listener is ready
         * @throws InterruptedException if waiting is interrupted
         */
        private boolean await() throws InterruptedException {
            long deadlineNanos = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            long waitStart = System.nanoTime();
            long t = LogUtil.mark();
            while (true) {
                if (listener.isCancelled()) {
                    long waitedNanos = System.nanoTime() - waitStart;
                    LOGGER.info("qid={} node={} backpressure=cancelled waited={}",
                            LogUtil.qid(), LogUtil.node(), LogUtil.elapsedNanos(waitStart));
                    LogUtil.logTiming(t, "duckdb.awaitListenerReadyCancelled",
                            "waitMs=" + waitedNanos / 1_000_000);
                    throw CallStatus.CANCELLED
                            .withDescription("Flight client cancelled while waiting for readiness")
                            .toRuntimeException();
                }
                if (listener.isReady()) {
                    break;
                }

                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    LOGGER.warn("qid={} node={} backpressure=timeout timeoutMs={} waited={}",
                            LogUtil.qid(), LogUtil.node(), timeoutMillis,
                            LogUtil.elapsedNanos(waitStart));
                    LogUtil.logTiming(t, "duckdb.awaitListenerReadyTimeout",
                            "timeoutMs=" + timeoutMillis);
                    throw CallStatus.TIMED_OUT
                            .withDescription("Flight listener was not ready within "
                                    + timeoutMillis + " ms")
                            .toRuntimeException();
                }

                long waitSliceNanos = Math.min(remainingNanos, LISTENER_STATE_POLL_NANOS);
                if (stateChanged.tryAcquire(waitSliceNanos, TimeUnit.NANOSECONDS)) {
                    continue;
                }
            }
            long totalWaitNanos = System.nanoTime() - waitStart;
            if (totalWaitNanos > LISTENER_TIMING_LOG_THRESHOLD_NANOS) {
                LogUtil.logTiming(t, "duckdb.awaitListenerReady",
                        "waitMs=" + totalWaitNanos / 1_000_000);
                LOGGER.debug("qid={} node={} backpressure=waited waitMs={} ready=true cancelled=false",
                        LogUtil.qid(), LogUtil.node(), totalWaitNanos / 1_000_000);
            }
            return true;
        }
    }

    /**
     * Return minimum of two comparable values
     * @param a first value
     * @param b second value
     * @return minimum value
     */
    @SuppressWarnings("unchecked")
    private static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    /**
     * Return maximum of two comparable values
     * @param a first value
     * @param b second value
     * @return maximum value
     */
    @SuppressWarnings("unchecked")
    private static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    /**
     * Closes all DuckDB connections created by the thread-local pool.
     */
    @Override
    public void close() {
        for (Connection conn : allConnections) {
            try {
                conn.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close DuckDB connection", e);
            }
        }
        allConnections.clear();
    }
}
