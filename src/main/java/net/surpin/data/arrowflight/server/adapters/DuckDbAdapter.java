package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.memory.BufferAllocator;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Manages DuckDB connection pool and SQL execution.
 * Each Java worker thread gets its own DuckDB in-memory connection.
 */
public final class DuckDbAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DuckDbAdapter.class);

    private final ThreadLocal<Connection> threadConn;
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

        this.threadConn = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                configureConnection(conn);
                return conn;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create thread-local DuckDB connection", e);
            }
        });
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
                setArrayOptionIfPresent(s, "allowed_paths", dataDir);
            } else {
                s.execute("SET allowed_paths = ARRAY[]");
            }
            if (appConfig.duckDbAllowUnsignedExtensions()) {
                s.execute("SET allow_unsigned_extensions = true");
            }
            String hdfsExtension = appConfig.duckDbHdfsExtension();
            if (hdfsExtension != null) {
                s.execute("LOAD " + sqlStringLiteral(hdfsExtension));
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
     * @param statement JDBC statement
     * @param optionName option name
     * @param value option value
     * @throws Exception on SQL failure
     */
    private static void setArrayOptionIfPresent(Statement statement, String optionName, String value)
            throws Exception {
        if (value == null) {
            return;
        }
        // DuckDB expects VARCHAR[] e.g. SET allowed_paths = ['/path']
        String escaped = sqlStringLiteral(value);
        statement.execute("SET " + optionName + " = " + "ARRAY[" + escaped + "]");
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
     *
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL query
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    public void streamSql(BufferAllocator allocator, String duckSql,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        LOGGER.info("Executing DuckDB SQL with Arrow batch size {}: {}", batchSize, duckSql);
        Connection conn = threadConn.get();
        try (Statement stmt = conn.createStatement();
                org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) stmt.executeQuery(duckSql);
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            if (startListener) {
                listener.start(root);
            }

            int batchesSent = 0;
            long rowsSent = 0;
            boolean cancelled = false;
            while (!cancelled && reader.loadNextBatch()) {
                if (root.getRowCount() == 0) {
                    root.clear();
                    continue;
                }
                if (!awaitListenerReady(
                        listener, appConfig.flightListenerReadyTimeoutMillis())) {
                    cancelled = true;
                    root.clear();
                    break;
                }
                listener.putNext();
                batchesSent++;
                rowsSent += root.getRowCount();
                root.clear();
            }
            LOGGER.info("DuckDB sent {} Flight batch(es), {} row(s){}",
                    batchesSent, rowsSent,
                    cancelled ? " before cancellation" : "");
        }
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
    public static Optional<Object[]> footerStats(org.apache.hadoop.fs.FileSystem fileSystem,
            org.apache.hadoop.fs.Path full, ParquetQueryParser pq) throws IOException {
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
                case COUNT_STAR -> sql.append("count(*) AS \"count(*)\"");
                case COUNT -> sql.append("count(\"").append(expr.inputColumn).append("\")");
                case SUM -> sql.append("sum(\"").append(expr.inputColumn).append("\")");
                case MIN -> sql.append("min(\"").append(expr.inputColumn).append("\")");
                case MAX -> sql.append("max(\"").append(expr.inputColumn).append("\")");
                case COLUMN -> sql.append('"').append(expr.inputColumn).append('"');
                default -> { }
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
            case COUNT -> sql.append("count(").append(quoteIdentifier(expr.inputColumn)).append(")");
            case SUM -> sql.append("sum(").append(quoteIdentifier(expr.inputColumn)).append(")");
            case MIN -> sql.append("min(").append(quoteIdentifier(expr.inputColumn)).append(")");
            case MAX -> sql.append("max(").append(quoteIdentifier(expr.inputColumn)).append(")");
            case COLUMN -> sql.append(quoteIdentifier(expr.inputColumn));
            default -> { }
        }
        if (expr.outputName != null && !expr.outputName.isBlank()) {
            sql.append(" AS ").append(quoteIdentifier(expr.outputName));
        }
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
     * @return true if ready, false if cancelled
     * @throws InterruptedException if waiting is interrupted
     * @throws IllegalArgumentException if timeout is not positive
     */
    public static boolean awaitListenerReady(
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            long timeoutMillis)
            throws InterruptedException {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Flight listener readiness timeout must be positive: " + timeoutMillis);
        }

        Semaphore stateChanged = new Semaphore(0);
        Runnable stateChangeHandler = stateChanged::release;
        listener.setOnReadyHandler(stateChangeHandler);
        listener.setOnCancelHandler(stateChangeHandler);

        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!listener.isCancelled() && !listener.isReady()) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0
                    || !stateChanged.tryAcquire(remainingNanos, TimeUnit.NANOSECONDS)) {
                LOGGER.warn("Listener readiness timeout after {}ms", timeoutMillis);
                return false;
            }
        }
        return !listener.isCancelled() && listener.isReady();
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
}
