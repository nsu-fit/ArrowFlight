package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.fs.Path;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import net.surpin.data.arrowflight.server.adapters.AceroAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.LogUtil;

import java.math.BigDecimal;

/**
 * Orchestrates query execution across DuckDB and Acero engines.
 * Routes queries to the optimal execution path based on query structure.
 */
public final class ExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionService.class);

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final AceroAdapter aceroAdapter;
    private final MetadataService metadataService;
    private final ExecutorService ioPool;
    private final AppConfig appConfig;
    private final Function<ParquetQueryParser, byte[]> filterBuilder;

    /**
     * Creates ExecutionService.
     *
     * @param parquetAdapter  Parquet metadata adapter
     * @param duckDbAdapter   DuckDB adapter
     * @param aceroAdapter    Acero adapter
     * @param metadataService schema/metadata service
     * @param appConfig       server configuration
     * @param ioPool          shared I/O thread pool
     * @param filterBuilder   converts parsed query to Substrait filter bytes, may return null
     */
    public ExecutionService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            AceroAdapter aceroAdapter, MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool,
            Function<ParquetQueryParser, byte[]> filterBuilder) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.aceroAdapter = aceroAdapter;
        this.metadataService = metadataService;
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.filterBuilder = filterBuilder;
    }

    /**
     * Reads specific Parquet files and sends Arrow batches through the listener.
     * Dispatches to JOIN, aggregation, or direct scan path based on query structure.
     *
     * @param allocator      Arrow buffer allocator
     * @param query          SQL query
     * @param fileUris       relative file paths
     * @param listener       Flight stream listener
     * @param startListener  whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        long tParse = LogUtil.mark();
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        LogUtil.logTiming(tParse, "parseQuery");

        if (parsedQuery.isJoin) {
            executeJoin(allocator, parsedQuery, fileUris, listener, startListener);
            return;
        }

        if (fileUris == null) {
            long tDiscover = LogUtil.mark();
            fileUris = parquetAdapter.locationsForQuery(query)
                    .keySet().toArray(new String[0]);
            LogUtil.logTiming(tDiscover, "files.discover", "files=" + fileUris.length);
        }

        List<Path> parquetFiles = resolveParquetFiles(parsedQuery, fileUris);
        if (parquetFiles.isEmpty()) {
            LOGGER.warn("No Parquet files to read for query: {}", query);
            return;
        }

        long tResolve = LogUtil.mark();
        List<String> resolvedUris = resolveUris(fileUris);
        LogUtil.logTiming(tResolve, "files.resolveUris", "files=" + resolvedUris.size());
        LOGGER.debug("qid={} node={} execution=filesResolved files={} paths={}",
                LogUtil.qid(), LogUtil.node(), resolvedUris.size(), resolvedUris);

        if (parsedQuery.hasAggregation) {
            LOGGER.info("qid={} node={} execution=engine engine=aggregation hasGroupBy={} isHdfs={} files={}",
                    LogUtil.qid(), LogUtil.node(), !parsedQuery.groupByColumnNames.isEmpty(),
                    isHdfsData(), parquetFiles.size());
            executeAggregation(allocator, parsedQuery, parquetFiles, resolvedUris,
                    fileUris, listener, startListener);
            return;
        }

        if (parsedQuery.filter != null && !parsedQuery.filter.isBlank()) {
            byte[] filterBytes = filterBuilder.apply(parsedQuery);
            if (isHdfsData() && filterBytes != null) {
                LOGGER.info("qid={} node={} execution=engine engine=Acero+SubstraitFilter files={}",
                        LogUtil.qid(), LogUtil.node(), resolvedUris.size());
                aceroAdapter.scanBatches(allocator, query, parsedQuery,
                        resolvedUris, filterBytes, listener, startListener);
                return;
            }
            if (isHdfsData()) {
                LOGGER.info("qid={} node={} execution=engine engine=Acero+DuckDB(hdfs-filter) files={}",
                        LogUtil.qid(), LogUtil.node(), resolvedUris.size());
                streamHdfsFilterViaArrow(allocator, parsedQuery, resolvedUris,
                        listener, startListener);
                return;
            }
            LOGGER.info("qid={} node={} execution=engine engine=DuckDB files={}",
                    LogUtil.qid(), LogUtil.node(), resolvedUris.size());
            String duckSql = DuckDbAdapter.buildSelectSql(parsedQuery,
                    DuckDbAdapter.readParquetFromClause(ducksDbPaths(resolvedUris)));
            duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
            return;
        }

        LOGGER.info("qid={} node={} execution=engine engine=Acero(full-scan) files={}",
                LogUtil.qid(), LogUtil.node(), resolvedUris.size());
        aceroAdapter.scanBatches(allocator, query, parsedQuery,
                resolvedUris, listener, startListener);
    }

    private boolean isHdfsData() {
        return "hdfs".equalsIgnoreCase(parquetAdapter.fileSystem().getUri().getScheme());
    }

    private void streamHdfsFilterViaArrow(BufferAllocator allocator, ParquetQueryParser pq,
            List<String> resolvedUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        try (BufferAllocator child = allocator.newChildAllocator(
                "hdfs-filter", 0, Long.MAX_VALUE)) {
            Connection conn = duckDbAdapter.connection();
            DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);
            try (AceroAdapter.RegisteredArrowStreams streams = aceroAdapter.exportToDuckDb(
                    child, resolvedUris, null, buildProjection(pq), duckConn)) {
                String duckSql = DuckDbAdapter.buildSelectSql(
                        pq, arrowStreamsFromClause(streams.aliases()));
                duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
            }
        }
    }

    /**
     * Builds a DuckDB FROM clause over registered Arrow stream aliases.
     *
     * @param aliases registered DuckDB Arrow stream aliases
     * @return FROM clause that unions all streams
     */
    static String arrowStreamsFromClause(List<String> aliases) {
        if (aliases.size() == 1) {
            return DuckDbAdapter.quoteIdentifier(aliases.get(0));
        }
        StringBuilder result = new StringBuilder("(");
        for (int i = 0; i < aliases.size(); i++) {
            if (i > 0) {
                result.append(" UNION ALL ");
            }
            result.append("SELECT * FROM ")
                    .append(DuckDbAdapter.quoteIdentifier(aliases.get(i)));
        }
        return result.append(')').toString();
    }

    /**
     * Executes aggregation queries via DuckDB or footer-stats fast paths.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param parquetFiles  resolved Parquet file paths
     * @param resolvedUris  resolved file URIs
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    private void executeAggregation(BufferAllocator allocator, ParquetQueryParser pq,
            List<Path> parquetFiles, List<String> resolvedUris, String[] fileUris,
            FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long t = LogUtil.mark();
        long aggStartNanos = System.nanoTime();

        if (parquetFiles.isEmpty()) {
            emitRowsAsArrow(allocator, pq, Collections.emptyList(), listener, startListener);
            return;
        }

        boolean noGroupByNoFilter = pq.groupByColumnNames.isEmpty()
                && (pq.filter == null || pq.filter.isBlank())
                && !pq.selectExprs.isEmpty();

        // Fast path A: COUNT(*) only — read row counts from Parquet footer
        if (noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(
                        e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)) {
            List<Future<Long>> futs = new ArrayList<>(parquetFiles.size());
            for (Path file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerRowCount(
                        parquetAdapter.fileSystem(), file)));
            }
            long total = 0;
            for (Future<Long> f : futs) {
                total += f.get();
            }
            LogUtil.logTiming(t, "engine:agg.footerCount", "files=" + parquetFiles.size() + " total=" + total);
            LOGGER.info("qid={} node={} execution=engine engine=footer-count files={} total={} elapsed={}",
                    LogUtil.qid(), LogUtil.node(), parquetFiles.size(), total,
                    LogUtil.elapsedNanos(aggStartNanos));
            int n = pq.selectExprs.size();
            Object[] row = new Object[n];
            Arrays.fill(row, total);
            emitRowsAsArrow(allocator, pq, Collections.singletonList(row), listener, startListener);
            return;
        }

        // Fast path B: MIN/MAX/COUNT(col) from Parquet footer statistics
        boolean statsEligible = noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)
                && pq.selectExprs.stream().anyMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT);
        if (statsEligible) {
            List<Future<Optional<Object[]>>> futs = new ArrayList<>(parquetFiles.size());
            for (Path file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerStats(
                        parquetAdapter.fileSystem(), file, pq)));
            }
            Object[] merged = null;
            boolean allHaveStats = true;
            for (Future<Optional<Object[]>> f : futs) {
                Optional<Object[]> opt = f.get();
                if (opt.isEmpty()) {
                    allHaveStats = false;
                    break;
                }
                if (merged == null) {
                    merged = opt.get().clone();
                } else {
                    mergeAggCols(pq.selectExprs, merged, opt.get(), 0);
                }
            }
            if (allHaveStats) {
                LogUtil.logTiming(t, "engine:agg.footerStats", "files=" + parquetFiles.size());
                LOGGER.debug("qid={} node={} execution=engine engine=footer-stats files={} elapsed={}",
                        LogUtil.qid(), LogUtil.node(), parquetFiles.size(),
                        LogUtil.elapsedNanos(aggStartNanos));
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                emitRowsAsArrow(allocator, pq, rows, listener, startListener);
                return;
            }
            LogUtil.logTiming(t, "engine:agg.footerStatsFallback", "files=" + parquetFiles.size());
            LOGGER.info("qid={} node={} execution=engine engine=DuckDB(fallback) reason=statsMissing files={} elapsed={}",
                    LogUtil.qid(), LogUtil.node(), parquetFiles.size(),
                    LogUtil.elapsedNanos(aggStartNanos));
        }

        if (isHdfsData()) {
            parallelAggregate(allocator, pq, fileUris, listener, startListener);
            return;
        }

        String duckSql = DuckDbAdapter.buildDuckSqlWithFilter(pq,
                DuckDbAdapter.readParquetFromClause(ducksDbPaths(resolvedUris)), false);
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    // ── DuckDB join execution ────────────────────────────────────────────

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
    private void executeJoin(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long t = LogUtil.mark();
        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
        List<AceroAdapter.RegisteredArrowStreams> registeredStreams = new ArrayList<>();
        try (BufferAllocator child = isHdfsData()
                ? allocator.newChildAllocator("hdfs-join", 0, Long.MAX_VALUE) : null) {
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

                for (int tableIndex = 0; tableIndex < pq.joinTables.size(); tableIndex++) {
                    ParquetQueryParser.JoinTable jt = pq.joinTables.get(tableIndex);
                    String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                    List<String> tableUris = tableFiles.get(key);
                    if (tableUris.isEmpty()) {
                        throw new IOException("No Parquet files found for table: " + key);
                    }
                    long tView = LogUtil.mark();
                    String fromClause;
                    if (isHdfsData()) {
                        DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);
                        AceroAdapter.RegisteredArrowStreams streams =
                                aceroAdapter.exportToDuckDb(
                                        child, tableUris, null, Optional.empty(), duckConn,
                                        "join" + tableIndex + "_");
                        registeredStreams.add(streams);
                        fromClause = arrowStreamsFromClause(streams.aliases());
                    } else {
                        fromClause = DuckDbAdapter.readParquetFromClause(tableUris);
                    }
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("CREATE OR REPLACE TEMP VIEW "
                                + DuckDbAdapter.quoteIdentifier(jt.alias())
                                + " AS SELECT * FROM " + fromClause);
                    }
                    LogUtil.logTiming(tView, "engine:join.createView", "alias=" + jt.alias() + " files=" + tableUris.size());
                    registeredAliases.add(jt.alias());
                }

                duckDbAdapter.streamSql(allocator, pq.duckDbSql, listener, startListener);
            } finally {
                long tDrop = LogUtil.mark();
                try (Statement stmt = conn.createStatement()) {
                    for (String alias : registeredAliases) {
                        try {
                            stmt.execute("DROP VIEW IF EXISTS "
                                    + DuckDbAdapter.quoteIdentifier(alias));
                        } catch (Exception ignored) {
                        }
                    }
                }
                LogUtil.logTiming(tDrop, "engine:join.dropViews", "aliases=" + registeredAliases.size());
                for (int i = registeredStreams.size() - 1; i >= 0; i--) {
                    registeredStreams.get(i).close();
                }
            }
        }
        LogUtil.logTiming(t, "engine:join", "tables=" + pq.joinTables.size());
    }

    // ── parallel aggregation ──────────────────────────────────────────────

    /**
     * Runs aggregation in parallel across files using Acero + DuckDB.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void parallelAggregate(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

        List<String> parquetUris = resolveUris(fileUris);
        boolean hasFilter = pq.filter != null && !pq.filter.isBlank();

        boolean isCountStarOnly = pq.groupByColumnNames.isEmpty()
                && !pq.selectExprs.isEmpty()
                && pq.selectExprs.stream()
                        .allMatch(e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR);

        if (isCountStarOnly) {
            byte[] filterBytes = filterBuilder.apply(pq);
            Optional<String[]> cols = buildProjection(pq);
            if (!hasFilter || filterBytes != null) {
                int numCountStarCols = pq.selectExprs.size();
                List<Future<List<Object[]>>> futures = new ArrayList<>(parquetUris.size());
                for (String uri : parquetUris) {
                    futures.add(ioPool.submit(() ->
                            aceroAdapter.aggregateFile(allocator, uri, filterBytes, cols, numCountStarCols)));
                }
                List<Object[]> merged = mergePartialRows(
                        pq.selectExprs, pq.groupByColumnNames, futures);
                emitRowsAsArrow(allocator, pq, merged, listener, startListener);
                return;
            }
        }

        // DuckDB path: Acero scans → Arrow C streams → DuckDB aggregates
        int numGroups = Math.min(duckDbAdapter.duckDbGroups(), parquetUris.size());
        List<List<String>> groups = partitionIntoGroups(parquetUris, numGroups);
        byte[] filterBytes = filterBuilder.apply(pq);
        Optional<String[]> cols = buildProjection(pq);

        List<Future<VectorSchemaRoot>> vsrFutures = new ArrayList<>(groups.size());
        for (List<String> group : groups) {
            String duckSql = DuckDbAdapter.buildGroupedDuckSql(
                    pq, group.size(), filterBytes != null);
            vsrFutures.add(ioPool.submit(() -> {
                BufferAllocator child = allocator.newChildAllocator("par-agg", 0, Long.MAX_VALUE);
                try {
                    Connection conn = duckDbAdapter.connection();
                    DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);
                    try (AceroAdapter.RegisteredArrowStreams streams =
                                    aceroAdapter.exportToDuckDb(
                                            child, group, filterBytes, cols, duckConn);
                            Statement stmt = conn.createStatement();
                            org.duckdb.DuckDBResultSet drs =
                                    (org.duckdb.DuckDBResultSet) stmt.executeQuery(duckSql);
                            ArrowReader arrowReader = (ArrowReader) drs.arrowExportStream(
                                    allocator, duckDbAdapter.batchSize())) {
                        return AceroAdapter.concatBatches(allocator, arrowReader);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    child.close();
                }
            }));
        }
        List<VectorSchemaRoot> partials = new ArrayList<>(vsrFutures.size());
        try {
            for (Future<VectorSchemaRoot> f : vsrFutures) {
                partials.add(f.get());
            }
            try (VectorSchemaRoot merged = mergeVsrPartials(allocator, pq, partials)) {
                if (startListener) {
                    listener.start(merged);
                }
                if (merged.getRowCount() > 0) {
                    DuckDbAdapter.awaitListenerReady(
                            listener, appConfig.flightListenerReadyTimeoutMillis());
                    listener.putNext();
                }
            }
        } finally {
            partials.forEach(VectorSchemaRoot::close);
        }
    }

    // ── private helpers ──────────────────────────────────────────────────

    /**
     * Resolves relative file URIs to absolute Parquet paths.
     *
     * @param pq       parsed query
     * @param fileUris relative file paths
     * @return resolved Parquet paths
     * @throws IOException on HDFS read failure
     */
    private List<Path> resolveParquetFiles(ParquetQueryParser pq, String[] fileUris)
            throws IOException {
        long t = LogUtil.mark();
        List<Path> files = new ArrayList<>();
        for (String uri : fileUris) {
            files.add(new Path(parquetAdapter.dataDirectory(), uri));
        }
        LogUtil.logTiming(t, "files.resolvePaths", "files=" + files.size());
        return files;
    }

    /**
     * Resolves relative file URIs for the native execution engines.
     *
     * @param fileUris relative file paths
     * @return resolved URIs
     * @throws IOException on HDFS read failure
     */
    private List<String> resolveUris(String[] fileUris) throws IOException {
        long t = LogUtil.mark();
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            org.apache.hadoop.fs.FileStatus status = parquetAdapter.fileSystem()
                    .getFileStatus(new Path(parquetAdapter.dataDirectory(), rel));
            uris.add(resolveEngineUri(status));
        }
        LogUtil.logTiming(t, "files.resolveUrisDetail", "files=" + uris.size());
        return uris;
    }

    private String resolveEngineUri(org.apache.hadoop.fs.FileStatus status) throws IOException {
        return engineUri(status);
    }

    /**
     * Returns the fully qualified URI exposed by Hadoop for a data file.
     *
     * @param status Hadoop file status
     * @return URI accepted by Arrow Dataset
     */
    static String engineUri(org.apache.hadoop.fs.FileStatus status) {
        return status.getPath().toUri().toString();
    }

    /**
     * Strips file: scheme from URIs for DuckDB consumption.
     *
     * @param uris fully-qualified URIs
     * @return DuckDB-compatible paths
     */
    static List<String> ducksDbPaths(List<String> uris) {
        return uris.stream().map(u -> {
            if (u.startsWith("file:")) {
                int colon = u.indexOf(':');
                // file:///path → /path,  file:/path → /path
                String stripped = u.substring(colon + 1);
                while (stripped.startsWith("/")) {
                    stripped = stripped.substring(1);
                }
                return "/" + stripped;
            }
            return u;
        }).toList();
    }

    /**
     * Recursively lists Parquet files for a schema.table key.
     *
     * @param key schema.table or table name
     * @return DuckDB-compatible file paths
     * @throws IOException on HDFS read failure
     */
    private List<String> resolveTableFiles(String key) throws IOException {
        long t = LogUtil.mark();
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
                        ? engineUri(f)
                        : plainDuckDbPath(f.getPath()));
            }
        }
        LogUtil.logTiming(t, "engine:join.resolveTableFiles", "table=" + key + " files=" + uris.size());
        return uris;
    }

    /**
     * Converts Hadoop path to DuckDB-quoted path string.
     *
     * @param p Hadoop path
     * @return quoted DuckDB path
     */
    static String duckDbPath(org.apache.hadoop.fs.Path p) {
        java.net.URI uri = p.toUri();
        String path = "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
        return "'" + path.replace("'", "''") + "'";
    }

    /**
     * Converts Hadoop path to unquoted DuckDB path string.
     *
     * @param p Hadoop path
     * @return unquoted DuckDB path
     */
    static String plainDuckDbPath(org.apache.hadoop.fs.Path p) {
        java.net.URI uri = p.toUri();
        return "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
    }

    /**
     * Builds column projection array from parsed query, including filter columns.
     *
     * @param pq parsed query
     * @return projected column names, empty if none needed
     */
    private Optional<String[]> buildProjection(ParquetQueryParser pq) {
        java.util.Set<String> scanCols = projectedColumns(pq);
        if (scanCols.isEmpty()) {
            org.apache.arrow.vector.types.pojo.Schema tSchema =
                    parquetAdapter.getTableSchema(pq.schema, pq.table);
            if (!tSchema.getFields().isEmpty()) {
                scanCols.add(tSchema.getFields().get(0).getName());
            }
        }
        return scanCols.isEmpty() ? Optional.empty()
                : Optional.of(scanCols.toArray(new String[0]));
    }

    /**
     * Collects physical columns needed by the projection, aggregation, and filter.
     *
     * @param pq parsed query
     * @return columns that must be present in the Acero stream
     */
    static java.util.Set<String> projectedColumns(ParquetQueryParser pq) {
        java.util.Set<String> scanCols = new java.util.LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr e : pq.selectExprs) {
            scanCols.addAll(e.inputColumns);
        }
        if (pq.filter != null && !pq.filter.isBlank()) {
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("\"([^\"]+)\"").matcher(pq.filter);
            while (m.find()) {
                scanCols.add(m.group(1));
            }
        }
        return scanCols;
    }

    /**
     * Builds Substrait filter bytes from parsed query.
     *
     * @param pq parsed query
     * @return filter bytes, may be null
     */
    private byte[] buildFilterBytes(ParquetQueryParser pq) {
        return filterBuilder.apply(pq);
    }

    // ── emit ─────────────────────────────────────────────────────────────

    /**
     * Emits aggregation result rows as Arrow batches through the listener.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param rows          result rows
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws InterruptedException if interrupted during send
     */
    private void emitRowsAsArrow(BufferAllocator allocator, ParquetQueryParser pq,
            List<Object[]> rows, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws InterruptedException {
        long t = LogUtil.mark();
        Schema aggSchema = pq.selectExprs.isEmpty()
                ? metadataService.getQuerySchema(buildSelectExprQuery(pq))
                : metadataService.buildAggregationSchema(pq);
        int numGbCols = pq.groupByColumnNames.size();

        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : aggSchema.getFields()) {
            FieldVector v = field.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(rows.size());
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(rows.size() * 16);
            }
            vectors.add(v);
        }

        try (VectorSchemaRoot root = new VectorSchemaRoot(aggSchema.getFields(), vectors)) {
            root.setRowCount(rows.size());
            int vecIdx = 0;
            int aggIdx = 0;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                FieldVector vec = vectors.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[gbPos]);
                    }
                } else {
                    int pos = numGbCols + aggIdx++;
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[pos]);
                    }
                }
            }
            if (startListener) {
                listener.start(root);
            }
            if (!rows.isEmpty()) {
                DuckDbAdapter.awaitListenerReady(
                        listener, appConfig.flightListenerReadyTimeoutMillis());
                listener.putNext();
            }
        }
        LogUtil.logTiming(t, "engine:agg.emitRows", "rows=" + rows.size());
    }

    /**
     * Builds a SELECT * query for schema inference.
     *
     * @param pq parsed query
     * @return SELECT * query string
     */
    private String buildSelectExprQuery(ParquetQueryParser pq) {
        if (pq.schema != null && pq.table != null) {
            return "SELECT * FROM " + pq.schema + "." + pq.table;
        }
        throw new IllegalArgumentException("Cannot build schema query: missing schema/table");
    }

    /**
     * Merges partial aggregation results from parallel file groups.
     *
     * @param allocator Arrow buffer allocator
     * @param pq        parsed query
     * @param partials  partial VSRs from each group
     * @return merged VectorSchemaRoot
     */
    private VectorSchemaRoot mergeVsrPartials(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials) {
        if (pq.groupByColumnNames.isEmpty()) {
            return mergeWithoutGroupBy(allocator, pq, partials);
        }
        return mergeWithGroupBy(allocator, pq, partials);
    }

    private VectorSchemaRoot mergeWithoutGroupBy(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials) {
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
        Long[] longAccum = new Long[exprs.size()];
        Object[] sumAccum = new Object[exprs.size()];
        Object[] objAccum = new Object[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            longAccum[i] = 0L;
        }
        boolean any = false;

        for (VectorSchemaRoot partial : partials) {
            if (partial.getRowCount() == 0) {
                continue;
            }
            any = true;
            int col = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector vec = partial.getVector(col);
                if (!vec.isNull(0)) {
                    switch (expr.func) {
                        case COUNT_STAR, COUNT -> longAccum[col] = (Long) addLongs(longAccum[col], toLong(vec, 0));
                        case SUM -> sumAccum[col] = addNumbers(
                                sumAccum[col], vec.getObject(0));
                        case MIN -> objAccum[col] = objAccum[col] == null
                                ? vec.getObject(0) : minOf(objAccum[col], vec.getObject(0));
                        case MAX -> objAccum[col] = objAccum[col] == null
                                ? vec.getObject(0) : maxOf(objAccum[col], vec.getObject(0));
                        default -> { }
                    }
                }
                col++;
            }
        }

        Schema outSchema = aggregationSchema(pq);
        List<FieldVector> outVecs = createOutputVectors(allocator, outSchema, 1);
        if (any) {
            int col = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector v = outVecs.get(col);
                switch (expr.func) {
                    case COUNT_STAR, COUNT -> {
                        if (longAccum[col] != null) {
                            ((BigIntVector) v).setSafe(0, longAccum[col]);
                        }
                    }
                    case SUM -> {
                        if (sumAccum[col] != null) {
                            setVectorValue(v, 0, sumAccum[col]);
                        }
                    }
                    case MIN, MAX -> setVectorValue(v, 0, objAccum[col]);
                    default -> { }
                }
                col++;
            }
        }
        VectorSchemaRoot r = new VectorSchemaRoot(outSchema.getFields(), outVecs);
        r.setRowCount(any ? 1 : 0);
        return r;
    }

    private VectorSchemaRoot mergeWithGroupBy(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials) {
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
        int numGbCols = pq.groupByColumnNames.size();
        int numAggExprs = (int) exprs.stream()
                .filter(e -> e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN).count();
        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();

        for (VectorSchemaRoot partial : partials) {
            Schema partialSchema = partial.getSchema();
            List<Field> partialFields = partialSchema.getFields();

            Map<String, Integer> colIndexByName = new LinkedHashMap<>();
            for (int i = 0; i < partialFields.size(); i++) {
                colIndexByName.put(partialFields.get(i).getName()
                        .toLowerCase(java.util.Locale.ROOT), i);
            }

            int rowCount = partial.getRowCount();
            for (int r = 0; r < rowCount; r++) {
                List<Object> key = new ArrayList<>(numGbCols);
                for (int c = 0; c < numGbCols; c++) {
                    FieldVector gv = numGbCols > c && c < partialFields.size()
                            ? partial.getVector(c) : null;
                    key.add(gv != null ? gv.getObject(r) : null);
                }
                Object[] accum = byKey.computeIfAbsent(key, k -> new Object[numAggExprs]);
                int ai = 0;
                for (ParquetQueryParser.SelectExpr expr : exprs) {
                    if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                        continue;
                    }
                    int colPos = numGbCols + ai;
                    String expectedName = expr.outputName != null ? expr.outputName
                            : expr.inputColumn;
                    Integer namedIdx = expectedName != null
                            ? colIndexByName.get(expectedName
                                    .toLowerCase(java.util.Locale.ROOT))
                            : null;
                    FieldVector vec = namedIdx != null
                            ? partial.getVector(namedIdx)
                            : (colPos < partialFields.size() ? partial.getVector(colPos) : null);
                    if (vec != null && !vec.isNull(r)) {
                        Object val = vec.getObject(r);
                        switch (expr.func) {
                            case COUNT_STAR, COUNT -> accum[ai] = addLongs(accum[ai], val);
                            case SUM -> accum[ai] = addNumbers(accum[ai], val);
                            case MIN -> accum[ai] = accum[ai] == null ? val : minOf(accum[ai], val);
                            case MAX -> accum[ai] = accum[ai] == null ? val : maxOf(accum[ai], val);
                            default -> { }
                        }
                    }
                    ai++;
                }
            }
        }

        Schema outSchema = aggregationSchema(pq);
        int totalRows = byKey.size();
        List<FieldVector> outVecs = createOutputVectors(allocator, outSchema, totalRows);
        int row = 0;
        for (Map.Entry<List<Object>, Object[]> entry : byKey.entrySet()) {
            List<Object> key = entry.getKey();
            Object[] accum = entry.getValue();
            int vecIdx = 0;
            int ai = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector v = outVecs.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    setVectorValue(v, row, key.get(gbPos));
                } else {
                    setVectorValue(v, row, accum[ai++]);
                }
            }
            row++;
        }
        for (FieldVector v : outVecs) {
            v.setValueCount(totalRows);
        }
        VectorSchemaRoot result = new VectorSchemaRoot(outSchema.getFields(), outVecs);
        result.setRowCount(totalRows);
        return result;
    }

    private Schema aggregationSchema(ParquetQueryParser pq) {
        if (pq.selectExprs.isEmpty()) {
            return metadataService.getQuerySchema(buildSelectExprQuery(pq));
        }
        return metadataService.buildAggregationSchema(pq);
    }

    private static List<FieldVector> createOutputVectors(BufferAllocator allocator,
            Schema outSchema, int totalRows) {
        List<FieldVector> outVecs = new ArrayList<>();
        for (Field f : outSchema.getFields()) {
            FieldVector v = f.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(Math.max(totalRows, 1));
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(Math.max(totalRows, 1) * 16);
            }
            outVecs.add(v);
        }
        return outVecs;
    }

    /**
     * Merges partial aggregation rows from parallel file scans.
     *
     * @param exprs            select expressions
     * @param groupByColumnNames group-by columns
     * @param futures          partial row futures
     * @return merged rows
     * @throws Exception on future resolution or merge failure
     */
    static List<Object[]> mergePartialRows(
            List<ParquetQueryParser.SelectExpr> exprs,
            List<String> groupByColumnNames,
            List<Future<List<Object[]>>> futures) throws Exception {

        int numGbCols = groupByColumnNames.size();
        if (numGbCols == 0) {
            Object[] merged = null;
            for (Future<List<Object[]>> f : futures) {
                List<Object[]> rows = f.get();
                if (rows.isEmpty()) {
                    continue;
                }
                Object[] row = rows.get(0);
                if (merged == null) {
                    merged = row.clone();
                } else {
                    mergeAggCols(exprs, merged, row, 0);
                }
            }
            return merged != null ? Collections.singletonList(merged) : Collections.emptyList();
        }

        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();
        for (Future<List<Object[]>> f : futures) {
            for (Object[] row : f.get()) {
                List<Object> key = new ArrayList<>(Arrays.asList(row).subList(0, numGbCols));
                Object[] existing = byKey.get(key);
                if (existing == null) {
                    byKey.put(key, row.clone());
                } else {
                    mergeAggCols(exprs, existing, row, numGbCols);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /**
     * Merges aggregation columns from one row into another.
     *
     * @param exprs     select expressions
     * @param into      target accumulators
     * @param from      source row
     * @param numGbCols number of group-by columns
     */
    static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                continue;
            }
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = addLongs(into[pos], from[pos]);
                case SUM -> into[pos] = addNumbers(into[pos], from[pos]);
                case MIN -> into[pos] = minOf(into[pos], from[pos]);
                case MAX -> into[pos] = maxOf(into[pos], from[pos]);
                default -> { }
            }
        }
    }

    /**
     * Extracts a long value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return long value
     */
    static long toLong(FieldVector vec, int index) {
        if (vec instanceof BigIntVector v) {
            return v.get(index);
        }
        if (vec instanceof IntVector v) {
            return v.get(index);
        }
        if (vec instanceof SmallIntVector v) {
            return v.get(index);
        }
        if (vec instanceof TinyIntVector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).longValue();
    }

    /**
     * Extracts a double value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return double value
     */
    static double toDouble(FieldVector vec, int index) {
        if (vec instanceof Float8Vector v) {
            return v.get(index);
        }
        if (vec instanceof Float4Vector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).doubleValue();
    }

    /**
     * Returns the minimum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return minimum value
     */
    @SuppressWarnings("unchecked")
    static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    /**
     * Returns the maximum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return maximum value
     */
    @SuppressWarnings("unchecked")
    static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    /**
     * Adds two long accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Long
     */
    static Object addLongs(Object a, Object b) {
        if (a == null) {
            return b == null ? 0L : ((Number) b).longValue();
        }
        if (b == null) {
            return ((Number) a).longValue();
        }
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    /**
     * Adds two double accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Double
     */
    static Object addDoubles(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : ((Number) b).doubleValue();
        }
        if (b == null) {
            return ((Number) a).doubleValue();
        }
        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
    }

    /**
     * Adds numeric aggregate values while preserving decimal precision.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return decimal sum when either value is decimal, otherwise a double sum
     */
    static Object addNumbers(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : b;
        }
        if (b == null) {
            return a;
        }
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            return toBigDecimal(a).add(toBigDecimal(b));
        }
        return addDoubles(a, b);
    }

    /**
     * Converts a numeric aggregate value to BigDecimal without losing integral precision.
     *
     * @param value numeric value
     * @return decimal representation
     */
    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    /**
     * Sets a value in a FieldVector at the given index, handling all supported types.
     *
     * @param vec   field vector
     * @param index row index
     * @param value value to set
     */
    @SuppressWarnings("unchecked")
    static void setVectorValue(FieldVector vec, int index, Object value) {
        if (value == null) {
            vec.setNull(index);
            return;
        }
        if (vec instanceof BigIntVector) {
            ((BigIntVector) vec).setSafe(index, ((Number) value).longValue());
        } else if (vec instanceof IntVector) {
            ((IntVector) vec).setSafe(index, ((Number) value).intValue());
        } else if (vec instanceof SmallIntVector) {
            ((SmallIntVector) vec).setSafe(index, ((Number) value).shortValue());
        } else if (vec instanceof TinyIntVector) {
            ((TinyIntVector) vec).setSafe(index, ((Number) value).byteValue());
        } else if (vec instanceof DecimalVector) {
            ((DecimalVector) vec).setSafe(index, toBigDecimal(value));
        } else if (vec instanceof Float8Vector) {
            ((Float8Vector) vec).setSafe(index, ((Number) value).doubleValue());
        } else if (vec instanceof Float4Vector) {
            ((Float4Vector) vec).setSafe(index, ((Number) value).floatValue());
        } else if (vec instanceof BitVector) {
            ((BitVector) vec).setSafe(index, ((Boolean) value) ? 1 : 0);
        } else if (vec instanceof VarCharVector) {
            byte[] bytes = value instanceof Text
                    ? ((Text) value).getBytes()
                    : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(index, bytes);
        }
    }

    /**
     * Partitions items into numGroups using round-robin distribution.
     *
     * @param items     items to partition
     * @param numGroups number of groups
     * @return partitioned lists
     */
    static <T> List<List<T>> partitionIntoGroups(List<T> items, int numGroups) {
        List<List<T>> groups = new ArrayList<>(numGroups);
        for (int i = 0; i < numGroups; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            groups.get(i % numGroups).add(items.get(i));
        }
        return groups;
    }
}
