package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import net.surpin.data.arrowflight.server.adapters.AceroAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;

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
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);

        if (parsedQuery.isJoin) {
            executeJoin(allocator, parsedQuery, fileUris, listener, startListener);
            return;
        }

        if (fileUris == null) {
            fileUris = parquetAdapter.locationsForQuery(query)
                    .keySet().toArray(new String[0]);
        }

        List<Path> parquetFiles = resolveParquetFiles(parsedQuery, fileUris);
        if (parquetFiles.isEmpty()) {
            LOGGER.warn("No Parquet files to read for query: {}", query);
            return;
        }

        List<String> resolvedUris = resolveUris(fileUris);

        if (parsedQuery.hasAggregation) {
            executeAggregation(allocator, parsedQuery, parquetFiles, resolvedUris, listener, startListener);
            return;
        }

        if (parsedQuery.filter != null && !parsedQuery.filter.isBlank()) {
            String duckSql = DuckDbAdapter.buildSelectSql(parsedQuery,
                    DuckDbAdapter.readParquetFromClause(ducksDbPaths(resolvedUris)));
            duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
            return;
        }

        aceroAdapter.scanBatches(allocator, query, parsedQuery,
                resolvedUris, listener, startListener);
    }

    /**
     * Executes aggregation queries via DuckDB or footer-stats fast paths.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param parquetFiles  resolved Parquet file paths
     * @param resolvedUris  resolved file URIs
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    private void executeAggregation(BufferAllocator allocator, ParquetQueryParser pq,
            List<Path> parquetFiles, List<String> resolvedUris,
            FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

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
            try {
                for (Future<Long> f : futs) {
                    total += f.get(60, TimeUnit.SECONDS);
                }
            } catch (TimeoutException e) {
                LOGGER.error("Timeout reading Parquet footer row counts");
                futs.forEach(f -> f.cancel(true));
                throw new IOException("Timeout reading Parquet footer row counts", e);
            }
            LOGGER.debug("COUNT(*) footer fast-path: {} file(s), total={}", parquetFiles.size(), total);
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
            try {
                for (Future<Optional<Object[]>> f : futs) {
                    Optional<Object[]> opt = f.get(60, TimeUnit.SECONDS);
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
            } catch (TimeoutException e) {
                LOGGER.error("Timeout reading Parquet footer statistics");
                futs.forEach(f -> f.cancel(true));
                throw new IOException("Timeout reading Parquet footer statistics", e);
            }
            if (allHaveStats) {
                LOGGER.debug("MIN/MAX footer stats fast-path: {} file(s)", parquetFiles.size());
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                emitRowsAsArrow(allocator, pq, rows, listener, startListener);
                return;
            }
            LOGGER.debug("MIN/MAX stats missing; falling back to DuckDB");
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

        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
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

            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                List<String> duckDbPaths = tableFiles.get(key);
                if (duckDbPaths.isEmpty()) {
                    throw new IOException("No Parquet files found for table: " + key);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE OR REPLACE TEMP VIEW "
                            + DuckDbAdapter.quoteIdentifier(jt.alias())
                            + " AS SELECT * FROM "
                            + DuckDbAdapter.readParquetFromClause(duckDbPaths));
                }
                registeredAliases.add(jt.alias());
            }

            duckDbAdapter.streamSql(allocator, pq.duckDbSql, listener, startListener);
        } finally {
            try (Statement stmt = conn.createStatement()) {
                for (String alias : registeredAliases) {
                    try {
                        stmt.execute("DROP VIEW IF EXISTS "
                                + DuckDbAdapter.quoteIdentifier(alias));
                    } catch (Exception ignored) {
                    }
                }
            }
        }
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
                    List<String> aliases = aceroAdapter.exportToDuckDb(
                            child, group, filterBytes, cols, duckConn);
                    try (Statement stmt = conn.createStatement()) {
                        org.duckdb.DuckDBResultSet drs =
                                (org.duckdb.DuckDBResultSet) stmt.executeQuery(duckSql);
                        try (ArrowReader arrowReader = (ArrowReader) drs.arrowExportStream(
                                allocator, duckDbAdapter.batchSize())) {
                            return AceroAdapter.concatBatches(allocator, arrowReader);
                        }
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
                    if (DuckDbAdapter.awaitListenerReady(listener)) {
                        listener.putNext();
                    }
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
        List<Path> files = new ArrayList<>();
        for (String uri : fileUris) {
            files.add(new Path(parquetAdapter.dataDirectory(), uri));
        }
        return files;
    }

    /**
     * Resolves relative file URIs to fully-qualified HDFS URIs.
     *
     * @param fileUris relative file paths
     * @return resolved URIs
     * @throws IOException on HDFS read failure
     */
    private List<String> resolveUris(String[] fileUris) throws IOException {
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            uris.add(parquetAdapter.fileSystem()
                    .getFileStatus(new Path(parquetAdapter.dataDirectory(), rel))
                    .getPath().toUri().toString());
        }
        return uris;
    }

    /**
     * Strips file: scheme from URIs for DuckDB consumption.
     *
     * @param uris fully-qualified URIs
     * @return DuckDB-compatible paths
     */
    private static List<String> ducksDbPaths(List<String> uris) {
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
                uris.add(plainDuckDbPath(f.getPath()));
            }
        }
        return uris;
    }

    /**
     * Converts Hadoop path to DuckDB-quoted path string.
     *
     * @param p Hadoop path
     * @return quoted DuckDB path
     */
    private static String duckDbPath(org.apache.hadoop.fs.Path p) {
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
    private static String plainDuckDbPath(org.apache.hadoop.fs.Path p) {
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
        java.util.Set<String> scanCols = new java.util.LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr e : pq.selectExprs) {
            if (e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && e.func != ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR
                    && e.inputColumn != null) {
                scanCols.add(e.inputColumn);
            }
        }
        if (pq.filter != null && !pq.filter.isBlank()) {
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("\"([^\"]+)\"").matcher(pq.filter);
            while (m.find()) {
                scanCols.add(m.group(1));
            }
        }
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
                if (DuckDbAdapter.awaitListenerReady(listener)) {
                    listener.putNext();
                }
            }
        }
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
        int numGbCols = pq.groupByColumnNames.size();
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;

        if (numGbCols == 0) {
                    Long[] longAccum = new Long[exprs.size()];
            Double[] dblAccum = new Double[exprs.size()];
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
                            case SUM -> dblAccum[col] = (Double) addDoubles(dblAccum[col], toDouble(vec, 0));
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

            Schema outSchema = pq.selectExprs.isEmpty()
                    ? metadataService.getQuerySchema(buildSelectExprQuery(pq))
                    : metadataService.buildAggregationSchema(pq);
            List<FieldVector> outVecs = new ArrayList<>();
            for (Field f : outSchema.getFields()) {
                FieldVector v = f.createVector(allocator);
                if (v instanceof FixedWidthVector fv) {
                    fv.allocateNew(1);
                } else if (v instanceof VariableWidthVector vv) {
                    vv.allocateNew(32);
                }
                outVecs.add(v);
            }
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
                            if (dblAccum[col] != null) {
                                ((Float8Vector) v).setSafe(0, dblAccum[col]);
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

        // GROUP BY path
        int numAggExprs = (int) exprs.stream()
                .filter(e -> e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN).count();
        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();

        for (VectorSchemaRoot partial : partials) {
            Schema partialSchema = partial.getSchema();
            List<Field> partialFields = partialSchema.getFields();

            // Build name→index map for defensive column lookup
            Map<String, Integer> colIndexByName = new LinkedHashMap<>();
            for (int i = 0; i < partialFields.size(); i++) {
                colIndexByName.put(partialFields.get(i).getName().toLowerCase(java.util.Locale.ROOT), i);
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
                            ? colIndexByName.get(expectedName.toLowerCase(java.util.Locale.ROOT))
                            : null;
                    FieldVector vec = namedIdx != null
                            ? partial.getVector(namedIdx)
                            : (colPos < partialFields.size() ? partial.getVector(colPos) : null);
                    if (vec != null && !vec.isNull(r)) {
                        Object val = vec.getObject(r);
                        switch (expr.func) {
                            case COUNT_STAR, COUNT -> accum[ai] = addLongs(accum[ai], val);
                            case SUM -> accum[ai] = addDoubles(accum[ai], val);
                            case MIN -> accum[ai] = accum[ai] == null ? val : minOf(accum[ai], val);
                            case MAX -> accum[ai] = accum[ai] == null ? val : maxOf(accum[ai], val);
                            default -> { }
                        }
                    }
                    ai++;
                }
            }
        }

        Schema outSchema = pq.selectExprs.isEmpty()
                ? metadataService.getQuerySchema(buildSelectExprQuery(pq))
                : metadataService.buildAggregationSchema(pq);
        int totalRows = byKey.size();
        List<FieldVector> outVecs = new ArrayList<>();
        for (Field f : outSchema.getFields()) {
            FieldVector v = f.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(totalRows);
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(totalRows * 16);
            }
            outVecs.add(v);
        }
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

    /**
     * Merges partial aggregation rows from parallel file scans.
     *
     * @param exprs            select expressions
     * @param groupByColumnNames group-by columns
     * @param futures          partial row futures
     * @return merged rows
     * @throws Exception on future resolution or merge failure
     */
    private static List<Object[]> mergePartialRows(
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
    private static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                continue;
            }
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = addLongs(into[pos], from[pos]);
                case SUM -> into[pos] = addDoubles(into[pos], from[pos]);
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
    private static long toLong(FieldVector vec, int index) {
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
    private static double toDouble(FieldVector vec, int index) {
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
     * Returns the maximum of two comparable values, null-safe.
     *
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
     * Adds two long accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Long
     */
    private static Object addLongs(Object a, Object b) {
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
    private static Object addDoubles(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : ((Number) b).doubleValue();
        }
        if (b == null) {
            return ((Number) a).doubleValue();
        }
        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
    }

    /**
     * Sets a value in a FieldVector at the given index, handling all supported types.
     *
     * @param vec   field vector
     * @param index row index
     * @param value value to set
     */
    @SuppressWarnings("unchecked")
    private static void setVectorValue(FieldVector vec, int index, Object value) {
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
    private static <T> List<List<T>> partitionIntoGroups(List<T> items, int numGroups) {
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
