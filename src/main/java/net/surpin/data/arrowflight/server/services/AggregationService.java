package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.adapters.AceroAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.MetadataService;

/**
 * Executes aggregation queries across DuckDB and Acero engines.
 * Supports footer-stat fast paths, parallel aggregation, and DuckDB fallback.
 */
public final class AggregationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationService.class);

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final AceroAdapter aceroAdapter;
    private final MetadataService metadataService;
    private final AppConfig appConfig;
    private final ExecutorService ioPool;
    private final Function<ParquetQueryParser, byte[]> filterBuilder;

    /**
     * Creates AggregationService.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param duckDbAdapter  DuckDB adapter
     * @param aceroAdapter   Acero adapter
     * @param metadataService schema/metadata service
     * @param appConfig      server configuration
     * @param ioPool         shared I/O thread pool
     * @param filterBuilder  converts parsed query to Substrait filter bytes, may return null
     */
    public AggregationService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            AceroAdapter aceroAdapter, MetadataService metadataService,
            AppConfig appConfig,
            ExecutorService ioPool,
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
    public void executeAggregation(BufferAllocator allocator, ParquetQueryParser pq,
            List<Path> parquetFiles, List<String> resolvedUris, String[] fileUris,
            FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long aggStartNanos = System.nanoTime();

        if (parquetFiles.isEmpty()) {
            AggregationResultMerger.emitRowsAsArrow(allocator, pq, Collections.emptyList(),
                    listener, startListener, metadataService, appConfig);
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
            LOGGER.info("qid={} node={} execution=engine engine=footer-count files={} total={} elapsed={}",
                    LogUtil.qid(), LogUtil.node(), parquetFiles.size(), total,
                    LogUtil.elapsedNanos(aggStartNanos));
            int n = pq.selectExprs.size();
            Object[] row = new Object[n];
            Arrays.fill(row, total);
            AggregationResultMerger.emitRowsAsArrow(allocator, pq,
                    Collections.singletonList(row), listener, startListener,
                    metadataService, appConfig);
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
                    AggregationResultMerger.mergeAggCols(pq.selectExprs, merged, opt.get(), 0);
                }
            }
            if (allHaveStats) {
                LOGGER.debug("qid={} node={} execution=engine engine=footer-stats files={} elapsed={}",
                        LogUtil.qid(), LogUtil.node(), parquetFiles.size(),
                        LogUtil.elapsedNanos(aggStartNanos));
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                AggregationResultMerger.emitRowsAsArrow(allocator, pq, rows,
                        listener, startListener, metadataService, appConfig);
                return;
            }
            LOGGER.info("qid={} node={} execution=engine engine=DuckDB(fallback) reason=statsMissing files={} elapsed={}",
                    LogUtil.qid(), LogUtil.node(), parquetFiles.size(),
                    LogUtil.elapsedNanos(aggStartNanos));
        }

        if (isHdfsData()) {
            parallelAggregate(allocator, pq, resolvedUris, listener, startListener);
            return;
        }

        String duckSql = DuckDbAdapter.buildDuckSqlWithFilter(pq,
                DuckDbAdapter.readParquetFromClause(PathUtils.ducksDbPaths(resolvedUris)), false);
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    /**
     * Runs aggregation in parallel across files using Acero + DuckDB.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param resolvedUris  resolved file URIs
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void parallelAggregate(BufferAllocator allocator, ParquetQueryParser pq,
            List<String> resolvedUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

        boolean hasFilter = pq.filter != null && !pq.filter.isBlank();

        boolean isCountStarOnly = pq.groupByColumnNames.isEmpty()
                && !pq.selectExprs.isEmpty()
                && pq.selectExprs.stream()
                        .allMatch(e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR);

        if (isCountStarOnly) {
            byte[] filterBytes = filterBuilder.apply(pq);
            Optional<String[]> cols = ProjectionHelper.buildProjection(pq, parquetAdapter);
            if (!hasFilter || filterBytes != null) {
                int numCountStarCols = pq.selectExprs.size();
                List<Future<List<Object[]>>> futures = new ArrayList<>(resolvedUris.size());
                for (String uri : resolvedUris) {
                    futures.add(ioPool.submit(() ->
                            aceroAdapter.aggregateFile(allocator, uri, filterBytes, cols, numCountStarCols)));
                }
                List<Object[]> merged = AggregationResultMerger.mergePartialRows(
                        pq.selectExprs, pq.groupByColumnNames, futures);
                AggregationResultMerger.emitRowsAsArrow(allocator, pq, merged,
                        listener, startListener, metadataService, appConfig);
                return;
            }
        }

        // DuckDB path: Acero scans -> Arrow C streams -> DuckDB aggregates
        int numGroups = Math.min(duckDbAdapter.duckDbGroups(), resolvedUris.size());
        List<List<String>> groups = VectorUtils.partitionIntoGroups(resolvedUris, numGroups);
        byte[] filterBytes = filterBuilder.apply(pq);
        Optional<String[]> cols = ProjectionHelper.buildProjection(pq, parquetAdapter);

        List<Future<VectorSchemaRoot>> vsrFutures = new ArrayList<>(groups.size());
        for (List<String> group : groups) {
            String duckSql = DuckDbAdapter.buildGroupedDuckSql(
                    pq, group.size(), filterBytes != null);
            vsrFutures.add(ioPool.submit(() -> {
                BufferAllocator child = allocator.newChildAllocator("par-agg", 0, Long.MAX_VALUE);
                try {
                    Connection conn = duckDbAdapter.connection();
                    org.duckdb.DuckDBConnection duckConn = conn.unwrap(org.duckdb.DuckDBConnection.class);
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
            try (VectorSchemaRoot merged = AggregationResultMerger.mergeVsrPartials(
                    allocator, pq, partials, metadataService)) {
                if (startListener) {
                    listener.start(merged);
                }
                if (merged.getRowCount() > 0) {
                    if (DuckDbAdapter.awaitListenerReady(
                            listener, appConfig.flightListenerReadyTimeoutMillis())) {
                        listener.putNext();
                    }
                }
            }
        } finally {
            partials.forEach(VectorSchemaRoot::close);
        }
    }

    private boolean isHdfsData() {
        return "hdfs".equalsIgnoreCase(parquetAdapter.fileSystem().getUri().getScheme());
    }
}
