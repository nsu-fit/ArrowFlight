package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import net.surpin.data.arrowflight.server.adapters.AceroAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.LogUtil;
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
    private final AceroFileResolver aceroFileResolver;
    private final Function<ParquetQueryParser, byte[]> filterBuilder;
    private final AggregationService aggregationService;
    private final JoinService joinService;

    /**
     * Creates ExecutionService.
     *
     * @param parquetAdapter     Parquet metadata adapter
     * @param duckDbAdapter      DuckDB adapter
     * @param aceroAdapter       Acero adapter
     * @param metadataService    schema/metadata service
     * @param appConfig          server configuration
     * @param ioPool             shared I/O thread pool
     * @param filterBuilder      converts parsed query to Substrait filter bytes, may return null
     * @param aggregationService aggregation execution service
     * @param joinService        join execution service
     */
    public ExecutionService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            AceroAdapter aceroAdapter, MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool,
            Function<ParquetQueryParser, byte[]> filterBuilder,
            AggregationService aggregationService,
            JoinService joinService) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.aceroAdapter = aceroAdapter;
        this.metadataService = metadataService;
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.aceroFileResolver = new AceroFileResolver(
                parquetAdapter.fileSystem(), new Path(parquetAdapter.dataDirectory()),
                appConfig.localDataDir(), appConfig.ioFileBufferSize());
        this.filterBuilder = filterBuilder;
        this.aggregationService = aggregationService;
        this.joinService = joinService;
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
            joinService.executeJoin(allocator, parsedQuery, fileUris, listener, startListener);
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
        LOGGER.debug("qid={} node={} execution=filesResolved files={} paths={}",
                LogUtil.qid(), LogUtil.node(), resolvedUris.size(), resolvedUris);

        if (parsedQuery.hasAggregation) {
            LOGGER.info("qid={} node={} execution=engine engine=aggregation hasGroupBy={} isHdfs={} files={}",
                    LogUtil.qid(), LogUtil.node(), !parsedQuery.groupByColumnNames.isEmpty(),
                    isHdfsData(), parquetFiles.size());
            aggregationService.executeAggregation(allocator, parsedQuery, parquetFiles, resolvedUris,
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
                    DuckDbAdapter.readParquetFromClause(PathUtils.ducksDbPaths(resolvedUris)));
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
            org.duckdb.DuckDBConnection duckConn = conn.unwrap(org.duckdb.DuckDBConnection.class);
            try (AceroAdapter.RegisteredArrowStreams streams = aceroAdapter.exportToDuckDb(
                    child, resolvedUris, null,
                    ProjectionHelper.buildProjection(pq, parquetAdapter), duckConn)) {
                String duckSql = DuckDbAdapter.buildSelectSql(
                        pq, PathUtils.arrowStreamsFromClause(streams.aliases().size()));
                duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
            }
        }
    }

    private List<Path> resolveParquetFiles(ParquetQueryParser pq, String[] fileUris)
            throws IOException {
        List<Path> files = new ArrayList<>();
        for (String uri : fileUris) {
            files.add(new Path(parquetAdapter.dataDirectory(), uri));
        }
        return files;
    }

    private List<String> resolveUris(String[] fileUris) throws IOException {
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            org.apache.hadoop.fs.FileStatus status = parquetAdapter.fileSystem()
                    .getFileStatus(new Path(parquetAdapter.dataDirectory(), rel));
            uris.add(resolveEngineUri(status));
        }
        return uris;
    }

    private String resolveEngineUri(org.apache.hadoop.fs.FileStatus status) throws IOException {
        if (isHdfsData()) {
            return aceroFileResolver.resolve(status);
        }
        return status.getPath().toUri().toString();
    }
}
