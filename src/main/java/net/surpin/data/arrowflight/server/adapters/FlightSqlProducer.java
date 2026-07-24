package net.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.surpin.data.arrowflight.server.LogUtil;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.CapacityExhaustedException;
import net.surpin.data.arrowflight.server.model.ReservationState;
import net.surpin.data.arrowflight.server.model.ReservationStatus;
import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.services.ClusterService;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import net.surpin.data.arrowflight.server.services.QueryPlanner;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

/**
 * Flight SQL producer that handles metadata and query execution requests.
 */
public final class FlightSqlProducer extends BasicFlightSqlProducer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlProducer.class);

    private final Location location;
    private final BufferAllocator allocator;
    private final MetadataService metadataService;
    private final QueryPlanner queryPlanner;
    private final ExecutionService executionService;
    private final ClusterService clusterService;
    private final long queryMemoryLimit;
    private final ExecutorService queryExecutor;
    private final Map<String, Runnable> cancellations = new ConcurrentHashMap<>();
    private final SqlInfoBuilder sqlInfoBuilder;

    /**
     * @param location server location for endpoint registration
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     */
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService) {
        this(location, allocator, metadataService, queryPlanner, executionService,
                clusterService, Math.min(268_435_456L, allocator.getLimit()), 4);
    }

    /**
     * Creates a producer with explicit per-query Arrow memory and concurrency limits.
     *
     * @param location server location for endpoint registration
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planner
     * @param executionService execution service
     * @param clusterService cluster coordination service
     * @param queryMemoryLimit per-query Arrow limit
     * @param maxConcurrentQueries query executor size
     */
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            long queryMemoryLimit, int maxConcurrentQueries) {
        this.location = location;
        this.allocator = allocator;
        this.metadataService = metadataService;
        this.queryPlanner = queryPlanner;
        this.executionService = executionService;
        this.clusterService = clusterService;
        this.queryMemoryLimit = queryMemoryLimit;
        this.queryExecutor = Executors.newFixedThreadPool(
                maxConcurrentQueries, runnable -> {
                    Thread thread = new Thread(runnable, "query-execution");
                    thread.setDaemon(true);
                    return thread;
                });

        this.sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder
                .withFlightSqlServerName("Hadoop-Arrow-Parquet Source")
                .withFlightSqlServerVersion("0.0.1")
                .withFlightSqlServerArrowVersion("0.0.1")
                .withFlightSqlServerReadOnly(true)
                .withFlightSqlServerSql(true)
                .withFlightSqlServerSubstrait(false)
                .withFlightSqlServerTransaction(
                        FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                .withSqlDdlCatalog(false)
                .withSqlDdlSchema(true)
                .withSqlDdlTable(true)
                .withSqlIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
                .withSqlQuotedIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                .withSqlAllTablesAreSelectable(true)
                .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                .withSqlMaxColumnsInTable(1000);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final ByteString handle = ticket.getStatementHandle();
        String handleValue = handle.toStringUtf8();
        String qid = qid(handle);
        long tGet = LogUtil.mark();
        HandleState state = clusterService.getHandle(handleValue);
        LogUtil.logTiming(tGet, "execution.getHandle");
        if (state == null) {
            LOGGER.error("qid={} No HandleState found", qid);
            listener.error(CallStatus.NOT_FOUND.withDescription(
                    "No HandleState found for qid=" + qid).toRuntimeException());
            return;
        }
        if (state.filePaths() == null) {
            LOGGER.error("qid={} No file paths in handle state", qid);
            listener.error(new IllegalStateException("No file paths for qid=" + qid));
            return;
        }

        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean submitted = new AtomicBoolean();
        AtomicReference<Future<?>> executionFuture = new AtomicReference<>();
        AtomicReference<UUID> reservationListener = new AtomicReference<>();
        Runnable cancellation = () -> {
            if (cancelled.compareAndSet(false, true)) {
                clusterService.removeReservationListener(reservationListener.getAndSet(null));
                Future<?> future = executionFuture.get();
                if (future != null) {
                    future.cancel(true);
                }
                if (state.reservationId() != null) {
                    clusterService.releaseExecution(state.reservationId());
                } else {
                    clusterService.removeHandle(handleValue);
                }
            }
            cancellations.remove(handleValue);
        };
        cancellations.put(handleValue, cancellation);
        listener.setOnCancelHandler(cancellation);
        if (listener.isCancelled()) {
            cancellation.run();
            return;
        }

        Consumer<ReservationState> stateObserver = reservation -> {
            if (reservation == null) {
                if (!cancelled.get() && !submitted.get()) {
                    cancelled.set(true);
                    listener.error(CallStatus.CANCELLED.withDescription(
                            "Execution reservation was released").toRuntimeException());
                    cancellations.remove(handleValue);
                }
                return;
            }
            if (reservation.status() == ReservationStatus.ACTIVE
                    && submitted.compareAndSet(false, true)) {
                MetricsService.recordQueueWaitMillis(Math.max(0L,
                        clusterService.clusterTimeMillis()
                                - reservation.createdAtMillis()));
                clusterService.removeReservationListener(
                        reservationListener.getAndSet(null));
                submitExecution(state, handleValue, qid, listener,
                        cancelled, executionFuture);
            }
        };

        if (state.reservationId() == null) {
            submitted.set(true);
            submitExecution(state, handleValue, qid, listener,
                    cancelled, executionFuture);
            return;
        }
        ReservationState claimed = clusterService.claimExecution(
                handleValue, state.reservationId());
        if (claimed == null) {
            cancellations.remove(handleValue);
            listener.error(CallStatus.NOT_FOUND.withDescription(
                    "Execution handle expired").toRuntimeException());
            return;
        }
        UUID listenerId = clusterService.watchReservation(
                state.reservationId(), stateObserver);
        reservationListener.set(listenerId);
        if (submitted.get() || cancelled.get()) {
            clusterService.removeReservationListener(
                    reservationListener.getAndSet(null));
        }
    }

    /**
     * Submits one active reservation without blocking the Flight worker.
     *
     * @param state execution handle state
     * @param handle handle string
     * @param qid query identifier
     * @param listener Flight stream listener
     * @param cancelled cancellation flag
     * @param executionFuture submitted future reference
     */
    private void submitExecution(
            HandleState state,
            String handle,
            String qid,
            FlightProducer.ServerStreamListener listener,
            AtomicBoolean cancelled,
            AtomicReference<Future<?>> executionFuture) {
        if (cancelled.get()) {
            if (state.reservationId() != null) {
                clusterService.releaseExecution(state.reservationId());
            }
            return;
        }
        try {
            Future<?> future = queryExecutor.submit(
                    () -> runExecution(state, handle, qid, listener, cancelled));
            executionFuture.set(future);
            if (cancelled.get()) {
                future.cancel(true);
            }
        } catch (RejectedExecutionException rejected) {
            cancellations.remove(handle);
            if (state.reservationId() != null) {
                clusterService.releaseExecution(state.reservationId());
            }
            listener.error(CallStatus.UNAVAILABLE.withDescription(
                    "Query executor is shutting down")
                    .withCause(rejected).toRuntimeException());
        }
    }

    /**
     * Executes and terminates one Flight stream on the query executor.
     *
     * @param state execution handle state
     * @param handle handle string
     * @param qid query identifier
     * @param listener Flight stream listener
     * @param cancelled cancellation flag
     */
    private void runExecution(
            HandleState state,
            String handle,
            String qid,
            FlightProducer.ServerStreamListener listener,
            AtomicBoolean cancelled) {
        String query = state.query();
        String[] filePaths = state.filePaths();
        String serverUri = state.serverUri() != null ? state.serverUri() : "local";
        long started = LogUtil.mark();
        long executionStartNanos = System.nanoTime();
        LogUtil.setQid(qid);
        MDC.put("qid", qid);
        MetricsService.QueryObservation observation =
                MetricsService.observeQuery(query, state.bytes());
        BufferAllocator queryAllocator = allocator;
        try {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                return;
            }
            LOGGER.debug("qid={} node={} thread={} execution=start server={} files={} bytes={}",
                    qid, LogUtil.node(), Thread.currentThread().getName(),
                    serverUri, filePaths.length, state.bytes());
            queryAllocator = allocator.newChildAllocator(
                    "query-" + qid, 0, queryMemoryLimit);
            executionService.readParquet(
                    queryAllocator, query, filePaths, listener, true);
            if (!cancelled.get() && !listener.isCancelled()) {
                listener.completed();
            }
            LogUtil.logTiming(started, "execution.total",
                    "files=" + filePaths.length);
        } catch (Exception error) {
            observation.markFailed();
            String failure = failureDescription(error);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - executionStartNanos);
            LOGGER.error("qid={} node={} execution=failed server={} elapsedMs={} error='{}'",
                    qid, LogUtil.node(), serverUri, elapsed, failure, error);
            if (!cancelled.get() && !listener.isCancelled()) {
                reportExecutionError(listener, error, failure);
            }
        } finally {
            if (queryAllocator != allocator) {
                queryAllocator.close();
            }
            observation.close();
            MDC.remove("qid");
            LogUtil.setQid(null);
            cancellations.remove(handle);
            if (state.reservationId() != null) {
                clusterService.releaseExecution(state.reservationId());
            } else {
                clusterService.removeHandle(handle);
            }
        }
    }

    /**
     * Maps execution failures to Flight terminal statuses.
     *
     * @param listener Flight stream listener
     * @param error execution error
     * @param failure bounded failure description
     */
    private static void reportExecutionError(
            FlightProducer.ServerStreamListener listener,
            Exception error,
            String failure) {
        if (isMemoryExhaustion(error)) {
            listener.error(CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("Query memory limit exceeded")
                    .withCause(error).toRuntimeException());
        } else if (error instanceof FlightRuntimeException flightException) {
            listener.error(flightException);
        } else {
            listener.error(CallStatus.INTERNAL
                    .withDescription(failure)
                    .withCause(error)
                    .toRuntimeException());
        }
    }

    /**
     * Recognizes Arrow and DuckDB memory exhaustion through cause classes and messages.
     *
     * @param error failure to inspect
     * @return whether memory was exhausted
     */
    private static boolean isMemoryExhaustion(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            String name = cause.getClass().getName().toLowerCase(java.util.Locale.ROOT);
            String message = cause.getMessage() == null ? ""
                    : cause.getMessage().toLowerCase(java.util.Locale.ROOT);
            if (name.contains("outofmemory")
                    || message.contains("out of memory")
                    || message.contains("memory limit")
                    || message.contains("failed to allocate")
                    || message.contains("allocation failure")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isFileNotFound(Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof java.io.FileNotFoundException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static String failureDescription(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String description = "Query execution failed: " + root.getClass().getSimpleName();
        if (message != null && !message.isBlank()) {
            description += ": " + message;
        }
        return description.length() <= 1024 ? description : description.substring(0, 1024);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        long t = LogUtil.mark();
        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        String query = command.getQuery();
        String qid = qid(handle);
        LogUtil.logTiming(t, "schema.resolveQuery", "qid=" + qid);

        Schema arrowSchema;
        try {
            arrowSchema = metadataService.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error getting Arrow schema for query: {}", query, e);
            if (isFileNotFound(e)) {
                throw CallStatus.NOT_FOUND
                        .withDescription("Could not find Arrow schema for query")
                        .withCause(e).toRuntimeException();
            }
            throw CallStatus.INTERNAL
                    .withDescription("Error getting Arrow schema for query")
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            LOGGER.error("Arrow schema not found for query: {}", query);
            throw CallStatus.NOT_FOUND
                    .withDescription("Could not find Arrow schema for query")
                    .toRuntimeException();
        }

        long tStore = LogUtil.mark();
        clusterService.storeHandle(handle.toStringUtf8(), HandleState.forQuery(query));
        LogUtil.logTiming(tStore, "planning.storeHandle", "qid=" + qid);

        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build();
        return getFlightInfoForSchema(ticket, descriptor, arrowSchema);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String query = command.getQuery();
        LOGGER.debug("getSchemaStatement: {}", query);

        Schema arrowSchema;
        try {
            arrowSchema = metadataService.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error getting Arrow schema for query: {}", query, e);
            throw CallStatus.INTERNAL
                    .withDescription("Error getting Arrow schema for query")
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            throw CallStatus.NOT_FOUND
                    .withDescription("Could not find Arrow schema for query")
                    .toRuntimeException();
        }

        return new SchemaResult(arrowSchema);
    }

    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(
            T request, FlightDescriptor descriptor, Schema schema) {
        long t = LogUtil.mark();
        try {
            if (request instanceof FlightSql.TicketStatementQuery ticketQuery) {
                long tGet = LogUtil.mark();
                final ByteString handle = ticketQuery.getStatementHandle();
                HandleState state = clusterService.getHandle(handle.toStringUtf8());
                LogUtil.logTiming(tGet, "planning.getHandle");
                if (state == null) {
                    throw CallStatus.NOT_FOUND
                            .withDescription("No handle state found")
                            .toRuntimeException();
                }
                String query = state.query();
                List<FlightEndpoint> endpoints = queryPlanner.determineEndpoints(query);
                clusterService.removeHandle(handle.toStringUtf8());
                LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size());
                return endpoints;
            } else {
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(new FlightEndpoint(ticket, location));
            }
        } catch (CapacityExhaustedException exhausted) {
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription(exhausted.getMessage())
                    .withCause(exhausted).toRuntimeException();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ── Metadata handlers ────────────────────────────────────────────────

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        this.sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getCatalogsRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        boolean errored = false;

        try {
            Map<String, org.apache.hadoop.fs.Path> schemas =
                    metadataService.getSchemas(schemaFilterPattern);
            try (VectorSchemaRoot root = metadataService.getSchemasRoot(
                    schemas.keySet(), allocator)) {
                listener.start(root);
                listener.putNext();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamSchemas", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTableTypesRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        Schema schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern = command.hasTableNameFilterPattern()
                ? command.getTableNameFilterPattern() : null;

        if (catalog != null
                && !MetadataService.CATALOG_NAME.equalsIgnoreCase(catalog)) {
            LOGGER.info("Catalog doesn't exist in getStreamTables: {}", catalog);
            throw CallStatus.NOT_FOUND
                    .withDescription("Could not getStreamTables for catalog: " + catalog)
                    .toRuntimeException();
        }

        final ProtocolStringList tableTypesList = command.getTableTypesList();
        if (!tableTypesList.isEmpty()
                && !tableTypesList.stream().allMatch(
                        MetadataService.TABLE_TYPE::equalsIgnoreCase)) {
            LOGGER.info("Table type not found in getStreamTables: {}", tableTypesList);
            throw CallStatus.NOT_FOUND
                    .withDescription("Table type not found in getStreamTables")
                    .toRuntimeException();
        }

        Map<String, Map<String, Schema>> tables = new java.util.LinkedHashMap<>();
        try {
            for (Map.Entry<String, org.apache.hadoop.fs.Path> schemaEntry :
                    metadataService.getSchemas(schemaFilterPattern).entrySet()) {
                Map<String, Schema> tablesForSchema = new java.util.LinkedHashMap<>();
                tables.put(schemaEntry.getKey(), tablesForSchema);
                for (Map.Entry<String, org.apache.hadoop.fs.Path> tableEntry :
                        metadataService.getTables(schemaEntry.getKey(), tableFilterPattern)
                                .entrySet()) {
                    Schema schema = command.getIncludeSchema()
                            ? metadataService.getTableSchema(
                                    schemaEntry.getKey(), tableEntry.getKey(), null)
                            : null;
                    tablesForSchema.put(tableEntry.getKey(), schema);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            return;
        }

        boolean errored = false;
        try (VectorSchemaRoot root = metadataService.getTablesRoot(
                tables, allocator, command.getIncludeSchema(),
                schemaFilterPattern, tableFilterPattern)) {
            listener.start(root);
            listener.putNext();
        } catch (Exception e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTypeInfoRoot(request, allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public void close() throws Exception {
        cancellations.values().forEach(Runnable::run);
        cancellations.clear();
        queryExecutor.shutdownNow();
        if (!queryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.warn("Query executor did not stop within timeout");
        }
    }

    /**
     * @param handle ticket handle as ByteString
     * @return truncated 8-character query identifier
     */
    private static String qid(ByteString handle) {
        String raw = handle.toStringUtf8();
        return raw.length() >= 8 ? raw.substring(0, 8) : raw;
    }

    /**
     * @param request protobuf request message
     * @param descriptor flight descriptor
     * @param schema Arrow schema for the response
     * @return FlightInfo with determined endpoints
     */
    private <T extends Message> FlightInfo getFlightInfoForSchema(
            T request, FlightDescriptor descriptor, Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
