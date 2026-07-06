package net.surpin.data.arrowflight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

public class HadoopFlightSqlService extends BasicFlightSqlProducer implements FlightProducer, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFlightSqlService.class);

    private static final String SERVER_REGISTRY_MAP = "server-registry";
    private static final String STATEMENT_CACHE_MAP = "statement-cache";

    private final Location location;
    private final ParquetManager parquetManager;
    private final BufferAllocator allocator;
    private final HazelcastInstance hazelcastInstance;
    private final SqlInfoBuilder sqlInfoBuilder;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, Long> serverRegistry;

    public HadoopFlightSqlService(Location location, ParquetManager parquetManager,
            BufferAllocator allocator, HazelcastInstance hazelcastInstance) {
        this.location = Objects.requireNonNull(location);
        this.parquetManager = Objects.requireNonNull(parquetManager);
        this.allocator = Objects.requireNonNull(allocator);
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance);
        this.statementCache = hazelcastInstance.getMap(STATEMENT_CACHE_MAP);
        this.serverRegistry = hazelcastInstance.getMap(SERVER_REGISTRY_MAP);

        serverRegistry.put(location.getUri().toString(), 0L);

        statementCache.addLocalEntryListener((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.serverUri() != null) {
                serverRegistry.compute(state.serverUri(), (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    long updated = v - state.bytes();
                    return updated <= 0 ? null : updated;
                });
            }
        });

        sqlInfoBuilder = new SqlInfoBuilder();

        sqlInfoBuilder
                .withFlightSqlServerName("Hadoop-Arrow-Parquet Source")
                .withFlightSqlServerVersion("0.0.1")
                .withFlightSqlServerArrowVersion("0.0.1")
                .withFlightSqlServerReadOnly(true)
                .withFlightSqlServerSql(true)
                .withFlightSqlServerSubstrait(false)
                .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                .withSqlDdlCatalog(false)
                .withSqlDdlSchema(true)
                .withSqlDdlTable(true)
                .withSqlIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
                .withSqlQuotedIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                .withSqlAllTablesAreSelectable(true)
                .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                .withSqlMaxColumnsInTable(1000);

        LOGGER.info("{} initialized", getClass().getName());
    }


    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor descriptor,
            Schema schema) {
        try {
            if (request instanceof FlightSql.TicketStatementQuery statementQuery) {
                final ByteString handle = statementQuery.getStatementHandle();
                HandleState outerState = (HandleState) statementCache.get(handle.toStringUtf8());
                String query = outerState.query();
                Map<String, FileAssignment> pathLocations = parquetManager.locationsForQuery(query);

                // All registered Flight servers.
                Set<String> allServerUris = serverRegistry.keySet();
                if (allServerUris.isEmpty()) {
                    allServerUris = Set.of(location.getUri().toString());
                }

                // Read current server loads from Hazelcast in one bulk call.
                Map<String, Long> serverLoad = new HashMap<>(serverRegistry.getAll(allServerUris));
                for (String uri : allServerUris) {
                    serverLoad.putIfAbsent(uri, 0L);
                }

                // Group files by assigned server. Data locality is the primary criterion;
                // among eligible servers, pick the one with the smallest cumulative load.
                Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
                Map<String, Long> serverAdditions = new HashMap<>();
                for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
                    String path = entry.getKey();
                    FileAssignment fa = entry.getValue();
                    String bestServer = pickServer(fa.hosts(), allServerUris, serverLoad);
                    serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(path);
                    serverLoad.merge(bestServer, fa.size(), Long::sum);
                    serverAdditions.merge(bestServer, fa.size(), Long::sum);
                }

                List<FlightEndpoint> endpoints = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
                    String serverUri = entry.getKey();
                    List<String> serverFiles = entry.getValue();

                    URI parsedUri = URI.create(serverUri);
                    Location serverLoc = Location.forGrpcInsecure(parsedUri.getHost(), parsedUri.getPort());

                    ByteString serverHandle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    long addedBytes = serverAdditions.getOrDefault(serverUri, 0L);
                    statementCache.put(serverHandle.toStringUtf8(),
                            HandleState.forServerFiles(query, serverFiles.toArray(new String[0]), serverUri, addedBytes),
                            10, TimeUnit.MINUTES);

                    Ticket serverTicket = new Ticket(Any.pack(
                            FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serverHandle).build())
                            .toByteArray());
                    endpoints.add(new FlightEndpoint(serverTicket, serverLoc));
                }

                // Persist cumulative server loads in Hazelcast for cross-query planning.
                for (Map.Entry<String, Long> addition : serverAdditions.entrySet()) {
                    String uri = addition.getKey();
                    long delta = addition.getValue();
                    serverRegistry.compute(uri, (k, v) -> (v == null ? 0L : v) + delta);
                }
                LOGGER.info("determineEndpoints: {} server endpoint(s) for {} file(s), query: {}",
                        endpoints.size(), pathLocations.size(), query);
                return endpoints;
            } else {
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(new FlightEndpoint(ticket, this.location));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(final FlightSql.CommandGetSqlInfo request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(final FlightSql.CommandGetSqlInfo command, final CallContext context,
            final ServerStreamListener listener) {
        this.sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(
            final FlightSql.CommandGetCatalogs request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        try (VectorSchemaRoot vectorSchemaRoot = MetadataUtils.getCatalogsRoot(allocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(
            final FlightSql.CommandGetDbSchemas request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(
            final FlightSql.CommandGetDbSchemas command,
            final CallContext context,
            final ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern()
                : null;

        try (VectorSchemaRoot vectorSchemaRoot = MetadataUtils
                .getSchemasRoot(parquetManager.getSchemas(schemaFilterPattern).keySet(), allocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamSchemas", e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(
            final FlightSql.CommandGetTableTypes request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(final CallContext context, final ServerStreamListener listener) {
        try (VectorSchemaRoot vectorSchemaRoot = MetadataUtils.getTableTypesRoot(allocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTables(
            final FlightSql.CommandGetTables request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(
            final FlightSql.CommandGetTables command,
            final CallContext context,
            final ServerStreamListener listener) {

        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern()
                : null;
        final String tableFilterPattern = command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern()
                : null;

        if (catalog != null && !MetadataUtils.CATALOG_NAME.equalsIgnoreCase(catalog)) {
            LOGGER.info("Catalog doesn't exists in call to getStreamTables: {}", catalog);
            throw CallStatus.NOT_FOUND.withDescription("Could not getStreamTables for catalog: " + catalog)
                    .toRuntimeException();
        }

        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes = protocolStringList.toArray(new String[protocolSize]);
        if (!Arrays.stream(tableTypes).allMatch(MetadataUtils.TABLE_TYPE::equalsIgnoreCase)) {
            LOGGER.info("Table type (at least one of) doesn't exists in call to getStreamTables: {}",
                    Arrays.toString(tableTypes));
            throw CallStatus.NOT_FOUND
                    .withDescription("Table type (at least one of) doesn't exists in call to getStreamTables: "
                            + Arrays.toString(tableTypes))
                    .toRuntimeException();
        }

        Map<String, Map<String, Schema>> tables = new HashMap<>();
        try {
            for (Map.Entry<String, Path> schemaEntry : parquetManager.getSchemas(schemaFilterPattern).entrySet()) {
                Map<String, Schema> tablesForSchema = new HashMap<>();
                tables.put(schemaEntry.getKey(), tablesForSchema);
                for (Map.Entry<String, Path> tableEntry : parquetManager
                        .getTables(schemaEntry.getKey(), tableFilterPattern).entrySet()) {
                    Schema schema = command.getIncludeSchema()
                            ? parquetManager.getTableSchema(schemaEntry.getKey(), tableEntry.getKey())
                            : null;
                    tablesForSchema.put(tableEntry.getKey(), schema);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            return;
        }

        try (VectorSchemaRoot vectorSchemaRoot = MetadataUtils.getTablesRoot(
                tables,
                allocator,
                command.getIncludeSchema(),
                schemaFilterPattern,
                tableFilterPattern)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (Exception e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(
            FlightSql.CommandGetXdbcTypeInfo request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
            ServerStreamListener listener) {
        try (VectorSchemaRoot vectorSchemaRoot = MetadataUtils.getTypeInfoRoot(request, allocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();

            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context,
            FlightDescriptor descriptor) {
        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        String query = command.getQuery();
        LOGGER.info("getFlightInfoStatement, ticket {}: {}", handle, query);

        Schema arrowSchema;
        try {
            arrowSchema = parquetManager.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error while getting Arrow schema for the query: " + query, e);
            throw CallStatus.INTERNAL.withDescription("Error while getting Arrow schema for the query: " + query)
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            LOGGER.error("Error while getting Arrow schema for the query: " + query);
            throw CallStatus.NOT_FOUND.withDescription("Could not get Arrow schema for the query: " + query)
                    .toRuntimeException();
        }

        if (LOGGER.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder("getFlightInfoStatement schema for '").append(query).append("': [");
            List<org.apache.arrow.vector.types.pojo.Field> fields = arrowSchema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0)
                    sb.append(", ");
                sb.append(fields.get(i).getName()).append(':').append(fields.get(i).getType());
            }
            sb.append(']');
            LOGGER.info("{}", sb);
        }

        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle)
                .build();

        statementCache.put(handle.toStringUtf8(), HandleState.forQuery(query), 10, TimeUnit.MINUTES);

        return getFlightInfoForSchema(ticket, descriptor, arrowSchema);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context,
            FlightDescriptor descriptor) {
        String query = command.getQuery();
        LOGGER.info("getSchemaStatement: {}", query);

        Schema arrowSchema;
        try {
            arrowSchema = parquetManager.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error while getting Arrow schema for the query: " + query, e);
            throw CallStatus.INTERNAL.withDescription("Error while getting Arrow schema for the query: " + query)
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            throw CallStatus.NOT_FOUND.withDescription("Could not get Arrow schema for the query: " + query)
                    .toRuntimeException();
        }

        return new SchemaResult(arrowSchema);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context,
            ServerStreamListener listener) {
        final ByteString handle = ticket.getStatementHandle();
        HandleState state = (HandleState) statementCache.get(handle.toStringUtf8());
        if (state == null) {
            String msg = "No HandleState found in cache for handle: " + handle;
            LOGGER.error(msg);
            listener.error(new IllegalStateException(msg));
            return;
        }

        String query = state.query();
        String[] filePaths = state.filePaths();
        if (filePaths == null) {
            String msg = "No file paths in handle state: " + handle;
            LOGGER.error(msg);
            listener.error(new IllegalStateException(msg));
            return;
        }

        LOGGER.info("Server-grouped getStream: {} file(s), query='{}'", filePaths.length, query);
        try {
            parquetManager.readParquet(allocator, query, filePaths, listener, true);
            listener.completed();
        } catch (Exception e) {
            LOGGER.error("Failed to process server-grouped ticket, files: " + Arrays.toString(filePaths), e);
            listener.error(e);
        } finally {
            if (state.serverUri() != null) {
                statementCache.remove(handle.toStringUtf8());
                serverRegistry.compute(state.serverUri(), (k, v) -> {
                    if (v == null) return null;
                    long updated = v - state.bytes();
                    return updated <= 0 ? null : updated;
                });
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            serverRegistry.remove(location.getUri().toString());
        } catch (Throwable t) {
            LOGGER.warn("Failed to deregister server from registry: {}", t.getMessage());
        }
        AutoCloseables.close(allocator);
    }

    /**
     * Picks the Flight server URI with the smallest cumulative load.
     * Prefers a server whose hostname matches one of the file's block hosts
     * (data locality) when not all servers are equally local.
     */
    private static String pickServer(Set<String> fileHosts, Set<String> allServerUris,
            Map<String, Long> serverLoad) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        var localServers = allServerUris.stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();

        boolean hasLocality = !localServers.isEmpty() && localServers.size() < allServerUris.size();
        var candidates = hasLocality ? localServers : List.copyOf(allServerUris);
        return candidates.stream().min(Comparator.comparingLong(serverLoad::get)).orElseThrow();
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(T request, FlightDescriptor descriptor,
            Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
