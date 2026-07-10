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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.surpin.data.arrowflight.server.db.ParquetManager;
import net.surpin.data.arrowflight.server.db.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.utils.MetadataUtils;
import net.surpin.data.arrowflight.server.utils.HostUtils;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

/**
 * Реализация Flight SQL сервиса для Hadoop Parquet сервера.
 */
public class HadoopFlightSqlService extends BasicFlightSqlProducer implements FlightProducer, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFlightSqlService.class);

    private static final String SERVER_REGISTRY_MAP = "server-registry";
    private static final String STATEMENT_CACHE_MAP = "statement-cache";
    private static final String SERVER_HEARTBEAT_MAP = "server-heartbeats";
    private static final long HEARTBEAT_INTERVAL_SEC = 15;
    private static final long HEARTBEAT_TIMEOUT_SEC = 45;

    private final Location location;
    private final ParquetManager parquetManager;
    private final BufferAllocator allocator;
    private final HazelcastInstance hazelcastInstance;
    private final SqlInfoBuilder sqlInfoBuilder;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, Long> serverRegistry;
    private final IMap<String, Long> serverHeartbeats;
    private final ScheduledExecutorService heartbeatExecutor;
    private final String serverUri;

    public HadoopFlightSqlService(Location location, ParquetManager parquetManager,
            BufferAllocator allocator, HazelcastInstance hazelcastInstance) {
        this.location = Objects.requireNonNull(location);
        this.parquetManager = Objects.requireNonNull(parquetManager);
        this.allocator = Objects.requireNonNull(allocator);
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance);
        this.statementCache = hazelcastInstance.getMap(STATEMENT_CACHE_MAP);
        this.serverRegistry = hazelcastInstance.getMap(SERVER_REGISTRY_MAP);
        this.serverHeartbeats = hazelcastInstance.getMap(SERVER_HEARTBEAT_MAP);

        this.serverUri = location.getUri().toString();
        LOGGER.info("Registering server: {}", serverUri);
        serverRegistry.put(serverUri, 0L);
        serverHeartbeats.put(serverUri, System.currentTimeMillis());

        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flight-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                serverHeartbeats.put(serverUri, System.currentTimeMillis());
            } catch (Exception e) {
                LOGGER.warn("Failed to update heartbeat for {}: {}", serverUri, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        statementCache.addLocalEntryListener((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.serverUri() != null) {
                serverRegistry.compute(state.serverUri(), (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    long updated = v - state.bytes();
                    return updated <= 0 ? 0L : updated;
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
                Set<String> allServerUris = new HashSet<>();
                for (var entry : serverRegistry.entrySet()) {
                    allServerUris.add(entry.getKey());
                }
                if (allServerUris.isEmpty()) {
                    LOGGER.info("Server registry is empty, please restart server for registration");
                    allServerUris = Set.of(location.getUri().toString());
                }

                // Filter out dead servers that haven't sent heartbeat recently.
                allServerUris = filterLiveServers(allServerUris);
                if (allServerUris.isEmpty()) {
                    LOGGER.info("No live servers found, falling back to self");
                    allServerUris = Set.of(serverUri);
                }

                // Read current server loads from Hazelcast in one bulk call.
                Map<String, Long> serverLoad = new HashMap<>(serverRegistry.getAll(allServerUris));
                for (String uri : allServerUris) {
                    serverLoad.putIfAbsent(uri, 0L);
                }

                // PATH A: JOIN

                // One endpoint per server: group files by their assigned server so each
                // server streams all its partial aggregation rows in a single response.
                ParquetQueryParser pq = ParquetQueryParser.parse(query);

                if (pq.isJoin) {
                    String allFilesServer = findServerWithAllFiles(pathLocations, allServerUris);
                    if (allFilesServer != null) {
                        LOGGER.info("Join executes server-side on {} with {} file(s)", allFilesServer, pathLocations.size());
                        long addedBytes = pathLocations.values().stream().mapToLong(FileAssignment::size).sum();
                        FlightEndpoint ep = createEndpoint(allFilesServer, pathLocations.keySet().stream().toList(), query, addedBytes);
                        serverRegistry.compute(allFilesServer, (k, v) -> (v == null ? 0L : v) + addedBytes);
                        return List.of(ep);
                    }
                    LOGGER.info("Join query spans multiple servers; splitting into per-table reads for Spark-side join");
                    List<FlightEndpoint> endpoints = new ArrayList<>();
                    Map<String, Set<String>> seenTables = new LinkedHashMap<>();
                    for (String path : pathLocations.keySet()) {
                        String table = extractTableFromPath(path);
                        seenTables.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(path);
                    }
                    for (Map.Entry<String, Set<String>> entry : seenTables.entrySet()) {
                        String table = entry.getKey();
                        Set<String> tablePaths = entry.getValue();
                        String perTableQuery = "SELECT * FROM " + table;
                        Map<String, FileAssignment> tableLocations = new LinkedHashMap<>();
                        for (String path : tablePaths) {
                            tableLocations.put(path, pathLocations.get(path));
                        }
                        Map<String, List<String>> byServer = groupFilesByServer(tableLocations, serverLoad);

                        for (Map.Entry<String, List<String>> e : byServer.entrySet()) {
                            long addedBytes = e.getValue().stream()
                                    .mapToLong(p -> tableLocations.get(p).size())
                                    .sum();
                            endpoints.add(createEndpoint(e.getKey(), e.getValue(), perTableQuery, addedBytes));
                            serverRegistry.compute(e.getKey(), (k, v) -> (v == null ? 0L : v) + addedBytes);
                        }
                    }
                    return endpoints;
                }

                // PATH B: OTHER

                // Group files by assigned server. Data locality is the primary criterion;
                // among eligible servers, pick the one with the smallest cumulative load.
                Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
                Map<String, Long> serverAdditions = new HashMap<>();
                for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
                    String path = entry.getKey();
                    FileAssignment fa = entry.getValue();
                    String bestServer = pickServer(fa.hosts(), serverLoad);
                    serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(path);
                    serverLoad.merge(bestServer, fa.size(), Long::sum);
                    serverAdditions.merge(bestServer, fa.size(), Long::sum);
                }

                List<FlightEndpoint> endpoints = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
                    String serverUri = entry.getKey();
                    List<String> serverFiles = entry.getValue();
                    long addedBytes = serverAdditions.getOrDefault(serverUri, 0L);
                    endpoints.add(createEndpoint(serverUri, serverFiles, query, addedBytes));
                    serverRegistry.compute(serverUri, (k, v) -> (v == null ? 0L : v) + addedBytes);
                }
                LOGGER.info("determineEndpoints: {} server(s) registered, {} endpoint(s) for {} file(s), query: {}",
                        allServerUris.size(), endpoints.size(), pathLocations.size(), query);
                LOGGER.debug("determineEndpoints: servers={}, files={}", allServerUris, pathLocations.keySet());
                return endpoints;
            } else {
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(new FlightEndpoint(ticket, this.location));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, List<String>> groupFilesByServer(Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            String serverUri = pickServer(entry.getValue().hosts(), serverLoad);
            serverToFiles.computeIfAbsent(serverUri, k -> new ArrayList<>()).add(entry.getKey());
        }
        return serverToFiles;
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
            LOGGER.error("Failed to process server-grouped ticket, files: {}", Arrays.toString(filePaths), e);
            listener.error(e);
        } finally {
            if (state.serverUri() != null) {
                // Keep the ticket readable until TTL so Spark can retry a failed task from
                // the beginning. Only the load accounting is cleared after the first stream.
                statementCache.set(handle.toStringUtf8(),
                        HandleState.forServerFiles(query, filePaths, state.serverUri(), 0L),
                        10, TimeUnit.MINUTES);
                serverRegistry.compute(state.serverUri(), (k, v) -> {
                    if (v == null) return null;
                    long updated = v - state.bytes();
                    return updated <= 0 ? 0L : updated;
                });
            }
        }
    }

    /** Returns a server URI that has ALL files in the map, or null if none does. */
    private String findServerWithAllFiles(Map<String, FileAssignment> pathLocations,
            Set<String> allServerUris) {
        outer:
        for (String serverUri : allServerUris) {
            String normServer = HostUtils.normalize(serverUri);
            for (FileAssignment fa : pathLocations.values()) {
                boolean hasHost = false;
                for (String host : fa.hosts()) {
                    if (HostUtils.normalize(host).equals(normServer)) {
                        hasHost = true;
                        break;
                    }
                }
                if (!hasHost) continue outer;
            }
            return serverUri;
        }
        return null;
    }

    static String extractTableFromPath(String path) {
        int lastSep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        if (lastSep < 0) return path;
        String parent = path.substring(0, lastSep);
        return parent.replace('\\', '.').replace('/', '.');
    }

    private FlightEndpoint createEndpoint(String serverUri, List<String> serverFiles,
            String query, long addedBytes) {
        URI parsedUri = URI.create(serverUri);
        Location serverLoc = Location.forGrpcInsecure(parsedUri.getHost(), parsedUri.getPort());
        ByteString serverHandle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        statementCache.put(serverHandle.toStringUtf8(),
                HandleState.forServerFiles(query, serverFiles.toArray(new String[0]), serverUri, addedBytes),
                10, TimeUnit.MINUTES);
        Ticket serverTicket = new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serverHandle).build())
                .toByteArray());
        return new FlightEndpoint(serverTicket, serverLoc);
    }

    @Override
    public void close() throws Exception {
        try {
            serverRegistry.remove(serverUri);
            serverHeartbeats.remove(serverUri);
        } catch (Throwable t) {
            LOGGER.warn("Failed to deregister server from registry: {}", t.getMessage());
        }
        heartbeatExecutor.shutdownNow();
        AutoCloseables.close(allocator);
    }

    /**
     * Filters out servers that haven't sent a heartbeat within the timeout window.
     */
    Set<String> filterLiveServers(Set<String> serverUris) {
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;

        Map<String, Long> heartbeats = serverHeartbeats.getAll(serverUris);
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHb = heartbeats.get(uri);
            if (lastHb == null) {
                // No heartbeat yet — probably just registered, give it a chance
                live.add(uri);
            } else if (lastHb >= deadline) {
                live.add(uri);
            } else {
                LOGGER.warn("Server {} is stale (last heartbeat {}ms ago), removing from pool",
                        uri, now - lastHb);
                serverRegistry.remove(uri);
                serverHeartbeats.remove(uri);
            }
        }
        if (live.size() < serverUris.size()) {
            LOGGER.info("filterLiveServers: {} of {} servers are alive", live.size(), serverUris.size());
        }
        return live;
    }

    /**
     * Picks the Flight server URI with the smallest cumulative load.
     * Prefers a server whose hostname matches one of the file's block hosts
     * (data locality) when not all servers are equally local.
     */
    private static String pickServer(Set<String> fileHosts, Map<String, Long> serverLoad) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        var localServers = serverLoad.keySet().stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();

        boolean hasLocality = !localServers.isEmpty() && localServers.size() < serverLoad.size();
        var candidates = hasLocality ? localServers : List.copyOf(serverLoad.keySet());
        return candidates.stream().min(Comparator.comparingLong(serverLoad::get)).orElseThrow();
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(T request, FlightDescriptor descriptor,
            Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
