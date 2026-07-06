package net.surpin.data.arrowflight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

/**
 * Реализация Flight SQL сервиса для Hadoop Parquet сервера.
 */
public class HadoopFlightSqlService extends BasicFlightSqlProducer implements FlightProducer, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFlightSqlService.class);

    private static final String KEY_DELIMITER = ":";
    private static final String SERVER_REGISTRY_MAP = "server-registry";

    private final Location location;
    private final ParquetManager parquetManager;
    private final BufferAllocator allocator;
    private final HazelcastInstance hazelcastInstance;
    private final SqlInfoBuilder sqlInfoBuilder;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, String> serverRegistry;
    private final String serverRegistrationKey;

    public HadoopFlightSqlService(Location location, ParquetManager parquetManager,
            BufferAllocator allocator, HazelcastInstance hazelcastInstance) {
        this.location = Objects.requireNonNull(location);
        this.parquetManager = Objects.requireNonNull(parquetManager);
        this.allocator = Objects.requireNonNull(allocator);
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance);
        this.statementCache = hazelcastInstance.getMap("statement-cache");
        this.serverRegistry = hazelcastInstance.getMap(SERVER_REGISTRY_MAP);
        this.serverRegistrationKey = "~srv~:" + randomUUID();
        serverRegistry.put(serverRegistrationKey, location.getUri().toString());

        sqlInfoBuilder = new SqlInfoBuilder();

        sqlInfoBuilder
                .withFlightSqlServerName("Hadoop-Arrow-Parquet Source")
                .withFlightSqlServerVersion("0.0.1")
                .withFlightSqlServerArrowVersion("0.0.1")
                .withFlightSqlServerReadOnly(true)
                .withFlightSqlServerSql(true)
                .withFlightSqlServerSubstrait(false)
                .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                // .withSqlIdentifierQuoteChar()
                .withSqlDdlCatalog(false)
                .withSqlDdlSchema(true)
                .withSqlDdlTable(true)
                .withSqlIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
                .withSqlQuotedIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                .withSqlAllTablesAreSelectable(true)
                .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                .withSqlMaxColumnsInTable(1000);
        // .withFlightSqlServerBulkIngestion(false)
        // .withFlightSqlServerBulkIngestionTransaction(false);

        LOGGER.info("{} initialized", getClass().getName());
    }


    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor descriptor,
            Schema schema) {
        try {
            if (request instanceof FlightSql.TicketStatementQuery statementQuery) {
                final ByteString handle = statementQuery.getStatementHandle();
                String query = (String) Objects.requireNonNull(statementCache.get(cacheKey(handle, "query")));
                Map<String, Set<String>> pathLocations = parquetManager.locationsForQuery(query);

                // All registered Flight servers, sorted for deterministic file assignment.
                List<String> allServerUris = new ArrayList<>(serverRegistry.values());
                Collections.sort(allServerUris);
                if (allServerUris.isEmpty()) {
                    allServerUris = List.of(location.getUri().toString());
                }

                // One endpoint per server: group files by their assigned server so each
                // server streams all its partial aggregation rows in a single response.
                ParquetQueryParser pq = ParquetQueryParser.parse(query);
                List<String> sortedPaths = pathLocations.keySet().stream().sorted().collect(Collectors.toList());

                if (pq.isJoin) {
                    String allFilesServer = findServerWithAllFiles(pathLocations, allServerUris);
                    if (allFilesServer != null) {
                        LOGGER.info("Join executes server-side on {} with {} file(s)", allFilesServer, sortedPaths.size());
                        return List.of(createEndpoint(allFilesServer, sortedPaths, query));
                    }
                    LOGGER.info("Join query spans multiple servers; splitting into per-table reads for Spark-side join");
                    List<FlightEndpoint> endpoints = new ArrayList<>();
                    Map<String, Set<String>> seenTables = new LinkedHashMap<>();
                    for (String path : sortedPaths) {
                        String table = extractTableFromPath(path);
                        seenTables.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(path);
                    }
                    for (Map.Entry<String, Set<String>> entry : seenTables.entrySet()) {
                        String table = entry.getKey();
                        List<String> tablePaths = new ArrayList<>(entry.getValue());
                        String perTableQuery = "SELECT * FROM " + table;
                        Map<String, List<String>> byServer = groupFilesByServer(pathLocations, allServerUris, tablePaths);

                        for (Map.Entry<String, List<String>> e : byServer.entrySet()) {
                            endpoints.add(createEndpoint(e.getKey(), e.getValue(), perTableQuery));
                        }
                    }
                    return endpoints;
                }

                // Non-join: existing per-server file grouping logic.
                Map<String, List<String>> serverToFiles = groupFilesByServer(pathLocations, allServerUris, sortedPaths);

                List<FlightEndpoint> endpoints = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
                    String serverUri = entry.getKey();
                    List<String> serverFiles = entry.getValue();

                    endpoints.add(createEndpoint(serverUri, serverFiles, query));
                }
                LOGGER.info("determineEndpoints: {} server endpoint(s) for {} file(s), query: {}",
                        endpoints.size(), sortedPaths.size(), query);
                return endpoints;
            } else {
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(new FlightEndpoint(ticket, this.location));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, List<String>> groupFilesByServer(Map<String, Set<String>> pathLocations, List<String> allServerUris, List<String> sortedPaths) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        for (int i = 0; i < sortedPaths.size(); i++) {
            String path = sortedPaths.get(i);
            String serverUri = pickServer(pathLocations.get(path), allServerUris, i);
            serverToFiles.computeIfAbsent(serverUri, k -> new ArrayList<>()).add(path);
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

        statementCache.put(cacheKey(handle, "query"), query, 10, TimeUnit.MINUTES);

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
        String query = (String) Objects.requireNonNull(statementCache.get(cacheKey(handle, "query")));

        // Per-server ticket: this server owns all files in the "files" list.
        String[] filePaths = (String[]) statementCache.get(cacheKey(handle, "files"));
        if (filePaths == null) {
            String msg = "No 'files' entry found in cache for handle: " + handle;
            LOGGER.error(msg);
            listener.error(new IllegalStateException(msg));
            return;
        }

        LOGGER.info("Server-grouped getStream: {} file(s), query='{}'", filePaths.length, query);
        try {
            // readParquet delegates to executeAggregation for aggregation queries.
            // Both paths receive all files at once → single FileSystemDatasetFactory
            // lets Arrow Dataset's internal C++ thread pool parallelise the scan.
            parquetManager.readParquet(allocator, query, filePaths, listener, true);
            listener.completed();
        } catch (Exception e) {
            LOGGER.error("Failed to process server-grouped ticket, files: {}", Arrays.toString(filePaths), e);
            listener.error(e);
        }
    }

    /** Returns a server URI that has ALL files in the map, or null if none does. */
    private String findServerWithAllFiles(Map<String, Set<String>> pathLocations,
            List<String> allServerUris) {
        outer:
        for (String serverUri : allServerUris) {
            String normServer = HostUtils.normalize(serverUri);
            for (Set<String> hosts : pathLocations.values()) {
                boolean hasHost = false;
                for (String host : hosts) {
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

    private static String extractTableFromPath(String path) {
        int lastSep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        if (lastSep < 0) return path;
        String parent = path.substring(0, lastSep);
        return parent.replace('\\', '.').replace('/', '.');
    }

    private FlightEndpoint createEndpoint(String serverUri, List<String> serverFiles,
            String query) {
        URI parsedUri = URI.create(serverUri);
        Location serverLoc = Location.forGrpcInsecure(parsedUri.getHost(), parsedUri.getPort());
        ByteString serverHandle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        statementCache.put(cacheKey(serverHandle, "query"), query, 10, TimeUnit.MINUTES);
        statementCache.put(cacheKey(serverHandle, "files"),
                serverFiles.toArray(new String[0]), 10, TimeUnit.MINUTES);
        Ticket serverTicket = new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serverHandle).build())
                .toByteArray());
        return new FlightEndpoint(serverTicket, serverLoc);
    }

    @Override
    public void close() throws Exception {
        try {
            serverRegistry.remove(serverRegistrationKey);
        } catch (Throwable t) {
            // Hazelcast may already be shut down; log and continue.
            LOGGER.warn("Failed to deregister server from registry: {}", t.getMessage());
        }
        // Don't clear statementCache: doing so would wipe other servers' in-flight
        // entries
        // from the shared distributed map. Individual statement entries expire after 10
        // min.
        AutoCloseables.close(allocator);
    }

    /**
     * Picks the Flight server URI that should handle a given file.
     * Prefers a server whose hostname matches one of the file's block hosts (data
     * locality).
     * Falls back to round-robin when all servers share the same host (e.g.,
     * localhost in tests)
     * or when no server matches any block host.
     */
    private static String pickServer(Set<String> fileHosts, List<String> allServerUris, int fileIndex) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        List<String> localServers = allServerUris.stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .collect(Collectors.toList());

        if (!localServers.isEmpty() && localServers.size() < allServerUris.size()) {
            return localServers.get(fileIndex % localServers.size());
        }
        return allServerUris.get(fileIndex % allServerUris.size());
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(T request, FlightDescriptor descriptor,
            Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private String cacheKey(ByteString handle, String... keys) {
        Objects.requireNonNull(handle);
        Objects.requireNonNull(keys);
        if (Arrays.stream(keys).anyMatch(key -> key == null || key.contains(KEY_DELIMITER))) {
            throw new IllegalArgumentException(
                    "Keys can't be empty or contain '" + KEY_DELIMITER + "'. But got " + String.join(",", keys));
        }

        return handle.toStringUtf8() + KEY_DELIMITER + String.join(":", keys);
    }
}
