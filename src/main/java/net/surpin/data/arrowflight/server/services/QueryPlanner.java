package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.surpin.data.arrowflight.server.adapters.HostUtils;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.NodeLoadSnapshot;
import net.surpin.data.arrowflight.server.model.QueryPlan;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;

import net.surpin.data.arrowflight.server.LogUtil;

/**
 * Plans query execution across cluster nodes.
 * Determines file locations, assigns files to servers based on data locality and load,
 * and creates Flight endpoints for distributed execution.
 */
public final class QueryPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);
    private static final long FILE_PLAN_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long DEFAULT_THROUGHPUT_BYTES_PER_SECOND =
            128L * 1024L * 1024L;
    private static final double SLOT_PRESSURE_MILLIS = 1_000.0;

    private final ParquetAdapter parquetAdapter;
    private final ClusterService clusterService;
    private final Map<String, CachedFilePlan> filePlanCache = new ConcurrentHashMap<>();

    /**
     * Stores immutable table file assignments until the inventory refresh deadline.
     *
     * @param files cached file assignments
     * @param expiresAtNanos monotonic expiration deadline
     */
    private record CachedFilePlan(
            Map<String, FileAssignment> files, long expiresAtNanos) {
    }

    /**
     * Contains one validated view of live servers and their loads.
     *
     * @param serverUris live server URIs
     * @param serverLoads current load by live server
     * @param nodeSnapshots recent execution state by live server
     * @param schedulerConfig adaptive scheduler configuration
     */
    private record ClusterSnapshot(
            Set<String> serverUris,
            Map<String, Long> serverLoads,
            Map<String, NodeLoadSnapshot> nodeSnapshots,
            SchedulerConfig schedulerConfig) {
    }

    /**
     * Creates QueryPlanner.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param clusterService cluster coordination service
     */
    public QueryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService) {
        this.parquetAdapter = parquetAdapter;
        this.clusterService = clusterService;
        try {
            this.clusterService.registerLocalFiles(this.parquetAdapter.localFileInventory());
            this.parquetAdapter.initCatalogReader();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to publish local Parquet inventory", e);
        }
    }

    /**
     * Determines Flight endpoints for a SQL query, distributing files across live servers.
     * Handles JOIN queries only when one node owns every required shard.
     *
     * @param query SQL query
     * @return list of Flight endpoints
     * @throws IOException on file system error
     */
    public List<FlightEndpoint> determineEndpoints(String query)
            throws IOException {
        return plan(query).endpoints();
    }

    /**
     * Plans endpoints and estimates Parquet input statistics for Spark.
     *
     * @param query SQL query
     * @return endpoints with byte and row estimates
     * @throws IOException on file-system or cluster metadata failure
     */
    public QueryPlan plan(String query) throws IOException {
        long t = LogUtil.mark();
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        long tSv = LogUtil.mark();
        ClusterSnapshot cluster = validatedCluster();
        Set<String> allServerUris = cluster.serverUris();
        boolean sharedStorage = usesSharedStorage();
        LogUtil.logTiming(tSv, "planning.validateServers", "servers=" + allServerUris.size());
        long tPaths = LogUtil.mark();
        Map<String, FileAssignment> pathLocations = validatedPathLocations(
                parsed, allServerUris, sharedStorage);
        LogUtil.logTiming(tPaths, "planning.fileLocations", "files=" + pathLocations.size());

        List<FlightEndpoint> endpoints;
        if (parsed.isJoin) {
            endpoints = joinEndpoints(
                    query, pathLocations, cluster, sharedStorage);
        } else {
            endpoints = distributeEndpoints(query, pathLocations,
                    new HashMap<>(cluster.serverLoads()),
                    cluster.nodeSnapshots(), cluster.schedulerConfig(),
                    sharedStorage);
        }
        long totalBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        long totalRecords = parquetAdapter.estimateRowCount(pathLocations);
        LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size() + " files=" + pathLocations.size());
        return new QueryPlan(endpoints, totalBytes, totalRecords);
    }

    private ClusterSnapshot validatedCluster() throws IOException {
        long t = LogUtil.mark();
        Map<String, Long> registry = clusterService.allServerLoads();
        if (registry.isEmpty()) {
            throw new IOException("Flight server registry is empty");
        }
        Set<String> uris = new LinkedHashSet<>(clusterService.filterLiveServers(registry.keySet()));
        if (uris.isEmpty()) {
            throw new IOException("No live Flight servers are registered");
        }
        SchedulerConfig schedulerConfig = clusterService.schedulerConfig();
        if (schedulerConfig == null) {
            schedulerConfig = SchedulerConfig.disabled();
        }
        Map<String, NodeLoadSnapshot> snapshots =
                new HashMap<>(clusterService.nodeSnapshots(uris));
        if (schedulerConfig.enabled() && !snapshots.isEmpty()) {
            long now = System.currentTimeMillis();
            SchedulerConfig finalSchedulerConfig = schedulerConfig;
            uris.removeIf(uri -> {
                NodeLoadSnapshot snapshot = snapshots.get(uri);
                return snapshot == null
                        || !snapshot.isFresh(
                                now, finalSchedulerConfig.snapshotStaleMillis())
                        || !snapshot.acceptingRequests();
            });
            if (uris.isEmpty()) {
                throw new NoSchedulableNodeException(
                        "No Flight servers have fresh schedulable load snapshots");
            }
            snapshots.keySet().retainAll(uris);
        }
        Set<String> missing = uris.stream()
                .filter(uri -> !clusterService.hasFileInventory(uri))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (!missing.isEmpty()) {
            throw new IOException("Flight nodes have not published file inventories: " + missing);
        }
        LogUtil.logTiming(t, "planning.validateServers", "live=" + uris.size() + " total=" + registry.size());
        Map<String, Long> serverLoad = new HashMap<>();
        for (String uri : uris) {
            Long load = registry.get(uri);
            serverLoad.put(uri, load != null ? load : 0L);
        }
        return new ClusterSnapshot(
                Set.copyOf(uris), Map.copyOf(serverLoad),
                Map.copyOf(snapshots), schedulerConfig);
    }

    private Map<String, FileAssignment> validatedPathLocations(
            ParquetQueryParser parsed, Set<String> allServerUris,
            boolean sharedStorage) throws IOException {
        String cacheKey = sourceCacheKey(parsed);
        long now = System.nanoTime();
        CachedFilePlan cached = filePlanCache.get(cacheKey);
        Map<String, FileAssignment> pathLocations;
        boolean reusedCache = cached != null && cached.expiresAtNanos() > now;
        if (reusedCache) {
            pathLocations = cached.files();
        } else {
            pathLocations = loadFilePlan(parsed, cacheKey, now);
        }
        try {
            validatePathLocations(
                    pathLocations, parsed, allServerUris, sharedStorage);
        } catch (IOException e) {
            if (!reusedCache) {
                throw e;
            }
            filePlanCache.remove(cacheKey, cached);
            pathLocations = loadFilePlan(parsed, cacheKey, System.nanoTime());
            validatePathLocations(
                    pathLocations, parsed, allServerUris, sharedStorage);
        }
        return pathLocations;
    }

    /**
     * Loads and caches file assignments for referenced tables.
     *
     * @param parsed parsed SQL query
     * @param cacheKey normalized source-table key
     * @param now current monotonic time
     * @return immutable file assignments
     */
    private Map<String, FileAssignment> loadFilePlan(
            ParquetQueryParser parsed, String cacheKey, long now) {
        Map<String, FileAssignment> loaded = filterForQuery(
                clusterService.fileLocations(), parsed);
        Map<String, FileAssignment> immutable = Collections.unmodifiableMap(
                new LinkedHashMap<>(loaded));
        if (!immutable.isEmpty()) {
            filePlanCache.put(cacheKey, new CachedFilePlan(
                    immutable, now + FILE_PLAN_TTL_NANOS));
        }
        return immutable;
    }

    /**
     * Validates cached assignments against current live cluster membership.
     *
     * @param pathLocations file assignments
     * @param parsed parsed SQL query
     * @param allServerUris live server URIs
     * @param sharedStorage whether every compute node can read every path
     * @throws IOException when files or required shard owners are unavailable
     */
    private static void validatePathLocations(
            Map<String, FileAssignment> pathLocations,
            ParquetQueryParser parsed, Set<String> allServerUris,
            boolean sharedStorage)
            throws IOException {
        if (pathLocations.isEmpty()) {
            throw new IOException("No distributed Parquet files found for query: " + parsed);
        }
        if (!sharedStorage) {
            for (Map.Entry<String, FileAssignment> file : pathLocations.entrySet()) {
                boolean hasLiveOwner = file.getValue().hosts().stream()
                        .anyMatch(allServerUris::contains);
                if (!hasLiveOwner) {
                    throw new IOException(
                            "No live Flight node owns required shard: "
                                    + file.getKey());
                }
            }
            requireShardCoverage(pathLocations, parsed, allServerUris);
        }
    }

    private List<FlightEndpoint> joinEndpoints(String query,
            Map<String, FileAssignment> pathLocations, ClusterSnapshot cluster,
            boolean sharedStorage)
            throws IOException {
        long addedBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        Set<String> fullyLocalServers = serversWithAllFiles(
                pathLocations, cluster.serverUris());
        String allFilesServer;
        if (sharedStorage) {
            allFilesServer = selectServer(
                    fullyLocalServers,
                    new HashMap<>(cluster.serverLoads()),
                    cluster.nodeSnapshots(),
                    true,
                    addedBytes,
                    cluster.schedulerConfig());
        } else {
            allFilesServer = fullyLocalServers.stream().sorted().findFirst().orElse(null);
        }
        if (allFilesServer == null) {
            throw new IOException(
                    "Server-side joins require all input shards on one Flight node; "
                            + "Spark must execute this distributed join");
        }
        FlightEndpoint ep = createEndpoint(allFilesServer,
                new ArrayList<>(pathLocations.keySet()), query, addedBytes, true);
        clusterService.addLoad(allFilesServer, addedBytes);
        return List.of(ep);
    }

    private List<FlightEndpoint> distributeEndpoints(String query,
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad,
            Map<String, NodeLoadSnapshot> nodeSnapshots,
            SchedulerConfig schedulerConfig, boolean sharedStorage) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        Map<String, Long> serverAdditions = new HashMap<>();
        List<Map.Entry<String, FileAssignment>> orderedFiles =
                new ArrayList<>(pathLocations.entrySet());
        orderedFiles.sort(Map.Entry.<String, FileAssignment>comparingByValue(
                Comparator.comparingLong(FileAssignment::size)).reversed()
                .thenComparing(Map.Entry.comparingByKey()));
        for (Map.Entry<String, FileAssignment> entry : orderedFiles) {
            FileAssignment fa = entry.getValue();
            String bestServer = selectServer(
                    fa.hosts(), serverLoad, nodeSnapshots,
                    sharedStorage, fa.size(), schedulerConfig);
            serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(entry.getKey());
            serverLoad.merge(bestServer, fa.size(), Long::sum);
            serverAdditions.merge(bestServer, fa.size(), Long::sum);
        }

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
            long addedBytes = serverAdditions.getOrDefault(entry.getKey(), 0L);
            endpoints.add(createEndpoint(entry.getKey(), entry.getValue(),
                    query, addedBytes, true));
            clusterService.addLoad(entry.getKey(), addedBytes);
        }
        return endpoints;
    }

    /**
     * Builds a stable cache key from the tables referenced by a query.
     *
     * @param parsed parsed SQL query
     * @return normalized source-table key
     */
    private static String sourceCacheKey(ParquetQueryParser parsed) {
        if (!parsed.isJoin) {
            return normalizedTable(parsed.schema, parsed.table);
        }
        return parsed.joinTables.stream()
                .map(table -> normalizedTable(table.schema(), table.table()))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    /**
     * Normalizes a schema and table name for metadata caching.
     *
     * @param schema schema name
     * @param table table name
     * @return normalized qualified table name
     */
    private static String normalizedTable(String schema, String table) {
        return ((schema == null ? "" : schema) + "." + table)
                .toLowerCase(Locale.ROOT);
    }

    private static Map<String, FileAssignment> filterForQuery(
            Map<String, FileAssignment> inventory, ParquetQueryParser query) {
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> file : inventory.entrySet()) {
            boolean matches;
            if (query.isJoin) {
                matches = query.joinTables.stream().anyMatch(table ->
                        belongsToTable(file.getKey(), table.schema(), table.table()));
            } else {
                matches = belongsToTable(file.getKey(), query.schema, query.table);
            }
            if (matches) {
                result.put(file.getKey(), file.getValue());
            }
        }
        return result;
    }

    private static boolean belongsToTable(String path, String schema, String table) {
        String normalized = path.replace('\\', '/');
        if (schema == null || schema.isEmpty()) {
            String parent = extractTableFromPath(normalized);
            return parent.equalsIgnoreCase(table)
                    || parent.toLowerCase().endsWith("." + table.toLowerCase());
        }
        String prefix = schema + "/" + table + "/";
        return normalized.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * The benchmark generator writes one shard (possibly empty) for every table
     * on every configured node. Requiring that coverage prevents an empty or
     * incomplete inventory from being mistaken for a complete distributed table.
     */
    private static void requireShardCoverage(Map<String, FileAssignment> files,
            ParquetQueryParser query, Set<String> liveServers) throws IOException {
        for (String server : liveServers) {
            if (query.isJoin) {
                for (ParquetQueryParser.JoinTable table : query.joinTables) {
                    if (!ownsTableShard(files, server, table.schema(), table.table())) {
                        throw new IOException("Flight node " + server
                                + " has no shard for required table " + table.table());
                    }
                }
            } else if (!ownsTableShard(files, server, query.schema, query.table)) {
                throw new IOException("Flight node " + server
                        + " has no shard for required table " + query.table);
            }
        }
    }

    private static boolean ownsTableShard(Map<String, FileAssignment> files,
            String server, String schema, String table) {
        return files.entrySet().stream().anyMatch(file ->
                file.getValue().hosts().contains(server)
                        && belongsToTable(file.getKey(), schema, table));
    }

    /**
     * Creates a Flight endpoint for a set of files assigned to a server.
     *
     * @param serverUri target server URI
     * @param filePaths file paths to stream
     * @param query     SQL query
     * @param bytes     estimated byte count for load tracking
     * @return Flight endpoint
     */
    public FlightEndpoint createEndpoint(String serverUri, List<String> filePaths,
            String query, long bytes) {
        return createEndpoint(serverUri, filePaths, query, bytes, true);
    }

    /**
     * Creates a Flight endpoint with explicit distributed load accounting.
     *
     * @param serverUri target server URI
     * @param filePaths file paths to stream
     * @param query SQL query
     * @param bytes estimated byte count
     * @param loadTracked whether bytes were added to distributed server load
     * @return Flight endpoint
     */
    private FlightEndpoint createEndpoint(String serverUri, List<String> filePaths,
            String query, long bytes, boolean loadTracked) {
        long t = LogUtil.mark();
        URI parsedUri = URI.create(serverUri);
        // Preserve the registered URI (including grpc+tls). Reconstructing every
        // location as insecure both loses transport information and can cause the
        // client to send a ticket to the wrong connection.
        Location serverLoc = new Location(parsedUri);
        HandleState state = HandleState.forServerFiles(
                query, filePaths.toArray(new String[0]),
                serverUri, bytes, loadTracked);
        ByteString serverHandle = ByteString.copyFrom(
                clusterService.createEndpointHandle(state));
        Ticket serverTicket = new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(serverHandle).build())
                .toByteArray());
        LogUtil.logTiming(t, "planning.createEndpoint", "server=" + serverUri + " files=" + filePaths.size() + " bytes=" + bytes);
        return new FlightEndpoint(serverTicket, serverLoc);
    }

    /**
     * Picks the best server for a set of file block hosts.
     * Prefers local servers with smallest load; falls back to globally least-loaded.
     *
     * @param fileHosts  set of hosts that have the file blocks
     * @param serverLoad current server loads
     * @return selected server URI
     */
    public static String pickServer(Set<String> fileHosts, Map<String, Long> serverLoad) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        var localServers = serverLoad.keySet().stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();

        boolean hasLocality = !localServers.isEmpty() && localServers.size() < serverLoad.size();
        var candidates = hasLocality ? localServers : List.copyOf(serverLoad.keySet());
        return candidates.stream()
                .min(Comparator.comparingLong(serverLoad::get))
                .orElseThrow();
    }

    /**
     * Selects the node with the lowest predicted completion score.
     *
     * @param fileHosts nodes with local blocks for the task
     * @param serverLoad outstanding reserved bytes by live node
     * @param snapshots recent node execution snapshots
     * @param sharedStorage whether remote reads are allowed
     * @param taskBytes logical bytes in the task
     * @param schedulerConfig adaptive scheduler configuration
     * @return selected Flight server URI
     */
    static String selectServer(
            Set<String> fileHosts,
            Map<String, Long> serverLoad,
            Map<String, NodeLoadSnapshot> snapshots,
            boolean sharedStorage,
            long taskBytes,
            SchedulerConfig schedulerConfig) {
        if (!schedulerConfig.enabled() || snapshots.isEmpty()) {
            return pickServer(fileHosts, serverLoad);
        }
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());
        List<String> localServers = serverLoad.keySet().stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();
        List<String> candidates;
        if (sharedStorage || localServers.isEmpty()) {
            candidates = List.copyOf(serverLoad.keySet());
        } else {
            candidates = localServers;
        }
        return candidates.stream()
                .min(Comparator
                        .comparingDouble((String uri) -> schedulingScore(
                                uri,
                                normalizedFileHosts,
                                serverLoad.getOrDefault(uri, 0L),
                                taskBytes,
                                snapshots.get(uri),
                                schedulerConfig))
                        .thenComparing(uri -> uri))
                .orElseThrow();
    }

    /**
     * Estimates how long a node will take to start and process a file.
     *
     * @param serverUri candidate server URI
     * @param normalizedFileHosts normalized hosts containing the file
     * @param reservedBytes bytes already reserved for the node
     * @param taskBytes bytes in the candidate task
     * @param snapshot latest node load snapshot
     * @param config scheduler configuration
     * @return comparable scheduling score in milliseconds
     */
    static double schedulingScore(
            String serverUri,
            Set<String> normalizedFileHosts,
            long reservedBytes,
            long taskBytes,
            NodeLoadSnapshot snapshot,
            SchedulerConfig config) {
        long throughput = snapshot != null
                && snapshot.throughputBytesPerSecond() > 0L
                ? snapshot.throughputBytesPerSecond()
                : DEFAULT_THROUGHPUT_BYTES_PER_SECOND;
        int limit = snapshot != null
                ? Math.max(1, snapshot.concurrencyLimit()) : 1;
        int active = snapshot != null ? snapshot.activeQueries() : 0;
        int queued = snapshot != null ? snapshot.queuedQueries() : 0;
        double workMillis = (reservedBytes + Math.max(0L, taskBytes))
                * 1_000.0 / throughput / limit;
        double slotPressure = (active + queued)
                * SLOT_PRESSURE_MILLIS / limit;
        double resourcePenalty = snapshot == null ? 0.0
                : pressurePenalty(Math.max(
                        snapshot.processCpuLoad(), snapshot.systemCpuLoad()))
                        + pressurePenalty(snapshot.memoryPressure());
        boolean local = normalizedFileHosts.contains(
                HostUtils.normalize(serverUri));
        double localityPenalty = local
                ? 0.0 : config.remoteLocalityPenaltyMillis();
        return workMillis + slotPressure + resourcePenalty + localityPenalty;
    }

    /**
     * Converts a sampled utilization value into a nonlinear scheduling penalty.
     *
     * @param pressure sampled utilization
     * @return scheduling penalty in milliseconds
     */
    private static double pressurePenalty(double pressure) {
        if (pressure < 0.0) {
            return 0.0;
        }
        return Math.pow(Math.min(1.0, pressure), 4.0) * 2_000.0;
    }

    /**
     * Groups files by their assigned server based on data locality and load.
     *
     * @param pathLocations file to host assignments
     * @param serverLoad    current server loads
     * @return map of server URI to list of file paths
     */
    public static Map<String, List<String>> groupFilesByServer(
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            String serverUri = pickServer(entry.getValue().hosts(), serverLoad);
            serverToFiles.computeIfAbsent(serverUri, k -> new ArrayList<>()).add(entry.getKey());
        }
        return serverToFiles;
    }

    /**
     * Finds every server that has all files in the map.
     *
     * @param pathLocations file to host assignments
     * @param allServerUris all registered server URIs
     * @return server URIs with all files
     */
    private static Set<String> serversWithAllFiles(
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris) {
        Set<String> result = new LinkedHashSet<>();
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
                if (!hasHost) {
                    continue outer;
                }
            }
            result.add(serverUri);
        }
        return result;
    }

    /**
     * Detects storage schemes that every compute node can read remotely.
     *
     * @return whether the configured data directory uses shared storage
     */
    private boolean usesSharedStorage() {
        return usesSharedStorage(parquetAdapter.dataDirectory());
    }

    /**
     * Detects a shared-storage scheme from a configured data directory.
     *
     * @param dataDirectory configured Parquet data directory
     * @return whether every compute node can read paths in the directory
     */
    static boolean usesSharedStorage(String dataDirectory) {
        if (dataDirectory == null || dataDirectory.isBlank()) {
            return false;
        }
        String scheme = new org.apache.hadoop.fs.Path(
                dataDirectory).toUri().getScheme();
        return scheme != null
                && scheme.length() > 1
                && !"file".equalsIgnoreCase(scheme);
    }

    /**
     * Signals that live nodes exist but none can currently accept planned work.
     */
    public static final class NoSchedulableNodeException extends IOException {

        /**
         * Creates a scheduling-capacity failure.
         *
         * @param message client-safe failure description
         */
        public NoSchedulableNodeException(String message) {
            super(message);
        }
    }

    /**
     * Extracts the table name from a relative file path.
     *
     * @param path relative file path
     * @return table name derived from parent directory
     */
    public static String extractTableFromPath(String path) {
        int lastSep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        if (lastSep < 0) {
            return path;
        }
        String parent = path.substring(0, lastSep);
        return parent.replace('\\', '.').replace('/', '.');
    }
}
