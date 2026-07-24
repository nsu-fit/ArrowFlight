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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import net.surpin.data.arrowflight.server.adapters.HostUtils;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.CapacityExhaustedException;
import net.surpin.data.arrowflight.server.model.ExecutionReservationRequest;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.ServerCapacity;

import static java.util.UUID.randomUUID;

import net.surpin.data.arrowflight.server.LogUtil;

/**
 * Plans query execution across cluster nodes.
 * Determines file locations, assigns files to servers based on data locality and load,
 * and creates Flight endpoints for distributed execution.
 */
public final class QueryPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);

    private final ParquetAdapter parquetAdapter;
    private final ClusterService clusterService;

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
        long t = LogUtil.mark();
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        long tSv = LogUtil.mark();
        Set<String> allServerUris = validatedServerUris();
        LogUtil.logTiming(tSv, "planning.validateServers", "servers=" + allServerUris.size());
        long tLoad = LogUtil.mark();
        Map<String, Long> serverLoad = validatedServerLoad(allServerUris);
        Map<String, ServerCapacity> capacities =
                clusterService.getCapacities(allServerUris);
        LogUtil.logTiming(tLoad, "planning.serverLoads", "servers=" + serverLoad.size());
        long tPaths = LogUtil.mark();
        Map<String, FileAssignment> pathLocations = validatedPathLocations(parsed, allServerUris);
        LogUtil.logTiming(tPaths, "planning.fileLocations", "files=" + pathLocations.size());

        List<FlightEndpoint> endpoints;
        if (parsed.isJoin) {
            endpoints = joinEndpoints(
                    query, pathLocations, allServerUris, serverLoad, capacities);
        } else {
            endpoints = distributeEndpoints(
                    query, pathLocations, serverLoad, capacities);
        }
        LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size() + " files=" + pathLocations.size());
        return endpoints;
    }

    private Set<String> validatedServerUris() throws IOException {
        long t = LogUtil.mark();
        Map<String, Long> registry = clusterService.allServerLoads();
        if (registry.isEmpty()) {
            throw new IOException("Flight server registry is empty");
        }
        Set<String> uris = new LinkedHashSet<>(clusterService.filterLiveServers(registry.keySet()));
        if (uris.isEmpty()) {
            throw new IOException("No live Flight servers are registered");
        }
        Set<String> missing = uris.stream()
                .filter(uri -> !clusterService.hasFileInventory(uri))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (!missing.isEmpty()) {
            throw new IOException("Flight nodes have not published file inventories: " + missing);
        }
        LogUtil.logTiming(t, "planning.validateServers", "live=" + uris.size() + " total=" + registry.size());
        return uris;
    }

    private Map<String, Long> validatedServerLoad(Set<String> serverUris) {
        Map<String, Long> allLoads = clusterService.allServerLoads();
        Map<String, Long> serverLoad = new HashMap<>();
        for (String uri : serverUris) {
            Long load = allLoads.get(uri);
            serverLoad.put(uri, load != null ? load : 0L);
        }
        return serverLoad;
    }

    private Map<String, FileAssignment> validatedPathLocations(
            ParquetQueryParser parsed, Set<String> allServerUris) throws IOException {
        Map<String, FileAssignment> pathLocations = filterForQuery(
                clusterService.fileLocations(), parsed);
        if (pathLocations.isEmpty()) {
            throw new IOException("No distributed Parquet files found for query: " + parsed);
        }
        for (Map.Entry<String, FileAssignment> file : pathLocations.entrySet()) {
            boolean hasLiveOwner = file.getValue().hosts().stream()
                    .anyMatch(allServerUris::contains);
            if (!hasLiveOwner) {
                throw new IOException("No live Flight node owns required shard: " + file.getKey());
            }
        }
        requireShardCoverage(pathLocations, parsed, allServerUris);
        return pathLocations;
    }

    private List<FlightEndpoint> joinEndpoints(String query,
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris,
            Map<String, Long> serverLoad,
            Map<String, ServerCapacity> capacities)
            throws IOException {
        Set<String> candidates = findServersWithAllFiles(
                pathLocations, allServerUris);
        if (candidates.isEmpty()) {
            throw new IOException(
                    "Server-side joins require all input shards on one Flight node; "
                            + "Spark must execute this distributed join");
        }
        String allFilesServer = pickAvailableServer(
                candidates, serverLoad, capacities);
        long addedBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        FlightEndpoint ep = createEndpoint(allFilesServer,
                new ArrayList<>(pathLocations.keySet()), query, addedBytes);
        return List.of(ep);
    }

    private List<FlightEndpoint> distributeEndpoints(String query,
            Map<String, FileAssignment> pathLocations,
            Map<String, Long> serverLoad,
            Map<String, ServerCapacity> capacities) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        Map<String, Long> serverAdditions = new HashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            FileAssignment fa = entry.getValue();
            String bestServer = pickServer(fa.hosts(), serverLoad, capacities);
            serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(entry.getKey());
            serverLoad.merge(bestServer, fa.size(), Long::sum);
            serverAdditions.merge(bestServer, fa.size(), Long::sum);
        }

        List<EndpointPlan> plans = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
            plans.add(endpointPlan(
                    entry.getKey(), entry.getValue(), query,
                    serverAdditions.getOrDefault(entry.getKey(), 0L)));
        }
        return createEndpoints(plans);
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
        return createEndpoints(List.of(
                endpointPlan(serverUri, filePaths, query, bytes))).get(0);
    }

    /**
     * Validates and prepares endpoint material before reservation.
     *
     * @param serverUri target server URI
     * @param filePaths assigned files
     * @param query SQL query
     * @param bytes logical input bytes
     * @return prepared endpoint plan
     */
    private EndpointPlan endpointPlan(String serverUri, List<String> filePaths,
            String query, long bytes) {
        URI parsedUri = URI.create(serverUri);
        ByteString handle = ByteString.copyFrom(
                randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        HandleState state = new HandleState(
                query, filePaths.toArray(new String[0]), serverUri, bytes, null);
        return new EndpointPlan(
                serverUri, new Location(parsedUri), handle, state);
    }

    /**
     * Reserves and materializes all endpoints atomically.
     *
     * @param plans prepared endpoint plans
     * @return Flight endpoints
     */
    private List<FlightEndpoint> createEndpoints(List<EndpointPlan> plans) {
        long started = LogUtil.mark();
        List<ExecutionReservationRequest> requests = plans.stream()
                .map(plan -> new ExecutionReservationRequest(
                        plan.serverUri(), plan.handle().toStringUtf8(),
                        plan.state()))
                .toList();
        clusterService.reserveExecutions(requests);
        List<String> reservationIds = requests.stream()
                .map(ExecutionReservationRequest::handle)
                .toList();
        try {
            List<FlightEndpoint> endpoints = new ArrayList<>(plans.size());
            for (EndpointPlan plan : plans) {
                Ticket ticket = new Ticket(Any.pack(
                        FlightSql.TicketStatementQuery.newBuilder()
                                .setStatementHandle(plan.handle()).build())
                        .toByteArray());
                endpoints.add(new FlightEndpoint(ticket, plan.location()));
            }
            LogUtil.logTiming(started, "planning.createEndpoints",
                    "endpoints=" + endpoints.size());
            return List.copyOf(endpoints);
        } catch (RuntimeException failure) {
            clusterService.releaseExecutions(reservationIds);
            throw failure;
        }
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
        Map<String, ServerCapacity> capacities = new HashMap<>();
        serverLoad.keySet().forEach(uri -> capacities.put(
                uri, ServerCapacity.empty(1, Integer.MAX_VALUE)));
        return pickServer(fileHosts, serverLoad, capacities);
    }

    /**
     * Picks a server by locality, free capacity, queue capacity, and logical load.
     *
     * @param fileHosts file owners
     * @param serverLoad logical server loads
     * @param capacities capacity snapshots
     * @return selected server URI
     * @throws CapacityExhaustedException when every candidate queue is full
     */
    public static String pickServer(Set<String> fileHosts,
            Map<String, Long> serverLoad,
            Map<String, ServerCapacity> capacities) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        var localServers = serverLoad.keySet().stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();

        boolean hasLocality = !localServers.isEmpty() && localServers.size() < serverLoad.size();
        var candidates = hasLocality ? localServers : List.copyOf(serverLoad.keySet());
        return pickAvailableServer(candidates, serverLoad, capacities);
    }

    /**
     * Selects an available candidate by free-slot priority and logical load.
     *
     * @param candidates candidate server URIs
     * @param serverLoad logical loads
     * @param capacities capacity snapshots
     * @return selected URI
     */
    private static String pickAvailableServer(
            Collection<String> candidates,
            Map<String, Long> serverLoad,
            Map<String, ServerCapacity> capacities) {
        return candidates.stream()
                .filter(uri -> hasCapacity(capacities.get(uri)))
                .min(Comparator
                        .comparingInt((String uri) ->
                                hasFreeSlot(capacities.get(uri)) ? 0 : 1)
                        .thenComparingLong(uri ->
                                serverLoad.getOrDefault(uri, 0L)))
                .orElseThrow(() -> new CapacityExhaustedException(
                        "All eligible execution queues are full"));
    }

    /**
     * Checks whether a capacity accepts an active or queued reservation.
     *
     * @param capacity capacity snapshot
     * @return whether admission is possible
     */
    private static boolean hasCapacity(ServerCapacity capacity) {
        return capacity != null
                && (long) capacity.activeSlots() + capacity.queuedQueries()
                + capacity.pendingQueries()
                < (long) capacity.maxActiveSlots() + capacity.maxQueuedQueries();
    }

    /**
     * Checks whether a capacity has an immediate execution slot.
     *
     * @param capacity capacity snapshot
     * @return whether an active slot is free
     */
    private static boolean hasFreeSlot(ServerCapacity capacity) {
        return capacity != null
                && (long) capacity.activeSlots() + capacity.pendingQueries()
                < capacity.maxActiveSlots();
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
     * Finds a server that has ALL files in the map, or null if none.
     *
     * @param pathLocations file to host assignments
     * @param allServerUris all registered server URIs
     * @return server URI with all files, or null
     */
    private Set<String> findServersWithAllFiles(
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

    /**
     * Immutable endpoint material prepared before the atomic reservation.
     */
    private record EndpointPlan(
            String serverUri,
            Location location,
            ByteString handle,
            HandleState state) {
    }
}
