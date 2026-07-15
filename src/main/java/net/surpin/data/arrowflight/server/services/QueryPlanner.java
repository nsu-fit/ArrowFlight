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
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;

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
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        Set<String> allServerUris = validatedServerUris();
        Map<String, Long> serverLoad = validatedServerLoad(allServerUris);
        Map<String, FileAssignment> pathLocations = validatedPathLocations(parsed, allServerUris);

        if (parsed.isJoin) {
            return joinEndpoints(query, pathLocations, allServerUris);
        }
        return distributeEndpoints(query, pathLocations, serverLoad);
    }

    private Set<String> validatedServerUris() throws IOException {
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
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris)
            throws IOException {
        String allFilesServer = findServerWithAllFiles(pathLocations, allServerUris);
        if (allFilesServer == null) {
            throw new IOException(
                    "Server-side joins require all input shards on one Flight node; "
                            + "Spark must execute this distributed join");
        }
        long addedBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        FlightEndpoint ep = createEndpoint(allFilesServer,
                new ArrayList<>(pathLocations.keySet()), query, addedBytes);
        clusterService.addLoad(allFilesServer, addedBytes);
        return List.of(ep);
    }

    private List<FlightEndpoint> distributeEndpoints(String query,
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        Map<String, Long> serverAdditions = new HashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            FileAssignment fa = entry.getValue();
            String bestServer = pickServer(fa.hosts(), serverLoad);
            serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(entry.getKey());
            serverLoad.merge(bestServer, fa.size(), Long::sum);
            serverAdditions.merge(bestServer, fa.size(), Long::sum);
        }

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
            long addedBytes = serverAdditions.getOrDefault(entry.getKey(), 0L);
            endpoints.add(createEndpoint(entry.getKey(), entry.getValue(), query, addedBytes));
            clusterService.addLoad(entry.getKey(), addedBytes);
        }
        return endpoints;
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
        LOGGER.debug("qid={} node={} planning=createEndpoint server={} files={} bytes={} query='{}'",
                LogUtil.qid(), LogUtil.node(), serverUri, filePaths.size(), bytes, query);
        URI parsedUri = URI.create(serverUri);
        // Preserve the registered URI (including grpc+tls). Reconstructing every
        // location as insecure both loses transport information and can cause the
        // client to send a ticket to the wrong connection.
        Location serverLoc = new Location(parsedUri);
        ByteString serverHandle = ByteString.copyFrom(
                randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        clusterService.storeHandle(serverHandle.toStringUtf8(),
                HandleState.forServerFiles(query, filePaths.toArray(new String[0]),
                        serverUri, bytes));
        Ticket serverTicket = new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(serverHandle).build())
                .toByteArray());
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
    private String findServerWithAllFiles(
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris) {
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
            return serverUri;
        }
        return null;
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
