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
    }

    /**
     * Determines Flight endpoints for a SQL query, distributing files across live servers.
     * Handles JOIN queries specially — single-server if all files are local, per-table split otherwise.
     *
     * @param query SQL query
     * @return list of Flight endpoints
     * @throws IOException on file system error
     */
    public List<FlightEndpoint> determineEndpoints(String query)
            throws IOException {
        Map<String, FileAssignment> pathLocations = parquetAdapter.locationsForQuery(query);

        Set<String> allServerUris = new LinkedHashSet<>(clusterService.allServerLoads().keySet());
        if (allServerUris.isEmpty()) {
            LOGGER.info("Server registry is empty, falling back to self");
            allServerUris = Set.of(clusterService.serverUri());
        }

        allServerUris = clusterService.filterLiveServers(allServerUris);
        if (allServerUris.isEmpty()) {
            LOGGER.info("No live servers found, falling back to self");
            allServerUris = Set.of(clusterService.serverUri());
        }

        Map<String, Long> serverLoad = new HashMap<>(
                clusterService.getLoads(allServerUris));
        for (String uri : allServerUris) {
            serverLoad.putIfAbsent(uri, 0L);
        }

        ParquetQueryParser parsed = ParquetQueryParser.parse(query);

        if (parsed.isJoin) {
            String allFilesServer = findServerWithAllFiles(pathLocations, allServerUris);
            if (allFilesServer != null) {
                long addedBytes = pathLocations.values().stream()
                        .mapToLong(FileAssignment::size).sum();
                FlightEndpoint ep = createEndpoint(
                        allFilesServer, new ArrayList<>(pathLocations.keySet()), query, addedBytes);
                clusterService.addLoad(allFilesServer, addedBytes);
                return List.of(ep);
            }
            LOGGER.info("Join query spans multiple servers; splitting into per-table reads");
            List<FlightEndpoint> endpoints = new ArrayList<>();
            Map<String, Set<String>> seenTables = new LinkedHashMap<>();
            for (String path : pathLocations.keySet()) {
                String table = extractTableFromPath(path);
                seenTables.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(path);
            }
            for (Map.Entry<String, Set<String>> entry : seenTables.entrySet()) {
                String perTableQuery = "SELECT * FROM " + entry.getKey();
                Map<String, FileAssignment> tableLocations = new LinkedHashMap<>();
                for (String tablePath : entry.getValue()) {
                    tableLocations.put(tablePath, pathLocations.get(tablePath));
                }
                Map<String, List<String>> byServer = groupFilesByServer(tableLocations, serverLoad);
                for (Map.Entry<String, List<String>> e : byServer.entrySet()) {
                    long addedBytes = e.getValue().stream()
                            .mapToLong(p -> tableLocations.get(p).size())
                            .sum();
                    endpoints.add(createEndpoint(e.getKey(), e.getValue(), perTableQuery, addedBytes));
                    clusterService.addLoad(e.getKey(), addedBytes);
                }
            }
            return endpoints;
        }

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
            long addedBytes = serverAdditions.getOrDefault(entry.getKey(), 0L);
            endpoints.add(createEndpoint(entry.getKey(), entry.getValue(), query, addedBytes));
            clusterService.addLoad(entry.getKey(), addedBytes);
        }
        LOGGER.info("determineEndpoints: {} server(s), {} endpoint(s) for {} file(s), query: {}",
                allServerUris.size(), endpoints.size(), pathLocations.size(), query);
        return endpoints;
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
        URI parsedUri = URI.create(serverUri);
        Location serverLoc = Location.forGrpcInsecure(parsedUri.getHost(), parsedUri.getPort());
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
