package net.surpin.data.arrowflight.server.services;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import net.surpin.data.arrowflight.server.adapters.HostUtils;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.NodeLoadSnapshot;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reassigns a claimed endpoint to a meaningfully less-loaded Flight node.
 */
public final class TaskRedirectService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(TaskRedirectService.class);

    private final ClusterService clusterService;
    private final ParquetAdapter parquetAdapter;
    private final SchedulerConfig config;

    /**
     * Contains the replacement URI, outer Flight ticket, and redirect hop count.
     *
     * @param targetUri replacement Flight server URI
     * @param ticket replacement outer Flight ticket bytes
     * @param redirectCount redirect hop count
     */
    public record Redirect(
            String targetUri,
            byte[] ticket,
            int redirectCount) {
    }

    /**
     * Creates a cross-node endpoint redirect service.
     *
     * @param clusterService cluster state and reservation service
     * @param parquetAdapter Parquet storage configuration
     * @param config adaptive scheduler configuration
     */
    public TaskRedirectService(
            ClusterService clusterService,
            ParquetAdapter parquetAdapter,
            SchedulerConfig config) {
        this.clusterService = clusterService;
        this.parquetAdapter = parquetAdapter;
        this.config = config;
    }

    /**
     * Attempts to atomically move a claimed endpoint to a better live node.
     *
     * @param sourceHandle currently claimed statement-handle bytes
     * @param state decoded source endpoint state
     * @return replacement endpoint when a sufficiently better target exists
     */
    public Optional<Redirect> tryRedirect(
            byte[] sourceHandle, HandleState state) {
        if (!eligible(state)) {
            return Optional.empty();
        }
        String sourceUri = clusterService.serverUri();
        Map<String, Long> loads =
                new HashMap<>(clusterService.allServerLoads());
        if (!loads.containsKey(sourceUri)) {
            return Optional.empty();
        }
        Set<String> live = clusterService.filterLiveServers(loads.keySet());
        loads.keySet().retainAll(live);
        Map<String, NodeLoadSnapshot> snapshots =
                new HashMap<>(clusterService.nodeSnapshots(live));
        long now = System.currentTimeMillis();
        snapshots.entrySet().removeIf(entry ->
                !entry.getValue().isFresh(now, config.snapshotStaleMillis()));
        NodeLoadSnapshot sourceSnapshot = snapshots.get(sourceUri);
        if (!sourcePressured(sourceSnapshot)) {
            return Optional.empty();
        }

        Set<String> fullyLocalServers = fullyLocalServers(
                state.filePaths(), live);
        boolean sharedStorage = QueryPlanner.usesSharedStorage(
                parquetAdapter.dataDirectory());
        Set<String> candidates = snapshots.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(sourceUri))
                .filter(entry -> entry.getValue().acceptingRequests())
                .map(Map.Entry::getKey)
                .filter(uri -> sharedStorage
                        || fullyLocalServers.contains(uri))
                .collect(Collectors.toSet());
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        Set<String> normalizedLocalServers = fullyLocalServers.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());
        double sourceScore = QueryPlanner.schedulingScore(
                sourceUri, normalizedLocalServers,
                loads.getOrDefault(sourceUri, 0L),
                0L, sourceSnapshot, config);
        String targetUri = candidates.stream()
                .min((left, right) -> {
                    int scoreComparison = Double.compare(
                            targetScore(
                                    left, normalizedLocalServers,
                                    loads, snapshots, state.bytes()),
                            targetScore(
                                    right, normalizedLocalServers,
                                    loads, snapshots, state.bytes()));
                    return scoreComparison != 0
                            ? scoreComparison : left.compareTo(right);
                })
                .orElseThrow();
        double targetScore = targetScore(
                targetUri, normalizedLocalServers,
                loads, snapshots, state.bytes());
        double requiredScore = sourceScore
                * (1.0 - config.redirectMinScoreImprovement());
        if (!(targetScore < requiredScore)) {
            return Optional.empty();
        }

        Optional<ClusterService.RedirectedEndpoint> transferred =
                clusterService.redirectEndpoint(
                        sourceHandle, state, targetUri);
        if (transferred.isEmpty()) {
            return Optional.empty();
        }
        ClusterService.RedirectedEndpoint endpoint = transferred.orElseThrow();
        byte[] ticket = outerTicket(endpoint.handle());
        MetricsService.recordRedirect();
        LOGGER.info(
                "Redirected Flight endpoint from {} to {} hop={} bytes={} "
                        + "sourceScore={} targetScore={}",
                sourceUri, targetUri, endpoint.state().redirectCount(),
                state.bytes(), sourceScore, targetScore);
        return Optional.of(new Redirect(
                targetUri, ticket, endpoint.state().redirectCount()));
    }

    /**
     * Returns whether an endpoint has remaining redirect hops.
     *
     * @param state endpoint state
     * @return whether redirect evaluation is allowed
     */
    public boolean canRedirect(HandleState state) {
        return eligible(state);
    }

    /**
     * Checks static endpoint and configuration requirements for redirect.
     *
     * @param state endpoint state
     * @return whether redirect evaluation is allowed
     */
    private boolean eligible(HandleState state) {
        return config.enabled()
                && config.redirectEnabled()
                && state.loadTracked()
                && state.serverUri() != null
                && state.serverUri().equals(clusterService.serverUri())
                && state.filePaths() != null
                && state.redirectCount() < config.maxRedirects();
    }

    /**
     * Checks whether local runtime pressure justifies moving assigned work.
     *
     * @param snapshot current local node snapshot
     * @return whether the local endpoint should consider another node
     */
    private boolean sourcePressured(NodeLoadSnapshot snapshot) {
        if (snapshot == null || !snapshot.acceptingRequests()) {
            return snapshot != null;
        }
        double cpu = Math.max(
                snapshot.processCpuLoad(), snapshot.systemCpuLoad());
        return snapshot.activeQueries() >= snapshot.concurrencyLimit()
                || snapshot.queuedQueries() > 0
                || cpu >= config.cpuHighWatermark()
                || snapshot.memoryPressure()
                        >= config.memoryHighWatermark();
    }

    /**
     * Finds live servers containing every assigned file.
     *
     * @param filePaths assigned relative file paths
     * @param liveServers current live Flight server URIs
     * @return servers with complete local coverage
     */
    private Set<String> fullyLocalServers(
            String[] filePaths, Set<String> liveServers) {
        Set<String> result = new HashSet<>(liveServers);
        Map<String, FileAssignment> locations =
                clusterService.fileLocations();
        for (String filePath : filePaths) {
            FileAssignment assignment = locations.get(filePath);
            if (assignment == null) {
                result.clear();
                break;
            }
            result.retainAll(assignment.hosts());
        }
        return result;
    }

    /**
     * Computes the predicted completion score after assigning work to a target.
     *
     * @param uri candidate target URI
     * @param normalizedLocalServers servers with all files local
     * @param loads current reserved bytes
     * @param snapshots current node snapshots
     * @param taskBytes endpoint bytes to transfer
     * @return target scheduling score
     */
    private double targetScore(
            String uri,
            Set<String> normalizedLocalServers,
            Map<String, Long> loads,
            Map<String, NodeLoadSnapshot> snapshots,
            long taskBytes) {
        return QueryPlanner.schedulingScore(
                uri, normalizedLocalServers,
                loads.getOrDefault(uri, 0L),
                taskBytes, snapshots.get(uri), config);
    }

    /**
     * Wraps a signed statement handle in the outer Flight SQL ticket.
     *
     * @param handle signed statement handle
     * @return serialized outer Flight ticket
     */
    private static byte[] outerTicket(byte[] handle) {
        return new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(ByteString.copyFrom(handle))
                        .build())
                .toByteArray()).getBytes();
    }
}
