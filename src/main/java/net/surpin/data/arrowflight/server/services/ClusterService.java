package net.surpin.data.arrowflight.server.services;

import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;

import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;

/**
 * Manages server registration, heartbeats, load tracking, and statement cache lifecycle.
 * Coordinates with other cluster members via Hazelcast distributed maps.
 */
public final class ClusterService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

    private static final long HEARTBEAT_INTERVAL_SEC = 15;
    private static final long HEARTBEAT_TIMEOUT_SEC = 45;

    private final HazelcastAdapter hazelcast;
    private final AppConfig appConfig;
    private final String serverUri;
    private final ScheduledExecutorService heartbeatExecutor;

    /**
     * Creates a ClusterService for the given server URI.
     * Registers the server in the cluster and starts periodic heartbeat.
     *
     * @param hazelcast  Hazelcast adapter
     * @param appConfig  server configuration
     * @param serverUri  this server's URI string
     */
    public ClusterService(HazelcastAdapter hazelcast, AppConfig appConfig, String serverUri) {
        this.hazelcast = hazelcast;
        this.appConfig = appConfig;
        this.serverUri = serverUri;

        hazelcast.serverRegistry().put(serverUri, 0L);
        hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());

        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flight-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
                hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
            } catch (Exception e) {
                LOGGER.warn("Failed to update heartbeat for {}: {}", serverUri, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        hazelcast.onStatementExpired((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.serverUri() != null) {
                hazelcast.serverRegistry().compute(state.serverUri(), (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    long updated = v - state.bytes();
                    return updated <= 0 ? 0L : updated;
                });
            }
        });
    }

    /**
     * Filters out servers that haven't sent heartbeat within timeout.
     *
     * @param serverUris set of URIs to check
     * @return set of live server URIs
     */
    public Set<String> filterLiveServers(Set<String> serverUris) {
        long t = LogUtil.mark();
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;

        Map<String, Long> heartbeats = hazelcast.serverHeartbeats().getAll(serverUris);
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHb = heartbeats.get(uri);
            if (lastHb == null) {
                // Registration and the first heartbeat are separate distributed-map writes.
                // Keep a newly registered node eligible during that short window.
                live.add(uri);
            } else if (lastHb >= deadline) {
                live.add(uri);
            } else {
                LOGGER.warn("Server {} is stale (last heartbeat {}ms ago), removing from pool",
                        uri, now - lastHb);
                hazelcast.serverRegistry().remove(uri);
                hazelcast.serverHeartbeats().remove(uri);
            }
        }
        LogUtil.logTiming(t, "hazelcast.filterLiveServers", "total=" + serverUris.size() + " live=" + live.size());
        return live;
    }

    /**
     * Returns all registered server URIs and their current loads.
     *
     * @return map of URI to load (bytes)
     */
    public Map<String, Long> allServerLoads() {
        long t = LogUtil.mark();
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : hazelcast.serverRegistry().entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        LogUtil.logTiming(t, "hazelcast.allServerLoads", "servers=" + result.size());
        return result;
    }

    /**
     * Returns load for a specific set of server URIs.
     *
     * @param serverUris URIs to query
     * @return map of URI to load
     */
    public Map<String, Long> getLoads(Set<String> serverUris) {
        return hazelcast.serverRegistry().getAll(serverUris);
    }

    /**
     * Publishes the files stored on this node. Every path is relative to the node's
     * configured data directory.
     *
     * @param files relative file path to byte size
     */
    public void registerLocalFiles(Map<String, Long> files) {
        hazelcast.serverFiles().put(this.serverUri, new LinkedHashMap<>(files));
        LOGGER.info("Registered {} local Parquet file(s) for {}", files.size(), this.serverUri);
    }

    /**
     * Merges all published node inventories into planner file assignments.
     * Duplicate relative paths are treated as replicas and retain every owner.
     *
     * @return relative path to size and owning servers
     */
    public Map<String, FileAssignment> fileLocations() {
        long t = LogUtil.mark();
        Map<String, Map<String, Long>> inventories = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> entry : hazelcast.serverFiles().entrySet()) {
            inventories.put(entry.getKey(), entry.getValue());
        }
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> server : inventories.entrySet()) {
            String serverUri = server.getKey();
            for (Map.Entry<String, Long> file : server.getValue().entrySet()) {
                result.compute(file.getKey(), (path, current) -> {
                    Set<String> owners = new LinkedHashSet<>();
                    long size = file.getValue();
                    if (current != null) {
                        if (current.size() != size) {
                            throw new IllegalStateException(
                                    "Conflicting sizes for replicated file " + path);
                        }
                        owners.addAll(current.hosts());
                    }
                    owners.add(serverUri);
                    return new FileAssignment(size, owners);
                });
            }
        }
        LogUtil.logTiming(t, "hazelcast.fileLocations", "files=" + result.size());
        return result;
    }

    /**
     * Checks whether a server published its local inventory, including an empty one.
     */
    public boolean hasFileInventory(String serverUri) {
        return hazelcast.serverFiles().containsKey(serverUri);
    }

    /**
     * Adds delta bytes to server's tracked load.
     *
     * @param uri   server URI
     * @param delta bytes to add (positive or negative)
     */
    public void addLoad(String uri, long delta) {
        long t = LogUtil.mark();
        hazelcast.serverRegistry().compute(uri, (k, v) -> {
            if (v == null) {
                return delta;
            }
            long updated = v + delta;
            return updated <= 0 ? 0L : updated;
        });
        LogUtil.logTiming(t, "hazelcast.addLoad", "uri=" + uri + " delta=" + delta);
    }

    /**
     * Stores a query handle in the distributed cache with 10-minute TTL.
     *
     * @param handle handle string
     * @param state  handle state
     */
    public void storeHandle(String handle, HandleState state) {
        long t = LogUtil.mark();
        hazelcast.statementCache().put(handle, state, 10, TimeUnit.MINUTES);
        LogUtil.logTiming(t, "hazelcast.storeHandle");
    }

    /**
     * Retrieves a handle state from the distributed cache.
     *
     * @param handle handle string
     * @return handle state, or null if not found
     */
    public HandleState getHandle(String handle) {
        long t = LogUtil.mark();
        HandleState state = (HandleState) hazelcast.statementCache().get(handle);
        LogUtil.logTiming(t, "hazelcast.getHandle", "found=" + (state != null));
        return state;
    }

    /**
     * Removes a handle from the distributed cache.
     *
     * @param handle handle string
     */
    public void removeHandle(String handle) {
        hazelcast.statementCache().remove(handle);
    }

    /**
     * Returns this server's URI.
     *
     * @return server URI string
     */
    public String serverUri() {
        return serverUri;
    }

    /**
     * Returns the underlying Hazelcast instance for cluster management.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance getHazelcastInstance() {
        return hazelcast.instance();
    }

    @Override
    public void close() {
        try {
            hazelcast.serverRegistry().remove(serverUri);
            hazelcast.serverHeartbeats().remove(serverUri);
        } catch (Exception e) {
            LOGGER.warn("Failed to deregister server from cluster: {}", e.getMessage());
        }
        heartbeatExecutor.shutdownNow();
        try {
            hazelcast.close();
        } catch (Exception e) {
            LOGGER.error("Error closing Hazelcast adapter", e);
        }
    }
}
