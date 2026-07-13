package net.surpin.data.arrowflight.server.services;

import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;

import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
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
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;

        Map<String, Long> heartbeats = hazelcast.serverHeartbeats().getAll(serverUris);
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHb = heartbeats.get(uri);
            if (lastHb == null) {
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
        return live;
    }

    /**
     * Returns all registered server URIs and their current loads.
     *
     * @return map of URI to load (bytes)
     */
    public Map<String, Long> allServerLoads() {
        return hazelcast.serverRegistry().getAll(hazelcast.serverRegistry().keySet());
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
     * Adds delta bytes to server's tracked load.
     *
     * @param uri   server URI
     * @param delta bytes to add (positive or negative)
     */
    public void addLoad(String uri, long delta) {
        hazelcast.serverRegistry().compute(uri, (k, v) -> {
            if (v == null) {
                return delta;
            }
            long updated = v + delta;
            return updated <= 0 ? 0L : updated;
        });
    }

    /**
     * Stores a query handle in the distributed cache with 10-minute TTL.
     *
     * @param handle handle string
     * @param state  handle state
     */
    public void storeHandle(String handle, HandleState state) {
        hazelcast.statementCache().put(handle, state, 10, TimeUnit.MINUTES);
    }

    /**
     * Retrieves a handle state from the distributed cache.
     *
     * @param handle handle string
     * @return handle state, or null if not found
     */
    public HandleState getHandle(String handle) {
        return (HandleState) hazelcast.statementCache().get(handle);
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
