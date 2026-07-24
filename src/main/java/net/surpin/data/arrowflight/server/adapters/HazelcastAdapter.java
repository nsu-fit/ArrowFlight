package net.surpin.data.arrowflight.server.adapters;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.QueueKey;
import net.surpin.data.arrowflight.server.model.QueueNode;
import net.surpin.data.arrowflight.server.model.ServerCapacity;
import net.surpin.data.arrowflight.server.model.ReservationState;

/**
 * Wraps HazelcastInstance lifecycle and distributed map access.
 */
public final class HazelcastAdapter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastAdapter.class);

    private static final String SERVER_REGISTRY_MAP = "server-registry";
    private static final String STATEMENT_CACHE_MAP = "statement-cache";
    private static final String SERVER_HEARTBEAT_MAP = "server-heartbeats";
    private static final String SERVER_FILES_MAP = "server-files";
    private static final String SERVER_CAPACITY_MAP = "server-capacity";
    private static final String RESERVATION_MAP = "execution-reservations";
    private static final String QUEUE_NODE_MAP = "execution-queue-nodes";

    private final HazelcastInstance instance;
    private final IMap<String, Long> serverRegistry;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, Long> serverHeartbeats;
    private final IMap<String, Map<String, Long>> serverFiles;
    private final IMap<String, ServerCapacity> serverCapacity;
    private final IMap<String, ReservationState> reservations;
    private final IMap<QueueKey, QueueNode> queueNodes;

    /**
     * Creates a new Hazelcast instance using TCP/IP join on the given hosts.
     *
     * @param appConfig server configuration
     * @param hosts cluster seed hosts
     */
    public HazelcastAdapter(AppConfig appConfig, String... hosts) {
        Config config = new Config();
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(appConfig.hazelcastPort());
        network.setPortAutoIncrement(false);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (String host : hosts) {
            tcpIpConfig.addMember(host);
        }
        tcpIpConfig.setEnabled(true);

        this.instance = Hazelcast.newHazelcastInstance(config);
        this.serverRegistry = instance.getMap(SERVER_REGISTRY_MAP);
        this.statementCache = instance.getMap(STATEMENT_CACHE_MAP);
        this.serverHeartbeats = instance.getMap(SERVER_HEARTBEAT_MAP);
        this.serverFiles = instance.getMap(SERVER_FILES_MAP);
        this.serverCapacity = instance.getMap(SERVER_CAPACITY_MAP);
        this.reservations = instance.getMap(RESERVATION_MAP);
        this.queueNodes = instance.getMap(QUEUE_NODE_MAP);
    }

    /**
     * Returns the distributed server registry (URI → load).
     */
    public IMap<String, Long> serverRegistry() {
        return serverRegistry;
    }

    /**
     * Returns the distributed statement cache (handle → state).
     */
    public IMap<String, Serializable> statementCache() {
        return statementCache;
    }

    /**
     * Returns the distributed heartbeat map (URI → timestamp).
     */
    public IMap<String, Long> serverHeartbeats() {
        return serverHeartbeats;
    }

    /**
     * Returns the distributed local-file inventory (server URI → relative path/size).
     */
    public IMap<String, Map<String, Long>> serverFiles() {
        return serverFiles;
    }

    /** Returns distributed execution capacity states. */
    public IMap<String, ServerCapacity> serverCapacity() {
        return serverCapacity;
    }

    /** Returns distributed execution reservations. */
    public IMap<String, ReservationState> reservations() {
        return reservations;
    }

    /**
     * Returns distributed FIFO nodes.
     *
     * @return execution queue nodes
     */
    public IMap<QueueKey, QueueNode> queueNodes() {
        return queueNodes;
    }

    /**
     * Returns the server registry map name for transactions.
     *
     * @return map name
     */
    public String serverRegistryMapName() {
        return SERVER_REGISTRY_MAP;
    }

    /**
     * Returns the statement cache map name for transactions.
     *
     * @return map name
     */
    public String statementCacheMapName() {
        return STATEMENT_CACHE_MAP;
    }

    /**
     * Returns the capacity map name for transactions.
     *
     * @return map name
     */
    public String serverCapacityMapName() {
        return SERVER_CAPACITY_MAP;
    }

    /**
     * Returns the reservation map name for transactions.
     *
     * @return map name
     */
    public String reservationMapName() {
        return RESERVATION_MAP;
    }

    /**
     * Returns the FIFO node map name for transactions.
     *
     * @return map name
     */
    public String queueNodeMapName() {
        return QUEUE_NODE_MAP;
    }

    /**
     * Registers an entry-expired listener on the statement cache.
     *
     * @param listener the listener to attach
     */
    public void onStatementExpired(EntryExpiredListener<String, Serializable> listener) {
        statementCache.addLocalEntryListener(listener);
    }

    /**
     * Returns the underlying HazelcastInstance for advanced operations.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance instance() {
        return instance;
    }

    @Override
    public void close() {
        instance.shutdown();
        LOGGER.info("Hazelcast instance shut down");
    }
}
