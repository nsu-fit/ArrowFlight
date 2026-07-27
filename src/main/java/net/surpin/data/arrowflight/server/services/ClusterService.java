package net.surpin.data.arrowflight.server.services;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTaskContext;
import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.CapacityExhaustedException;
import net.surpin.data.arrowflight.server.model.ExecutionReservationRequest;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.QueueKey;
import net.surpin.data.arrowflight.server.model.QueueNode;
import net.surpin.data.arrowflight.server.model.ReservationState;
import net.surpin.data.arrowflight.server.model.ReservationStatus;
import net.surpin.data.arrowflight.server.model.ServerCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Manages server registration, distributed execution capacity, and statement lifecycle.
 */
public final class ClusterService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);
    private static final long HEARTBEAT_INTERVAL_SEC = 15;
    private static final long HEARTBEAT_TIMEOUT_SEC = 45;
    private static final long UNCLAIMED_HANDLE_TTL_MINUTES = 10;
    private static final long HANDLE_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final int TICKET_SECRET_BYTES = 32;
    private static final String TICKET_SECRET_KEY = "__arrowflight-ticket-secret-v1";
    private static final String TICKET_LOAD_PREFIX = "__arrowflight-ticket-load-v1:";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final HazelcastAdapter hazelcast;
    private final AppConfig appConfig;
    private final String serverUri;
    private final ScheduledExecutorService heartbeatExecutor;
    private final EndpointTicketCodec endpointTicketCodec;
    private final Map<String, LocalHandle> localHandles = new ConcurrentHashMap<>();

    /**
     * Associates locally cached state with its expiration deadline.
     *
     * @param state handle state
     * @param expiresAtNanos monotonic expiration deadline
     */
    private record LocalHandle(HandleState state, long expiresAtNanos) {
    }

    /**
     * Creates a cluster service and atomically recovers stale state for the local URI.
     *
     * @param hazelcast Hazelcast adapter
     * @param appConfig server configuration
     * @param serverUri local Flight URI
     */
    public ClusterService(HazelcastAdapter hazelcast, AppConfig appConfig, String serverUri) {
        this.hazelcast = hazelcast;
        this.appConfig = appConfig;
        this.serverUri = serverUri;
        this.endpointTicketCodec = new EndpointTicketCodec(ticketSecret(hazelcast));

        if (hazelcast.instance() != null && hazelcast.reservations() != null
            && hazelcast.queueNodes() != null) {
            cleanupServer(serverUri);
        }
        // Heartbeat first: liveness must never depend on the (slower, lock-contending)
        // state-repair transaction below.
        hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
        registerLocalServerState();

        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "flight-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            // Always renew the heartbeat first. This is a single non-transactional put and
            // must not be blocked by lock contention on the registry/capacity maps, or a
            // perfectly live node can be marked stale and evicted from the pool.
            try {
                hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
                registerLocalServerState();
                removeExpiredLocalEntries();
            } catch (Exception error) {
                LOGGER.warn("Failed to repair registry state for {}: {}",
                    serverUri, error.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        hazelcast.onStatementExpired((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.reservationId() != null) {
                releaseExecution(state.reservationId());
            } else if (value instanceof HandleState state && state.serverUri() != null
                    && state.loadTracked()) {
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
     * Filters stale servers and releases every reservation owned by them.
     *
     * @param serverUris server URIs to inspect
     * @return live server URIs
     */
    public Set<String> filterLiveServers(Set<String> serverUris) {
        long started = LogUtil.mark();
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;
        Map<String, Long> heartbeats = hazelcast.serverHeartbeats().getAll(serverUris);
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHeartbeat = heartbeats.get(uri);
            if (lastHeartbeat == null || lastHeartbeat >= deadline) {
                live.add(uri);
            } else {
                LOGGER.warn("Server {} is stale (last heartbeat {}ms ago), removing from pool",
                        uri, now - lastHeartbeat);
                if (hazelcast.instance() != null && hazelcast.reservations() != null
                        && hazelcast.queueNodes() != null) {
                    cleanupServer(uri);
                } else {
                    hazelcast.serverRegistry().remove(uri);
                }
                hazelcast.serverHeartbeats().remove(uri);
                if (serverUri.equals(uri)) {
                    registerLocalServerState();
                    hazelcast.serverHeartbeats().put(
                            serverUri, System.currentTimeMillis());
                }
            }
        }
        LogUtil.logTiming(started, "hazelcast.filterLiveServers",
                "total=" + serverUris.size() + " live=" + live.size());
        return live;
    }

    /**
     * Returns all server logical loads.
     *
     * @return server URI to logical input bytes
     */
    public Map<String, Long> allServerLoads() {
        Map<String, Long> result = new LinkedHashMap<>();
        hazelcast.serverRegistry().entrySet().forEach(
                entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    /**
     * Returns logical loads for selected servers.
     *
     * @param serverUris server URIs
     * @return server load map
     */
    public Map<String, Long> getLoads(Set<String> serverUris) {
        return hazelcast.serverRegistry().getAll(serverUris);
    }

    /**
     * Returns capacity snapshots for selected servers.
     *
     * @param serverUris server URIs
     * @return capacity snapshots
     */
    public Map<String, ServerCapacity> getCapacities(Set<String> serverUris) {
        return hazelcast.serverCapacity().getAll(serverUris);
    }

    /**
     * Publishes the files stored on the local node.
     *
     * @param files relative path to byte size
     */
    public void registerLocalFiles(Map<String, Long> files) {
        hazelcast.serverFiles().put(serverUri, new LinkedHashMap<>(files));
        LOGGER.info("Registered {} local Parquet file(s) for {}", files.size(), serverUri);
    }

    /**
     * Merges published inventories into replicated file assignments.
     *
     * @return relative path to file assignment
     */
    public Map<String, FileAssignment> fileLocations() {
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> server
                : hazelcast.serverFiles().entrySet()) {
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
                    owners.add(server.getKey());
                    return new FileAssignment(size, owners);
                });
            }
        }
        return result;
    }

    /**
     * Checks whether a server published its file inventory.
     *
     * @param uri server URI
     * @return whether an inventory exists
     */
    public boolean hasFileInventory(String uri) {
        return hazelcast.serverFiles().containsKey(uri);
    }

    /**
     * Adds logical bytes to a server load counter.
     *
     * @param uri server URI
     * @param delta byte delta
     */
    public void addLoad(String uri, long delta) {
        hazelcast.serverRegistry().compute(uri, (key, value) ->
                addLoadValue(value, delta));
    }

    /**
     * Atomically creates all endpoint reservations and unclaimed handles.
     *
     * @param requests endpoint reservation requests
     * @return reservations in request order
     * @throws CapacityExhaustedException if any target queue is full
     */
    public List<ReservationState> reserveExecutions(
            List<ExecutionReservationRequest> requests) {
        if (requests.isEmpty()) {
            return List.of();
        }
        List<ExecutionReservationRequest> ordered = new ArrayList<>(requests);
        ordered.sort(Comparator.comparing(ExecutionReservationRequest::serverUri));
        Map<String, ReservationState> byHandle;
        try {
            byHandle = withTransaction(context -> {
            TransactionalMap<String, ServerCapacity> capacities =
                    context.getMap(hazelcast.serverCapacityMapName());
            TransactionalMap<String, ReservationState> reservations =
                    context.getMap(hazelcast.reservationMapName());
            TransactionalMap<QueueKey, QueueNode> queue =
                    context.getMap(hazelcast.queueNodeMapName());
            TransactionalMap<String, Serializable> handles =
                    context.getMap(hazelcast.statementCacheMapName());
            TransactionalMap<String, Long> loads =
                    context.getMap(hazelcast.serverRegistryMapName());

            for (String uri : ordered.stream()
                    .map(ExecutionReservationRequest::serverUri)
                    .distinct().toList()) {
                ServerCapacity capacity = capacities.getForUpdate(uri);
                Long load = loads.getForUpdate(uri);
                if (capacity == null || load == null) {
                    throw new CapacityExhaustedException(
                            "Execution server is no longer available: " + uri);
                }
            }
            Map<String, ReservationState> created = new LinkedHashMap<>();
            for (ExecutionReservationRequest request : ordered) {
                String uri = request.serverUri();
                ServerCapacity capacity = capacities.get(uri);
                long admitted = (long) capacity.activeSlots()
                        + capacity.queuedQueries() + capacity.pendingQueries();
                long maximum = (long) capacity.maxActiveSlots()
                        + capacity.maxQueuedQueries();
                if (admitted >= maximum) {
                    throw new CapacityExhaustedException(
                            "Execution queue is full for " + uri);
                }
                capacity = new ServerCapacity(
                        capacity.activeSlots(),
                        capacity.queuedQueries(),
                        capacity.pendingQueries() + 1,
                        capacity.maxActiveSlots(),
                        capacity.maxQueuedQueries(),
                        capacity.nextSequence(),
                        capacity.headSequence(),
                        capacity.tailSequence());
                String reservationId = request.handle();
                HandleState requested = request.handleState();
                HandleState distributed = new HandleState(
                        requested.query(), requested.filePaths(), uri,
                        requested.bytes(), reservationId, requested.loadTracked());
                ReservationState reservation = new ReservationState(
                        reservationId, uri, request.handle(),
                        ReservationStatus.PENDING,
                        requested.bytes(), null, clusterTimeMillis(), false);
                capacities.put(uri, capacity);
                reservations.put(reservationId, reservation);
                handles.put(request.handle(), distributed,
                        UNCLAIMED_HANDLE_TTL_MINUTES, TimeUnit.MINUTES);
                loads.put(uri, addLoadValue(loads.getForUpdate(uri), requested.bytes()));
                created.put(request.handle(), reservation);
            }
                return created;
            });
        } catch (CapacityExhaustedException exhausted) {
            MetricsService.recordQueueRejection();
            throw exhausted;
        }
        List<ReservationState> result = new ArrayList<>(requests.size());
        for (ExecutionReservationRequest request : requests) {
            result.add(byHandle.get(request.handle()));
        }
        return List.copyOf(result);
    }

    /**
     * Claims an execution handle and removes its expiry.
     *
     * @param reservationId reservation identifier
     * @return current reservation, or null when it has expired
     */
    public ReservationState claimExecution(String reservationId) {
        return withTransaction(context -> {
            TransactionalMap<String, Serializable> handles =
                    context.getMap(hazelcast.statementCacheMapName());
            TransactionalMap<String, ReservationState> reservations =
                    context.getMap(hazelcast.reservationMapName());
            Serializable value = handles.getForUpdate(reservationId);
            ReservationState reservation = reservations.getForUpdate(reservationId);
            if (!(value instanceof HandleState) || reservation == null) {
                return null;
            }
            if (reservation.claimed()) {
                handles.put(reservationId, value);
                return reservation;
            }
            TransactionalMap<String, ServerCapacity> capacities =
                    context.getMap(hazelcast.serverCapacityMapName());
            TransactionalMap<QueueKey, QueueNode> queue =
                    context.getMap(hazelcast.queueNodeMapName());
            ServerCapacity capacity = capacities.getForUpdate(
                    reservation.serverUri());
            if (capacity == null) {
                return null;
            }
            handles.put(reservationId, value);
            capacity = new ServerCapacity(
                    capacity.activeSlots(),
                    capacity.queuedQueries(),
                    Math.max(0, capacity.pendingQueries() - 1),
                    capacity.maxActiveSlots(),
                    capacity.maxQueuedQueries(),
                    capacity.nextSequence(),
                    capacity.headSequence(),
                    capacity.tailSequence());
            ReservationState claimed;
            if (capacity.activeSlots() < capacity.maxActiveSlots()) {
                claimed = reservation.claim().activate();
                capacity = new ServerCapacity(
                        capacity.activeSlots() + 1,
                        capacity.queuedQueries(),
                        capacity.pendingQueries(),
                        capacity.maxActiveSlots(),
                        capacity.maxQueuedQueries(),
                        capacity.nextSequence(),
                        capacity.headSequence(),
                        capacity.tailSequence());
            } else {
                long sequence = capacity.nextSequence();
                claimed = reservation.queue(sequence);
                capacity = enqueue(queue, reservation.serverUri(),
                        sequence, capacity, reservationId);
            }
            capacities.put(reservation.serverUri(), capacity);
            reservations.put(reservationId, claimed);
            return claimed;
        });
    }

    /**
     * Claims an execution reservation using the legacy handle argument.
     *
     * @param handle legacy statement handle
     * @param reservationId reservation identifier
     * @return current reservation, or null when it has expired
     */
    public ReservationState claimExecution(String handle, String reservationId) {
        return claimExecution(reservationId);
    }

    /**
     * Registers a cross-member reservation listener before re-reading current state.
     *
     * @param reservationId reservation identifier
     * @param observer state observer receiving null after removal
     * @return listener registration identifier
     */
    public UUID watchReservation(String reservationId,
            Consumer<ReservationState> observer) {
        ReservationMapListener listener = new ReservationMapListener() {
            @Override
            public void entryUpdated(EntryEvent<String, ReservationState> event) {
                observer.accept(event.getValue());
            }

            @Override
            public void entryRemoved(EntryEvent<String, ReservationState> event) {
                observer.accept(null);
            }
        };
        UUID listenerId = hazelcast.reservations().addEntryListener(
                listener, reservationId, true);
        observer.accept(hazelcast.reservations().get(reservationId));
        return listenerId;
    }

    /**
     * Removes a reservation listener.
     *
     * @param listenerId listener registration identifier
     */
    public void removeReservationListener(UUID listenerId) {
        if (listenerId != null) {
            hazelcast.reservations().removeEntryListener(listenerId);
        }
    }

    /**
     * Idempotently completes or cancels one execution.
     *
     * @param reservationId reservation identifier
     */
    public void releaseExecution(String reservationId) {
        releaseExecutions(List.of(reservationId));
    }

    /**
     * Atomically releases multiple execution reservations.
     *
     * @param reservationIds reservation identifiers
     */
    public void releaseExecutions(Collection<String> reservationIds) {
        if (reservationIds.isEmpty()) {
            return;
        }
        withTransaction(context -> {
            TransactionalMap<String, ReservationState> reservations =
                    context.getMap(hazelcast.reservationMapName());
            List<ReservationState> states = reservationIds.stream()
                    .map(reservations::getForUpdate)
                    .filter(java.util.Objects::nonNull)
                    .sorted(Comparator.comparing(ReservationState::serverUri))
                    .toList();
            TransactionalMap<String, ServerCapacity> capacities =
                    context.getMap(hazelcast.serverCapacityMapName());
            states.stream().map(ReservationState::serverUri)
                    .distinct().forEach(capacities::getForUpdate);
            for (ReservationState state : states) {
                ReservationState current =
                        reservations.get(state.reservationId());
                if (current != null) {
                    releaseOne(context, current);
                }
            }
            return null;
        });
    }

    /**
     * Stores a non-execution handle with the planning TTL.
     *
     * @param handle handle string
     * @param state handle state
     */
    public void storeHandle(String handle, HandleState state) {
        long t = LogUtil.mark();
        localHandles.put(handle, new LocalHandle(
                state, System.nanoTime() + HANDLE_TTL_NANOS));
        hazelcast.statementCache().put(handle, state, 10, TimeUnit.MINUTES);
        LogUtil.logTiming(t, "hazelcast.storeHandle");
    }

    /**
     * Retrieves a statement handle.
     *
     * @param handle handle string
     * @return handle state, or null
     */
    public HandleState getHandle(String handle) {
        long t = LogUtil.mark();
        LocalHandle local = localHandles.get(handle);
        if (local != null) {
            if (local.expiresAtNanos() > System.nanoTime()) {
                LogUtil.logTiming(t, "local.getHandle", "found=true");
                return local.state();
            }
            localHandles.remove(handle, local);
        }
        HandleState state = (HandleState) hazelcast.statementCache().get(handle);
        if (state != null) {
            localHandles.put(handle, new LocalHandle(
                    state, System.nanoTime() + HANDLE_TTL_NANOS));
        }
        LogUtil.logTiming(t, "hazelcast.getHandle", "found=" + (state != null));
        return state;
    }

    /**
     * Creates a signed self-contained handle for a server-directed Flight endpoint.
     *
     * @param state endpoint execution state
     * @return authenticated handle bytes
     */
    public byte[] createEndpointHandle(HandleState state) {
        byte[] handle = endpointTicketCodec.encode(state);
        if (state.reservationId() == null && state.loadTracked()) {
            hazelcast.statementCache().put(
                    loadReservationKey(handle), state, 10, TimeUnit.MINUTES);
        }
        return handle;
    }

    /**
     * Resolves a signed endpoint handle or a legacy distributed UUID handle.
     *
     * @param handle ticket statement-handle bytes
     * @return endpoint state, or null for an unknown legacy handle
     */
    public HandleState resolveEndpointHandle(byte[] handle) {
        if (endpointTicketCodec.isEncoded(handle)) {
            return endpointTicketCodec.decode(handle);
        }
        return getHandle(new String(handle, StandardCharsets.UTF_8));
    }

    /**
     * Releases distributed load once for a completed signed endpoint ticket.
     *
     * @param handle ticket statement-handle bytes
     * @param state completed endpoint state
     */
    public void releaseEndpointLoad(byte[] handle, HandleState state) {
        if (state.reservationId() != null || !state.loadTracked()
                || state.serverUri() == null || state.bytes() == 0L) {
            return;
        }
        Serializable reservation = hazelcast.statementCache()
                .remove(loadReservationKey(handle));
        if (reservation instanceof HandleState reserved) {
            addLoad(reserved.serverUri(), -reserved.bytes());
        }
    }

    /**
     * Removes a handle from the distributed cache.
     *
     * @param handle handle string
     */
    public void removeHandle(String handle) {
        localHandles.remove(handle);
        hazelcast.statementCache().remove(handle);
    }

    /**
     * Obtains the cluster-shared secret used to authenticate endpoint tickets.
     *
     * @param hazelcast Hazelcast adapter
     * @return cluster-shared secret bytes
     */
    private static byte[] ticketSecret(HazelcastAdapter hazelcast) {
        byte[] candidate = new byte[TICKET_SECRET_BYTES];
        SECURE_RANDOM.nextBytes(candidate);
        Serializable existing = hazelcast.statementCache()
                .putIfAbsent(TICKET_SECRET_KEY, candidate);
        if (existing == null) {
            return candidate;
        }
        if (existing instanceof byte[] secret && secret.length >= TICKET_SECRET_BYTES) {
            return secret.clone();
        }
        throw new IllegalStateException("Invalid cluster Flight ticket secret");
    }

    /**
     * Creates a compact stable key for a distributed load reservation.
     *
     * @param handle ticket statement-handle bytes
     * @return base64-encoded SHA-256 digest
     */
    private static String loadReservationKey(byte[] handle) {
        try {
            return TICKET_LOAD_PREFIX + Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-256").digest(handle));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Removes expired local handle and idempotency entries.
     */
    private void removeExpiredLocalEntries() {
        long now = System.nanoTime();
        localHandles.entrySet().removeIf(
                entry -> entry.getValue().expiresAtNanos() <= now);
    }

    /**
     * Returns this server's URI.
     *
     * @return server URI
     */
    public String serverUri() {
        return serverUri;
    }

    /**
     * Returns cluster time for distributed queue-wait measurements.
     *
     * @return cluster epoch time in milliseconds
     */
    public long clusterTimeMillis() {
        return hazelcast.instance().getCluster().getClusterTime();
    }

    /**
     * Returns local execution capacity for metrics.
     *
     * @return local capacity state
     */
    public ServerCapacity localCapacity() {
        return hazelcast.serverCapacity().get(serverUri);
    }

    /**
     * Returns the underlying Hazelcast instance.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance getHazelcastInstance() {
        return hazelcast.instance();
    }

    @Override
    public void close() {
        heartbeatExecutor.shutdownNow();
        localHandles.clear();
        try {
            if (hazelcast.instance() != null && hazelcast.reservations() != null
                    && hazelcast.queueNodes() != null) {
                cleanupServer(serverUri);
            } else {
                hazelcast.serverRegistry().remove(serverUri);
            }
            hazelcast.serverHeartbeats().remove(serverUri);
        } catch (Exception error) {
            LOGGER.warn("Failed to deregister server from cluster: {}", error.getMessage());
        }
        hazelcast.close();
    }

    /**
     * Creates local empty capacity from resolved configuration.
     *
     * @return empty capacity
     */
    private ServerCapacity emptyCapacity() {
        return ServerCapacity.empty(
                appConfig.maxConcurrentQueries(), appConfig.maxQueuedQueries());
    }

    /**
     * Atomically restores local load and capacity after stale-state cleanup.
     */
    private void registerLocalServerState() {
        if (hazelcast.instance() == null || hazelcast.serverCapacity() == null) {
            hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
            return;
        }
        // Cheap, non-locking fast path: skip the transaction entirely once both entries
        // exist (the common case on every heartbeat tick after the first successful init).
        // Avoids contending for the same key-lock that query reservation/release transactions use.
        if (hazelcast.serverRegistry().containsKey(serverUri)
            && hazelcast.serverCapacity().containsKey(serverUri)) {
            return;
        }
        withTransaction(context -> {
            TransactionalMap<String, Long> loads =
                context.getMap(hazelcast.serverRegistryMapName());
            TransactionalMap<String, ServerCapacity> capacities =
                context.getMap(hazelcast.serverCapacityMapName());
            Long load = loads.getForUpdate(serverUri);
            ServerCapacity capacity = capacities.getForUpdate(serverUri);
            if (load == null) {
                loads.put(serverUri, 0L);
            }
            if (capacity == null) {
                capacities.put(serverUri, emptyCapacity());
            }
            return null;
        });
    }

    /**
     * Appends one node to a transactional FIFO.
     *
     * @param queue transactional queue-node map
     * @param uri target server URI
     * @param sequence node sequence
     * @param capacity current capacity
     * @param reservationId reservation identifier
     * @return updated capacity
     */
    private static ServerCapacity enqueue(
            TransactionalMap<QueueKey, QueueNode> queue,
            String uri,
            long sequence,
            ServerCapacity capacity,
            String reservationId) {
        Long tail = capacity.tailSequence();
        QueueNode node = new QueueNode(reservationId, tail, null);
        if (tail != null) {
            QueueKey tailKey = new QueueKey(uri, tail);
            QueueNode tailNode = queue.getForUpdate(tailKey);
            if (tailNode != null) {
                queue.put(tailKey, tailNode.withNext(sequence));
            }
        }
        queue.put(new QueueKey(uri, sequence), node);
        Long head = capacity.headSequence() == null
                ? sequence : capacity.headSequence();
        return new ServerCapacity(
                capacity.activeSlots(),
                capacity.queuedQueries() + 1,
                capacity.pendingQueries(),
                capacity.maxActiveSlots(),
                capacity.maxQueuedQueries(),
                sequence + 1,
                head,
                sequence);
    }

    /**
     * Removes one reservation and applies its capacity and load changes.
     *
     * @param context transaction context
     * @param state current reservation state
     */
    private void releaseOne(TransactionalTaskContext context,
            ReservationState state) {
        TransactionalMap<String, ServerCapacity> capacities =
                context.getMap(hazelcast.serverCapacityMapName());
        TransactionalMap<String, ReservationState> reservations =
                context.getMap(hazelcast.reservationMapName());
        TransactionalMap<QueueKey, QueueNode> queue =
                context.getMap(hazelcast.queueNodeMapName());
        TransactionalMap<String, Serializable> handles =
                context.getMap(hazelcast.statementCacheMapName());
        TransactionalMap<String, Long> loads =
                context.getMap(hazelcast.serverRegistryMapName());

        ServerCapacity capacity = capacities.get(state.serverUri());
        if (capacity == null) {
            capacity = emptyCapacity();
        }
        if (state.status() == ReservationStatus.PENDING) {
            capacity = new ServerCapacity(
                    capacity.activeSlots(),
                    capacity.queuedQueries(),
                    Math.max(0, capacity.pendingQueries() - 1),
                    capacity.maxActiveSlots(),
                    capacity.maxQueuedQueries(),
                    capacity.nextSequence(),
                    capacity.headSequence(),
                    capacity.tailSequence());
        } else if (state.status() == ReservationStatus.QUEUED) {
            capacity = unlinkQueued(queue, state, capacity);
        } else {
            capacity = releaseActive(queue, reservations, state, capacity);
        }
        capacities.put(state.serverUri(), capacity);
        reservations.remove(state.reservationId());
        handles.remove(state.handle());
        loads.put(state.serverUri(),
                addLoadValue(loads.getForUpdate(state.serverUri()),
                        -state.fileLoadBytes()));
    }

    /**
     * Unlinks one arbitrary queued reservation.
     *
     * @param queue transactional queue-node map
     * @param state queued reservation
     * @param capacity current capacity
     * @return updated capacity
     */
    private static ServerCapacity unlinkQueued(
            TransactionalMap<QueueKey, QueueNode> queue,
            ReservationState state,
            ServerCapacity capacity) {
        Long sequence = state.queueSequence();
        if (sequence == null) {
            return capacity;
        }
        QueueKey key = new QueueKey(state.serverUri(), sequence);
        QueueNode node = queue.getForUpdate(key);
        if (node == null) {
            return capacity;
        }
        if (node.previousSequence() != null) {
            QueueKey previousKey = new QueueKey(
                    state.serverUri(), node.previousSequence());
            QueueNode previous = queue.getForUpdate(previousKey);
            if (previous != null) {
                queue.put(previousKey, previous.withNext(node.nextSequence()));
            }
        }
        if (node.nextSequence() != null) {
            QueueKey nextKey = new QueueKey(state.serverUri(), node.nextSequence());
            QueueNode next = queue.getForUpdate(nextKey);
            if (next != null) {
                queue.put(nextKey, next.withPrevious(node.previousSequence()));
            }
        }
        queue.remove(key);
        Long head = sequence.equals(capacity.headSequence())
                ? node.nextSequence() : capacity.headSequence();
        Long tail = sequence.equals(capacity.tailSequence())
                ? node.previousSequence() : capacity.tailSequence();
        return new ServerCapacity(
                capacity.activeSlots(),
                Math.max(0, capacity.queuedQueries() - 1),
                capacity.pendingQueries(),
                capacity.maxActiveSlots(),
                capacity.maxQueuedQueries(),
                capacity.nextSequence(),
                head,
                tail);
    }

    /**
     * Releases an active slot and promotes the FIFO head when present.
     *
     * @param queue transactional queue-node map
     * @param reservations transactional reservation map
     * @param state active reservation
     * @param capacity current capacity
     * @return updated capacity
     */
    private static ServerCapacity releaseActive(
            TransactionalMap<QueueKey, QueueNode> queue,
            TransactionalMap<String, ReservationState> reservations,
            ReservationState state,
            ServerCapacity capacity) {
        int active = Math.max(0, capacity.activeSlots() - 1);
        Long headSequence = capacity.headSequence();
        if (headSequence == null) {
            return new ServerCapacity(
                    active, capacity.queuedQueries(),
                    capacity.pendingQueries(),
                    capacity.maxActiveSlots(), capacity.maxQueuedQueries(),
                    capacity.nextSequence(), null, capacity.tailSequence());
        }
        QueueKey headKey = new QueueKey(state.serverUri(), headSequence);
        QueueNode head = queue.getForUpdate(headKey);
        if (head == null) {
            return new ServerCapacity(
                    active, capacity.queuedQueries(),
                    capacity.pendingQueries(),
                    capacity.maxActiveSlots(), capacity.maxQueuedQueries(),
                    capacity.nextSequence(), null, null);
        }
        ReservationState promoted = reservations.getForUpdate(head.reservationId());
        if (promoted != null) {
            reservations.put(promoted.reservationId(), promoted.activate());
            active++;
        }
        queue.remove(headKey);
        Long nextSequence = head.nextSequence();
        if (nextSequence != null) {
            QueueKey nextKey = new QueueKey(state.serverUri(), nextSequence);
            QueueNode next = queue.getForUpdate(nextKey);
            if (next != null) {
                queue.put(nextKey, next.withPrevious(null));
            }
        }
        return new ServerCapacity(
                active,
                Math.max(0, capacity.queuedQueries() - 1),
                capacity.pendingQueries(),
                capacity.maxActiveSlots(),
                capacity.maxQueuedQueries(),
                capacity.nextSequence(),
                nextSequence,
                nextSequence == null ? null : capacity.tailSequence());
    }

    /**
     * Atomically removes all execution state for one server.
     *
     * @param uri server URI
     */
    private void cleanupServer(String uri) {
        withTransaction(context -> {
            TransactionalMap<String, ServerCapacity> capacities =
                    context.getMap(hazelcast.serverCapacityMapName());
            capacities.getForUpdate(uri);
            TransactionalMap<String, Long> loads =
                    context.getMap(hazelcast.serverRegistryMapName());
            loads.getForUpdate(uri);
            TransactionalMap<String, ReservationState> reservations =
                    context.getMap(hazelcast.reservationMapName());
            List<ReservationState> states = reservations.values().stream()
                    .filter(state -> uri.equals(state.serverUri()))
                    .map(state -> reservations.getForUpdate(
                            state.reservationId()))
                    .filter(java.util.Objects::nonNull)
                    .toList();
            for (ReservationState state : states) {
                ReservationState current =
                        reservations.get(state.reservationId());
                if (current != null) {
                    releaseOne(context, current);
                }
            }
            TransactionalMap<QueueKey, QueueNode> queue =
                    context.getMap(hazelcast.queueNodeMapName());
            List<QueueKey> queueKeys = queue.keySet().stream()
                    .filter(key -> uri.equals(key.serverUri()))
                    .toList();
            queueKeys.forEach(queue::remove);
            capacities.remove(uri);
            loads.remove(uri);
            return null;
        });
    }

    /**
     * Executes work in a two-phase Hazelcast transaction.
     *
     * @param work transactional callback
     * @param <T> result type
     * @return callback result
     */
    private <T> T withTransaction(TransactionWork<T> work) {
        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
        TransactionContext context = hazelcast.instance().newTransactionContext(options);
        context.beginTransaction();
        try {
            T result = work.apply(context);
            context.commitTransaction();
            return result;
        } catch (RuntimeException | Error failure) {
            try {
                context.rollbackTransaction();
            } catch (RuntimeException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        }
    }

    /**
     * Applies a saturating non-negative logical load delta.
     *
     * @param current current load
     * @param delta load delta
     * @return updated load
     */
    private static long addLoadValue(Long current, long delta) {
        long value = current == null ? 0L : current;
        if (delta > 0 && value > Long.MAX_VALUE - delta) {
            return Long.MAX_VALUE;
        }
        if (delta < 0 && value < -delta) {
            return 0L;
        }
        return Math.max(0L, value + delta);
    }

    /**
     * Listener contract used for key-specific cross-member reservation events.
     */
    private interface ReservationMapListener
            extends EntryUpdatedListener<String, ReservationState>,
            EntryRemovedListener<String, ReservationState> {
    }

    /**
     * Transaction callback with a return value.
     *
     * @param <T> callback result
     */
    @FunctionalInterface
    private interface TransactionWork<T> {

        /**
         * Executes transactional work.
         *
         * @param context transactional task context
         * @return callback result
         */
        T apply(TransactionalTaskContext context);
    }
}
