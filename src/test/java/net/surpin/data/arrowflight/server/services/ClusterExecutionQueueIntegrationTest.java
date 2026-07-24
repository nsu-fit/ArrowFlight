package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.CapacityExhaustedException;
import net.surpin.data.arrowflight.server.model.ExecutionReservationRequest;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.ReservationState;
import net.surpin.data.arrowflight.server.model.ReservationStatus;
import net.surpin.data.arrowflight.server.model.ServerCapacity;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises transactional FIFO behavior on two Hazelcast members.
 */
@Tag("integration")
class ClusterExecutionQueueIntegrationTest {

    /**
     * Verifies FIFO promotion, cross-member wakeup, idempotent release, and large files.
     *
     * @throws Exception on cluster startup or listener failure
     */
    @Test
    void promotesFifoAcrossMembersAndReleasesExactlyOnce() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        AppConfig firstConfig = config(firstPort, 2);
        AppConfig secondConfig = config(secondPort, 2);
        String firstUri = "grpc://127.0.0.1:32010";
        String secondUri = "grpc://127.0.0.1:32011";

        try (HazelcastAdapter firstAdapter = new HazelcastAdapter(firstConfig);
                ClusterService first = new ClusterService(
                        firstAdapter, firstConfig, firstUri);
                HazelcastAdapter secondAdapter = new HazelcastAdapter(
                        secondConfig, "127.0.0.1:" + firstPort);
                ClusterService second = new ClusterService(
                        secondAdapter, secondConfig, secondUri)) {
            List<ReservationState> states = second.reserveExecutions(List.of(
                    request(firstUri, "h1", 100L),
                    request(firstUri, "h2", 200L),
                    request(firstUri, "h3", 100_000_000_000L)));

            assertEquals(ReservationStatus.PENDING, states.get(0).status());
            assertEquals(ReservationStatus.PENDING, states.get(1).status());
            assertEquals(ReservationStatus.PENDING, states.get(2).status());
            assertTrue(secondAdapter.statementCache().getEntryView("h2")
                    .getExpirationTime() > second.clusterTimeMillis());
            assertEquals(ReservationStatus.ACTIVE,
                    second.claimExecution("h1", "h1").status());
            assertNotNull(second.claimExecution("h2", "h2"));
            assertEquals(ReservationStatus.QUEUED,
                    secondAdapter.reservations().get("h2").status());
            assertEquals(ReservationStatus.QUEUED,
                    second.claimExecution("h3", "h3").status());
            long claimedExpiration = secondAdapter.statementCache()
                    .getEntryView("h2").getExpirationTime();
            assertTrue(claimedExpiration == 0L
                    || claimedExpiration == Long.MAX_VALUE);

            CountDownLatch promoted = new CountDownLatch(1);
            UUID listenerId = second.watchReservation("h2", state -> {
                if (state != null && state.status() == ReservationStatus.ACTIVE) {
                    promoted.countDown();
                }
            });
            first.releaseExecution("h1");
            assertTrue(promoted.await(5, TimeUnit.SECONDS));
            second.removeReservationListener(listenerId);

            ServerCapacity afterPromotion =
                    firstAdapter.serverCapacity().get(firstUri);
            assertEquals(1, afterPromotion.activeSlots());
            assertEquals(1, afterPromotion.queuedQueries());
            assertEquals(0, afterPromotion.pendingQueries());

            first.releaseExecution("h1");
            assertEquals(afterPromotion,
                    firstAdapter.serverCapacity().get(firstUri));

            second.releaseExecution("h2");
            assertEquals(ReservationStatus.ACTIVE,
                    firstAdapter.reservations().get("h3").status());
            first.releaseExecution("h3");

            ServerCapacity empty = firstAdapter.serverCapacity().get(firstUri);
            assertEquals(0, empty.activeSlots());
            assertEquals(0, empty.queuedQueries());
            assertTrue(firstAdapter.queueNodes().isEmpty());
            assertFalse(firstAdapter.statementCache().containsKey("h1"));
            assertFalse(firstAdapter.statementCache().containsKey("h2"));
            assertFalse(firstAdapter.statementCache().containsKey("h3"));
            assertEquals(0L, firstAdapter.serverRegistry().get(firstUri));
        }
    }

    /**
     * Verifies arbitrary queued cancellation unlinks in constant state and enforces bounds.
     *
     * @throws Exception on cluster startup
     */
    @Test
    void unlinksQueuedCancellationAndRejectsBeyondBound() throws Exception {
        int port = freePort();
        AppConfig config = config(port, 2);
        String uri = "grpc://127.0.0.1:32020";
        try (HazelcastAdapter adapter = new HazelcastAdapter(config);
                ClusterService service = new ClusterService(adapter, config, uri)) {
            service.reserveExecutions(List.of(
                    request(uri, "a", 1L),
                    request(uri, "b", 1L),
                    request(uri, "c", 1L)));
            assertEquals(ReservationStatus.ACTIVE,
                    service.claimExecution("a", "a").status());
            assertEquals(ReservationStatus.QUEUED,
                    service.claimExecution("b", "b").status());
            assertEquals(ReservationStatus.QUEUED,
                    service.claimExecution("c", "c").status());
            service.releaseExecution("b");

            ServerCapacity oneQueued = adapter.serverCapacity().get(uri);
            assertEquals(1, oneQueued.queuedQueries());
            assertEquals(oneQueued.headSequence(), oneQueued.tailSequence());
            assertNull(adapter.reservations().get("b"));

            service.reserveExecutions(List.of(request(uri, "d", 1L)));
            assertThrows(CapacityExhaustedException.class,
                    () -> service.reserveExecutions(
                            List.of(request(uri, "e", 1L))));
            assertFalse(adapter.statementCache().containsKey("e"));
            assertEquals(1, adapter.serverCapacity().get(uri).queuedQueries());
            assertEquals(1, adapter.serverCapacity().get(uri).pendingQueries());

            service.releaseExecutions(List.of("a", "c", "d"));
            assertEquals(0, adapter.serverCapacity().get(uri).activeSlots());
            assertEquals(0, adapter.serverCapacity().get(uri).queuedQueries());
            assertEquals(0, adapter.serverCapacity().get(uri).pendingQueries());

            String fullUri = "grpc://127.0.0.1:32021";
            adapter.serverCapacity().put(fullUri,
                    new ServerCapacity(1, 2, 0, 1, 2, 2L, 0L, 1L));
            adapter.serverRegistry().put(fullUri, 0L);
            assertThrows(CapacityExhaustedException.class,
                    () -> service.reserveExecutions(List.of(
                            request(uri, "batch-first", 10L),
                            request(fullUri, "batch-full", 10L))));
            assertFalse(adapter.statementCache().containsKey("batch-first"));
            assertNull(adapter.reservations().get("batch-first"));
            assertEquals(0, adapter.serverCapacity().get(uri).activeSlots());
        }
    }

    /**
     * Creates a reservation request for queue tests.
     *
     * @param uri target server URI
     * @param handle reservation handle
     * @param bytes logical input bytes
     * @return reservation request
     */
    private static ExecutionReservationRequest request(
            String uri, String handle, long bytes) {
        return new ExecutionReservationRequest(uri, handle,
                new HandleState("select * from s.t",
                        new String[]{"s/t/f.parquet"}, uri, bytes, null));
    }

    /**
     * Creates queue-test configuration.
     *
     * @param hazelcastPort member port
     * @param maxQueuedQueries queue bound
     * @return application configuration
     */
    private static AppConfig config(int hazelcastPort, int maxQueuedQueries) {
        return new AppConfig(
                2, 4096, 4, 131072, 1, 1, 1,
                2_147_483_648L, 1, 75, maxQueuedQueries,
                null, false, null, null, null, null,
                false, 1048576, 67108864, 60000L,
                "/data/parquet", null, 32010, hazelcastPort, 5,
                3, 1000, 0);
    }

    /**
     * Allocates a currently free TCP port.
     *
     * @return free port
     * @throws IOException if a socket cannot be opened
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
