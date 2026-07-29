package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.NodeLoadSnapshot;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests load-aware reassignment of claimed endpoint tasks.
 */
@Tag("unit")
class TaskRedirectServiceTest {

    private static final String SOURCE = "grpc+tcp://node-1:32010";
    private static final String TARGET = "grpc+tcp://node-2:32010";

    /**
     * Verifies unrelated system CPU pressure redirects to an idle HDFS node.
     */
    @Test
    void redirectsFromSystemOverloadedNode() {
        ClusterService cluster = mock(ClusterService.class);
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        SchedulerConfig config = config();
        HandleState state = new HandleState(
                "SELECT * FROM s.t",
                new String[] {"s/t/f.parquet"},
                SOURCE, 100L, true, 0);
        when(cluster.serverUri()).thenReturn(SOURCE);
        when(cluster.allServerLoads())
                .thenReturn(Map.of(SOURCE, 100L, TARGET, 0L));
        when(cluster.filterLiveServers(anySet()))
                .thenReturn(Set.of(SOURCE, TARGET));
        when(cluster.nodeSnapshots(anySet())).thenReturn(Map.of(
                SOURCE, snapshot(4, 4, 1, 0.20, 0.99),
                TARGET, snapshot(4, 0, 0, 0.10, 0.10)));
        when(cluster.fileLocations()).thenReturn(Map.of(
                "s/t/f.parquet",
                new FileAssignment(100L, Set.of(SOURCE))));
        when(parquet.dataDirectory()).thenReturn("hdfs://namenode/data");
        HandleState redirected = state.redirectedTo(TARGET);
        when(cluster.redirectEndpoint(any(), any(), any()))
                .thenReturn(Optional.of(
                        new ClusterService.RedirectedEndpoint(
                                new byte[] {7}, redirected)));
        TaskRedirectService service =
                new TaskRedirectService(cluster, parquet, config);

        Optional<TaskRedirectService.Redirect> result =
                service.tryRedirect(new byte[] {1}, state);

        assertTrue(result.isPresent());
        assertEquals(TARGET, result.orElseThrow().targetUri());
        assertEquals(1, result.orElseThrow().redirectCount());
        verify(cluster).redirectEndpoint(any(), any(), any());
    }

    /**
     * Verifies the signed redirect hop limit prevents ping-pong.
     */
    @Test
    void stopsAtRedirectHopLimit() {
        ClusterService cluster = mock(ClusterService.class);
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        HandleState state = new HandleState(
                "SELECT * FROM s.t",
                new String[] {"s/t/f.parquet"},
                SOURCE, 100L, true, 2);
        when(cluster.serverUri()).thenReturn(SOURCE);
        TaskRedirectService service =
                new TaskRedirectService(cluster, parquet, config());

        assertTrue(service.tryRedirect(
                new byte[] {1}, state).isEmpty());
        verify(cluster, never()).allServerLoads();
    }

    /**
     * Creates scheduler configuration with two redirect hops.
     *
     * @return redirect-enabled scheduler configuration
     */
    private static SchedulerConfig config() {
        return new SchedulerConfig(
                true, 1_000L, 5_000L, 5_000L,
                1, 4, 64, 30_000L,
                0.65, 0.90, 0.70, 0.85, 250L,
                true, 500L, 2, 0.30);
    }

    /**
     * Creates a fresh node snapshot.
     *
     * @param limit concurrency limit
     * @param active active query count
     * @param queued queued query count
     * @param processCpu process CPU load
     * @param systemCpu total system CPU load
     * @return fresh accepting snapshot
     */
    private static NodeLoadSnapshot snapshot(
            int limit,
            int active,
            int queued,
            double processCpu,
            double systemCpu) {
        return new NodeLoadSnapshot(
                System.currentTimeMillis(),
                limit, active, queued,
                processCpu, systemCpu, 0.20,
                128L * 1024L * 1024L, true);
    }
}
