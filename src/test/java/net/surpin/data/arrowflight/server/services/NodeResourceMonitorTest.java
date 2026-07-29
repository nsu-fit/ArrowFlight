package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.model.NodeLoadSnapshot;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests publication of local scheduler resource snapshots.
 */
@Tag("unit")
class NodeResourceMonitorTest {

    /**
     * Verifies admission and Arrow memory state are included in a published snapshot.
     */
    @Test
    void publishesCurrentNodeState() {
        ClusterService clusterService = mock(ClusterService.class);
        BufferAllocator allocator = mock(BufferAllocator.class);
        when(allocator.getAllocatedMemory()).thenReturn(200L);
        when(allocator.getLimit()).thenReturn(1_000L);
        SchedulerConfig config = new SchedulerConfig(
                false, 1_000L, 5_000L, 5_000L,
                1, 3, 8, 30_000L,
                0.65, 0.90, 0.70, 0.85, 250L);
        AdaptiveAdmissionController admissionController =
                new AdaptiveAdmissionController(config);
        NodeResourceMonitor monitor = new NodeResourceMonitor(
                clusterService, admissionController, allocator, config);

        monitor.publish();

        ArgumentCaptor<NodeLoadSnapshot> snapshot =
                ArgumentCaptor.forClass(NodeLoadSnapshot.class);
        verify(clusterService).publishNodeSnapshot(snapshot.capture());
        assertEquals(3, snapshot.getValue().concurrencyLimit());
        assertEquals(0, snapshot.getValue().activeQueries());
        assertTrue(snapshot.getValue().memoryPressure() >= 0.20);
        monitor.close();
        verify(clusterService).removeNodeSnapshot();
    }
}
