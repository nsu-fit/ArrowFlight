package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Bounded execution capacity and FIFO pointers for one Flight server.
 */
public record ServerCapacity(
        int activeSlots,
        int queuedQueries,
        int pendingQueries,
        int maxActiveSlots,
        int maxQueuedQueries,
        long nextSequence,
        Long headSequence,
        Long tailSequence) implements Serializable {

    /**
     * Creates an empty capacity state.
     *
     * @param maxActiveSlots maximum active executions
     * @param maxQueuedQueries maximum queued executions
     * @return empty capacity state
     */
    public static ServerCapacity empty(int maxActiveSlots, int maxQueuedQueries) {
        return new ServerCapacity(0, 0, 0, maxActiveSlots, maxQueuedQueries,
                0L, null, null);
    }
}
