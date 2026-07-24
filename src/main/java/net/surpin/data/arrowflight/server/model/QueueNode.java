package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Doubly linked FIFO node stored separately from server capacity.
 */
public record QueueNode(
        String reservationId,
        Long previousSequence,
        Long nextSequence) implements Serializable {

    /**
     * Returns a node with another previous pointer.
     *
     * @param sequence previous sequence
     * @return updated node
     */
    public QueueNode withPrevious(Long sequence) {
        return new QueueNode(reservationId, sequence, nextSequence);
    }

    /**
     * Returns a node with another next pointer.
     *
     * @param sequence next sequence
     * @return updated node
     */
    public QueueNode withNext(Long sequence) {
        return new QueueNode(reservationId, previousSequence, sequence);
    }
}
