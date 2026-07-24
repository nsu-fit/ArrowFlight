package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Execution reservation stored in Hazelcast.
 */
public record ReservationState(
        String reservationId,
        String serverUri,
        String handle,
        ReservationStatus status,
        long fileLoadBytes,
        Long queueSequence,
        long createdAtMillis,
        boolean claimed) implements Serializable {

    /**
     * Returns the reservation promoted to active execution.
     *
     * @return active reservation
     */
    public ReservationState activate() {
        return new ReservationState(reservationId, serverUri, handle,
                ReservationStatus.ACTIVE, fileLoadBytes, null, createdAtMillis, claimed);
    }

    /**
     * Returns the reservation claimed by a DoGet call.
     *
     * @return claimed reservation
     */
    public ReservationState claim() {
        return new ReservationState(reservationId, serverUri, handle,
                status, fileLoadBytes, queueSequence, createdAtMillis, true);
    }

    /**
     * Returns the claimed reservation appended to the execution queue.
     *
     * @param sequence queue sequence
     * @return queued reservation
     */
    public ReservationState queue(long sequence) {
        return new ReservationState(reservationId, serverUri, handle,
                ReservationStatus.QUEUED, fileLoadBytes, sequence,
                createdAtMillis, true);
    }
}
