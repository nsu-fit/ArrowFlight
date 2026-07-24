package net.surpin.data.arrowflight.server.model;

/**
 * Complete endpoint state reserved atomically during query planning.
 */
public record ExecutionReservationRequest(
        String serverUri,
        String handle,
        HandleState handleState) {
}
