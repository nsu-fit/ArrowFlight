package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Tracks whether a load-reserved endpoint is available or already executing.
 *
 * @param state signed endpoint state
 * @param status current endpoint ownership state
 */
public record EndpointLease(
        HandleState state,
        Status status) implements Serializable {

    /**
     * Describes the exclusive lifecycle state of an endpoint ticket.
     */
    public enum Status {
        RESERVED,
        CLAIMED
    }

    /**
     * Creates a newly reserved endpoint lease.
     *
     * @param state endpoint state
     * @return reserved lease
     */
    public static EndpointLease reserved(HandleState state) {
        return new EndpointLease(state, Status.RESERVED);
    }

    /**
     * Creates an exclusively claimed version of this lease.
     *
     * @return claimed lease
     */
    public EndpointLease claimed() {
        return new EndpointLease(state, Status.CLAIMED);
    }

    /**
     * Returns this lease to the retryable reserved state.
     *
     * @return reserved lease
     */
    public EndpointLease reserved() {
        return new EndpointLease(state, Status.RESERVED);
    }
}
