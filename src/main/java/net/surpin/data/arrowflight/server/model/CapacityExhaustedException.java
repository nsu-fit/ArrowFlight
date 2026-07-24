package net.surpin.data.arrowflight.server.model;

/**
 * Indicates that all execution slots and bounded queue positions are occupied.
 */
public final class CapacityExhaustedException extends RuntimeException {

    /**
     * Creates an execution-capacity failure.
     *
     * @param message failure description
     */
    public CapacityExhaustedException(String message) {
        super(message);
    }
}
