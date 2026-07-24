package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Stable key for one node in a server FIFO.
 */
public record QueueKey(String serverUri, long sequence) implements Serializable {
}
