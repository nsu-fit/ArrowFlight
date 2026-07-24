package net.surpin.data.arrowflight.server.model;

import java.io.Serializable;

/**
 * Distributed execution reservation status.
 */
public enum ReservationStatus implements Serializable {
    PENDING,
    ACTIVE,
    QUEUED
}
