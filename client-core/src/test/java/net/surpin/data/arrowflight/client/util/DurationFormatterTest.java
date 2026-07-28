package net.surpin.data.arrowflight.client.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests client duration formatting.
 */
class DurationFormatterTest {

    /**
     * Formats a millisecond-scale elapsed duration.
     */
    @Test
    void formatsMilliseconds() {
        String formatted = DurationFormatter.elapsedNanos(System.nanoTime() - 2_000_000L);

        assertTrue(formatted.endsWith("ms") || formatted.endsWith("s"));
    }
}
