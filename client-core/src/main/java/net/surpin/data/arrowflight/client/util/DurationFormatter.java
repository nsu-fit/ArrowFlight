package net.surpin.data.arrowflight.client.util;

/**
 * Formats client-side operation durations for structured logging.
 */
public final class DurationFormatter {

    private DurationFormatter() {
    }

    /**
     * Formats elapsed nanoseconds as a human-readable duration.
     *
     * @param startNanos timestamp returned by {@link System#nanoTime()}
     * @return formatted elapsed duration
     */
    public static String elapsedNanos(long startNanos) {
        long nanos = System.nanoTime() - startNanos;
        if (nanos < 1_000) {
            return nanos + "ns";
        }
        if (nanos < 1_000_000) {
            return String.format("%.1fµs", nanos / 1000.0);
        }
        if (nanos < 1_000_000_000) {
            return String.format("%.2fms", nanos / 1_000_000.0);
        }
        return String.format("%.3fs", nanos / 1_000_000_000.0);
    }
}
