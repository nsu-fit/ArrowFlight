package net.surpin.data.arrowflight.common;

import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;

import java.net.URI;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Encodes replacement Flight endpoints in error metadata for client-aware redirects.
 */
public final class FlightRedirectProtocol {

    public static final String URI_HEADER = "arrowflight-redirect-uri";
    public static final String TICKET_HEADER =
            "arrowflight-redirect-ticket-bin";
    public static final String COUNT_HEADER = "arrowflight-redirect-count";

    private static final Set<String> ALLOWED_SCHEMES =
            Set.of("grpc+tcp", "grpc+tls", "grpc+unix");

    private FlightRedirectProtocol() {
    }

    /**
     * Creates Flight error metadata containing one replacement endpoint.
     *
     * @param targetUri target Flight server URI
     * @param ticket replacement Flight ticket bytes
     * @param redirectCount redirect hop count in the replacement ticket
     * @return error metadata suitable for a RESOURCE_EXHAUSTED response
     */
    public static ErrorFlightMetadata metadata(
            String targetUri, byte[] ticket, int redirectCount) {
        ErrorFlightMetadata metadata = new ErrorFlightMetadata();
        metadata.insert(URI_HEADER, targetUri);
        metadata.insert(TICKET_HEADER, ticket.clone());
        metadata.insert(COUNT_HEADER, Integer.toString(redirectCount));
        return metadata;
    }

    /**
     * Extracts a validated replacement endpoint from a Flight failure.
     *
     * @param failure Flight stream failure
     * @return replacement endpoint when redirect metadata is present and valid
     */
    public static Optional<FlightEndpoint> endpoint(
            FlightRuntimeException failure) {
        if (failure == null
                || failure.status().code()
                != FlightStatusCode.RESOURCE_EXHAUSTED) {
            return Optional.empty();
        }
        ErrorFlightMetadata metadata = failure.status().metadata();
        if (metadata == null) {
            return Optional.empty();
        }
        String target = metadata.get(URI_HEADER);
        byte[] ticket = metadata.getByte(TICKET_HEADER);
        if (target == null || target.isBlank()
                || ticket == null || ticket.length == 0) {
            return Optional.empty();
        }
        try {
            URI uri = URI.create(target);
            String scheme = uri.getScheme() == null
                    ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            if (!ALLOWED_SCHEMES.contains(scheme)) {
                return Optional.empty();
            }
            return Optional.of(new FlightEndpoint(
                    new Ticket(ticket), new Location(uri)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /**
     * Reads the redirect hop count for diagnostics.
     *
     * @param failure Flight stream failure
     * @return non-negative hop count, or zero when unavailable
     */
    public static int redirectCount(FlightRuntimeException failure) {
        if (failure == null || failure.status().metadata() == null) {
            return 0;
        }
        String value = failure.status().metadata().get(COUNT_HEADER);
        try {
            return value == null ? 0 : Math.max(0, Integer.parseInt(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
