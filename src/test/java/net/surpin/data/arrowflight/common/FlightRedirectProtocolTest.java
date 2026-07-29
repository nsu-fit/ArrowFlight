package net.surpin.data.arrowflight.common;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests encoding and validation of Flight endpoint redirect metadata.
 */
@Tag("unit")
class FlightRedirectProtocolTest {

    /**
     * Verifies valid metadata reconstructs the replacement endpoint.
     */
    @Test
    void reconstructsReplacementEndpoint() {
        byte[] ticket = new byte[] {1, 2, 3};
        FlightRuntimeException failure = CallStatus.RESOURCE_EXHAUSTED
                .withMetadata(FlightRedirectProtocol.metadata(
                        "grpc+tcp://node-2:32010", ticket, 1))
                .toRuntimeException();

        Optional<FlightEndpoint> endpoint =
                FlightRedirectProtocol.endpoint(failure);

        assertTrue(endpoint.isPresent());
        assertEquals(
                "grpc+tcp://node-2:32010",
                endpoint.orElseThrow().getLocations()
                        .getFirst().getUri().toString());
        assertArrayEquals(
                ticket, endpoint.orElseThrow().getTicket().getBytes());
        assertEquals(1, FlightRedirectProtocol.redirectCount(failure));
    }

    /**
     * Verifies unsupported redirect schemes are ignored.
     */
    @Test
    void rejectsUnsupportedTargetScheme() {
        ErrorFlightMetadata metadata = FlightRedirectProtocol.metadata(
                "https://example.test", new byte[] {1}, 1);
        FlightRuntimeException failure = CallStatus.RESOURCE_EXHAUSTED
                .withMetadata(metadata)
                .toRuntimeException();

        assertTrue(FlightRedirectProtocol.endpoint(failure).isEmpty());
    }
}
