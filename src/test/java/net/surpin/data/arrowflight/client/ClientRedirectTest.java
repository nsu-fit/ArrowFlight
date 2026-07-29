package net.surpin.data.arrowflight.client;

import net.surpin.data.arrowflight.common.FlightRedirectProtocol;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests client-side following of endpoint redirect trailers.
 */
@Tag("unit")
class ClientRedirectTest {

    /**
     * Verifies only the redirected endpoint is reopened and its first batch is preserved.
     *
     * @throws Exception if the redirect-aware stream cannot be opened
     */
    @Test
    void followsRedirectBeforeExposingFirstBatch() throws Exception {
        FlightEndpoint initial = new FlightEndpoint(
                new Ticket(new byte[] {1}),
                Location.forGrpcInsecure("node-1", 32010));
        byte[] replacementTicket = new byte[] {2};
        FlightRuntimeException redirect = CallStatus.RESOURCE_EXHAUSTED
                .withMetadata(FlightRedirectProtocol.metadata(
                        "grpc+tcp://node-2:32010",
                        replacementTicket, 1))
                .toRuntimeException();
        FlightStream first = mock(FlightStream.class);
        FlightStream second = mock(FlightStream.class);
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(first.next()).thenThrow(redirect);
        when(second.next()).thenReturn(true, false);
        when(second.getRoot()).thenReturn(root);
        AtomicInteger calls = new AtomicInteger();
        List<FlightEndpoint> opened = new ArrayList<>();

        try (Client.RedirectingStream stream = Client.followRedirects(
                initial, endpoint -> {
                    opened.add(endpoint);
                    return calls.getAndIncrement() == 0 ? first : second;
                })) {
            assertTrue(stream.next());
            assertSame(root, stream.getRoot());
            assertFalse(stream.next());
        }

        assertArrayEquals(
                replacementTicket,
                opened.get(1).getTicket().getBytes());
        verify(first).close();
        verify(second).close();
    }
}
