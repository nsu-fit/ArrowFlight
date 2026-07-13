package net.surpin.data.arrowflight.client.query;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class EndpointTest {

    @Test
    void constructorAndGetters() {
        URI[] uris = { URI.create("grpc://localhost:32010") };
        byte[] ticket = { 1, 2, 3 };

        Endpoint ep = new Endpoint(uris, ticket);

        assertArrayEquals(uris, ep.getURIs());
        assertArrayEquals(ticket, ep.getTicket());
    }

    @Test
    void toStringContainsUri() {
        URI[] uris = { URI.create("grpc://host:32010") };
        Endpoint ep = new Endpoint(uris, new byte[0]);

        String str = ep.toString();
        assertTrue(str.contains("grpc://host:32010"));
        assertTrue(str.contains("Endpoint"));
    }

    @Test
    void nullUris() {
        Endpoint ep = new Endpoint(null, new byte[] { 1 });

        assertNull(ep.getURIs());
        byte[] ticket = ep.getTicket();
        assertEquals(1, ticket.length);
    }

    @Test
    void multipleUris() {
        URI[] uris = {
                URI.create("grpc://host1:32010"),
                URI.create("grpc://host2:32010"),
        };
        Endpoint ep = new Endpoint(uris, new byte[0]);

        assertEquals(2, ep.getURIs().length);
    }
}
