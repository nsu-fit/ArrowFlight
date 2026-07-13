package net.surpin.data.arrowflight.client.query;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class QueryEndpointsTest {

    @Test
    void constructorAndGetters() {
        Endpoint[] eps = { new Endpoint(new URI[] { URI.create("grpc://host:10") }, new byte[0]) };
        QueryEndpoints qe = new QueryEndpoints(null, eps);

        assertNull(qe.getSchema());
        assertEquals(1, qe.getEndpoints().length);
    }

    @Test
    void emptyEndpoints() {
        QueryEndpoints qe = new QueryEndpoints(null, new Endpoint[0]);

        assertEquals(0, qe.getEndpoints().length);
    }

    @Test
    void toStringIncludesEndpoints() {
        Endpoint[] eps = {
                new Endpoint(new URI[] { URI.create("grpc://a:1") }, new byte[] { 1 }),
                new Endpoint(new URI[] { URI.create("grpc://b:2") }, new byte[] { 2 }),
        };
        QueryEndpoints qe = new QueryEndpoints(null, eps);

        String str = qe.toString();
        assertTrue(str.contains("QueryEndpoints"));
        assertTrue(str.contains("grpc://a:1"));
    }

    @Test
    void schemaPassedThrough() {
        Schema schema = new Schema(java.util.List.of());
        QueryEndpoints qe = new QueryEndpoints(schema, null);

        assertSame(schema, qe.getSchema());
    }
}
