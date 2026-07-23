package net.surpin.data.arrowflight.client.query;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class QueryEndpointsTest {

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

}
