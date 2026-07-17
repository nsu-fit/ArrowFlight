package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.query.Endpoint;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlightInputPartitionTest {
    @Test
    void flightEndpointInputPartition_returnsEndpoint() {
        URI[] uris = new URI[]{URI.create("grpc://localhost:32010")};
        Endpoint ep = new Endpoint(uris, new byte[]{1, 2, 3});
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true))));

        var partition = new FlightInputPartition.FlightEndpointInputPartition(schema, ep);
        assertEquals(ep, partition.getEndpoint());
    }

    @Test
    void flightEndpointInputPartition_returnsSchema() throws IOException {
        URI[] uris = new URI[]{URI.create("grpc://localhost:32010")};
        Endpoint ep = new Endpoint(uris, new byte[]{1, 2, 3});
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true))));

        var partition = new FlightInputPartition.FlightEndpointInputPartition(schema, ep);
        Schema retrieved = partition.getSchema();
        assertEquals(1, retrieved.getFields().size());
        assertEquals("id", retrieved.getFields().get(0).getName());
    }

    @Test
    void flightQueryInputPartition_returnsQuery() {
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true))));
        var partition = new FlightInputPartition.FlightQueryInputPartition(schema, "SELECT * FROM t");
        assertEquals("SELECT * FROM t", partition.getQuery());
    }
}
