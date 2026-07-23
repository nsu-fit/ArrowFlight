package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.query.Endpoint;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** Verifies Spark locality hints exposed by Flight input partitions. */
class FlightInputPartitionTest {

    /** Verifies benchmark endpoint aliases map to their colocated Spark worker. */
    @Test
    void endpointPartitionPrefersFlightAndServerNodeHosts() {
        Endpoint endpoint = new Endpoint(
                new URI[]{URI.create("grpc+tcp://flight-server-3:32010")},
                new byte[]{1});
        FlightInputPartition partition =
                new FlightInputPartition.FlightEndpointInputPartition(
                        new Schema(List.of()), endpoint);

        assertArrayEquals(new String[]{"flight-server-3", "server-node-3"},
                partition.preferredLocations());
    }

    /** Verifies generic Flight hosts remain valid locality hints. */
    @Test
    void endpointPartitionPreservesGenericHost() {
        Endpoint endpoint = new Endpoint(
                new URI[]{URI.create("grpc+tcp://flight.example:32010")},
                new byte[]{1});
        FlightInputPartition partition =
                new FlightInputPartition.FlightEndpointInputPartition(
                        new Schema(List.of()), endpoint);

        assertArrayEquals(new String[]{"flight.example"}, partition.preferredLocations());
    }
}
