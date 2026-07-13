package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class FlightPartitionReaderFactoryTest {
    @Test
    void usesStableRowReaderByDefault() {
        Configuration configuration = new Configuration("localhost", 32010, "user", "pass", null);
        FlightPartitionReaderFactory factory = new FlightPartitionReaderFactory(configuration);

        assertFalse(factory.supportColumnarReads(new InputPartition() { }));
    }
}
