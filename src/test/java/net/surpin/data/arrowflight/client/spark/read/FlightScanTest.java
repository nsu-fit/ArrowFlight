package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FlightScanTest {
    @Test
    void toBatchCreatesFlightBatch() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        Table table = mock(Table.class);
        when(table.getQueryStatement()).thenReturn("SELECT * FROM t");

        FlightScan scan = new FlightScan(config, table);
        assertNotNull(scan.toBatch());
    }

    @Test
    void readSchemaReturnsSparkSchema() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        Table table = mock(Table.class);
        Schema arrowSchema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true))));
        when(table.getSparkSchema()).thenReturn(
                new org.apache.spark.sql.types.StructType(new org.apache.spark.sql.types.StructField[]{
                        new org.apache.spark.sql.types.StructField("id",
                                org.apache.spark.sql.types.DataTypes.IntegerType, true,
                                org.apache.spark.sql.types.Metadata.empty())
                }));

        FlightScan scan = new FlightScan(config, table);
        assertEquals(1, scan.readSchema().size());
        assertEquals("id", scan.readSchema().fields()[0].name());
    }

    @Test
    void descriptionReturnsQueryStatement() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        Table table = mock(Table.class);
        when(table.getQueryStatement()).thenReturn("SELECT * FROM test_table");

        FlightScan scan = new FlightScan(config, table);
        assertEquals("SELECT * FROM test_table", scan.description());
    }
}
