package net.surpin.data.arrowflight.client.model;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.query.Endpoint;
import net.surpin.data.arrowflight.client.query.QueryEndpoints;
import net.surpin.data.arrowflight.client.spark.read.FlightInputPartition;
import net.surpin.data.arrowflight.client.spark.read.FlightScan;
import net.surpin.data.arrowflight.client.spark.read.FlightScanBuilder;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies schema and endpoint planning for runtime-filtered Flight scans.
 */
@Tag("unit")
class FlightScanPlanningTest {

    /**
     * Verifies scalar scans defer both metadata calls until endpoint planning.
     *
     * @throws Exception when planned schema deserialization fails
     */
    @Test
    void columnarModeDefersAndDeduplicatesEndpointPlanning() throws Exception {
        AtomicInteger schemaCalls = new AtomicInteger();
        AtomicInteger endpointCalls = new AtomicInteger();
        Schema arrowSchema = arrowSchema();
        Endpoint endpoint = new Endpoint(
                new URI[]{URI.create("grpc://localhost:32010")}, new byte[]{1});
        Table.MetadataProvider metadataProvider = new Table.MetadataProvider() {
            @Override
            public Schema getQuerySchema(Configuration config, String query) {
                schemaCalls.incrementAndGet();
                return arrowSchema;
            }

            @Override
            public QueryEndpoints getQueryEndpoints(
                    Configuration config, String query) {
                endpointCalls.incrementAndGet();
                assertTrue(query.contains("\"id\" in (17,19)"));
                return new QueryEndpoints(arrowSchema, new Endpoint[]{endpoint});
            }
        };
        Table catalogTable = Table.forTable(
                "test_table", "\"", metadataProvider);
        catalogTable.setArrowSchema(baseArrowSchema());
        FlightScanBuilder builder = new FlightScanBuilder(
                config(), catalogTable, noPartitioning());
        builder.pruneColumns(projectedSparkSchema());

        FlightScan scan = (FlightScan) builder.build();

        assertEquals(0, schemaCalls.get());
        assertEquals(0, endpointCalls.get());
        assertEquals(projectedSparkSchema(), scan.readSchema());
        assertEquals(1, scan.filterAttributes().length);
        assertEquals("id", scan.filterAttributes()[0].fieldNames()[0]);
        assertEquals(0, schemaCalls.get());
        assertEquals(Scan.ColumnarSupportMode.SUPPORTED, scan.columnarSupportMode());
        assertEquals(0, schemaCalls.get());
        assertEquals(0, endpointCalls.get());

        scan.filter(new Predicate[]{in("id", 17, 19)});
        InputPartition[] first = scan.toBatch().planInputPartitions();
        InputPartition[] second = scan.toBatch().planInputPartitions();

        assertEquals(1, first.length);
        assertEquals(1, second.length);
        assertEquals(0, schemaCalls.get());
        assertEquals(1, endpointCalls.get());
        assertEquals(arrowSchema,
                ((FlightInputPartition) first[0]).getSchema());
        assertEquals("select * from test_table  ", catalogTable.getQueryStatement());
    }

    /**
     * Verifies Spark-only metadata safely falls back to one schema lookup.
     */
    @Test
    void sparkOnlySchemaFallsBackToSchemaPlanning() {
        AtomicInteger schemaCalls = new AtomicInteger();
        AtomicInteger endpointCalls = new AtomicInteger();
        Schema projectedSchema = arrowSchema();
        Table.MetadataProvider metadataProvider = new Table.MetadataProvider() {
            @Override
            public Schema getQuerySchema(Configuration config, String query) {
                schemaCalls.incrementAndGet();
                assertTrue(query.startsWith("select \"id\" from test_table"));
                return projectedSchema;
            }

            @Override
            public QueryEndpoints getQueryEndpoints(
                    Configuration config, String query) {
                endpointCalls.incrementAndGet();
                return new QueryEndpoints(projectedSchema, new Endpoint[0]);
            }
        };
        Table catalogTable = Table.forTable(
                "test_table", "\"", metadataProvider);
        catalogTable.setSparkSchema(baseSparkSchema());
        FlightScanBuilder builder = new FlightScanBuilder(
                config(), catalogTable, noPartitioning());
        builder.pruneColumns(projectedSparkSchema());

        FlightScan scan = (FlightScan) builder.build();

        assertEquals(1, schemaCalls.get());
        assertEquals(0, endpointCalls.get());
        assertEquals(projectedSparkSchema(), scan.readSchema());
        assertEquals(Scan.ColumnarSupportMode.SUPPORTED, scan.columnarSupportMode());
        assertEquals(1, schemaCalls.get());
    }

    /**
     * Verifies aggregate result typing still uses the schema metadata endpoint.
     */
    @Test
    void aggregateBuildKeepsSchemaPlanning() {
        AtomicInteger schemaCalls = new AtomicInteger();
        AtomicInteger endpointCalls = new AtomicInteger();
        Schema aggregateSchema = new Schema(List.of(new Field(
                "count(*)",
                FieldType.nullable(new ArrowType.Int(64, true)),
                null)));
        Table.MetadataProvider metadataProvider = new Table.MetadataProvider() {
            @Override
            public Schema getQuerySchema(Configuration config, String query) {
                schemaCalls.incrementAndGet();
                assertTrue(query.startsWith("select count(*) from test_table"));
                return aggregateSchema;
            }

            @Override
            public QueryEndpoints getQueryEndpoints(
                    Configuration config, String query) {
                endpointCalls.incrementAndGet();
                return new QueryEndpoints(aggregateSchema, new Endpoint[0]);
            }
        };
        Table catalogTable = Table.forTable(
                "test_table", "\"", metadataProvider);
        catalogTable.setArrowSchema(baseArrowSchema());
        FlightScanBuilder builder = new FlightScanBuilder(
                config(), catalogTable, noPartitioning());
        Aggregation aggregation = new Aggregation(
                new AggregateFunc[]{new CountStar()}, new Expression[0]);

        assertTrue(builder.pushAggregation(aggregation));
        FlightScan scan = (FlightScan) builder.build();

        assertEquals(1, schemaCalls.get());
        assertEquals(0, endpointCalls.get());
        assertEquals(DataTypes.LongType, scan.readSchema().fields()[0].dataType());
    }

    /**
     * Verifies query partitions retain their schema-only metadata call.
     */
    @Test
    void queryPartitionBuildKeepsSchemaPlanning() {
        AtomicInteger schemaCalls = new AtomicInteger();
        AtomicInteger endpointCalls = new AtomicInteger();
        Schema arrowSchema = baseArrowSchema();
        Table.MetadataProvider metadataProvider = new Table.MetadataProvider() {
            @Override
            public Schema getQuerySchema(Configuration config, String query) {
                schemaCalls.incrementAndGet();
                return arrowSchema;
            }

            @Override
            public QueryEndpoints getQueryEndpoints(
                    Configuration config, String query) {
                endpointCalls.incrementAndGet();
                return new QueryEndpoints(arrowSchema, new Endpoint[0]);
            }
        };
        Table catalogTable = Table.forTable(
                "test_table", "\"", metadataProvider);
        catalogTable.setArrowSchema(baseArrowSchema());
        PartitionBehavior partitioning = new PartitionBehavior(
                null, null, 2, null, null,
                new String[]{"id < 10", "id >= 10"});
        FlightScanBuilder builder = new FlightScanBuilder(
                config(), catalogTable, partitioning);

        FlightScan scan = (FlightScan) builder.build();
        InputPartition[] partitions = scan.toBatch().planInputPartitions();

        assertEquals(1, schemaCalls.get());
        assertEquals(0, endpointCalls.get());
        assertEquals(2, partitions.length);
    }

    /**
     * Creates connector configuration for driver-only tests.
     *
     * @return connector configuration
     */
    private static Configuration config() {
        return new Configuration("localhost", 32010, "user", "pass", null);
    }

    /**
     * Creates disabled client partitioning.
     *
     * @return partition behavior
     */
    private static PartitionBehavior noPartitioning() {
        return new PartitionBehavior(null, null, 1, null, null, null);
    }

    /**
     * Creates the base Spark schema used by the catalog table.
     *
     * @return two-column Spark schema
     */
    private static StructType baseSparkSchema() {
        return new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("payload", DataTypes.StringType, true, Metadata.empty())
        });
    }

    /**
     * Creates the projected Spark schema used by the endpoint scan.
     *
     * @return one-column Spark schema
     */
    private static StructType projectedSparkSchema() {
        return new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty())
        });
    }

    /**
     * Creates the Arrow schema returned by the metadata provider.
     *
     * @return one-column Arrow schema
     */
    private static Schema arrowSchema() {
        Field field = new Field(
                "id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        return new Schema(List.of(field));
    }

    /**
     * Creates the unprojected Arrow schema used by query partitions.
     *
     * @return two-column Arrow schema
     */
    private static Schema baseArrowSchema() {
        return new Schema(List.of(
                new Field("id",
                        FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("payload",
                        FieldType.nullable(new ArrowType.Time(
                                TimeUnit.MICROSECOND, 64)), null)));
    }

    /**
     * Creates an integer IN predicate.
     *
     * @param column base column
     * @param values literal values
     * @return IN predicate
     */
    private static Predicate in(String column, int... values) {
        Expression[] children = new Expression[values.length + 1];
        children[0] = FieldReference.column(column);
        for (int index = 0; index < values.length; index++) {
            children[index + 1] =
                    new LiteralValue<>(values[index], DataTypes.IntegerType);
        }
        return new Predicate("IN", children);
    }
}
