package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import net.surpin.data.arrowflight.client.query.PushAggregation;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies runtime filtering preserves the scan's static pushdowns.
 */
@Tag("unit")
class FlightScanRuntimeFilteringTest {

    /**
     * Verifies the scan exposes Spark's V2 runtime filtering contract.
     */
    @Test
    void implementsRuntimeV2Filtering() {
        assertTrue(SupportsRuntimeV2Filtering.class.isAssignableFrom(FlightScan.class));
    }

    /**
     * Verifies omitted base columns are not advertised to Spark's pruning resolver.
     */
    @Test
    void advertisesOnlyColumnsPresentInScanOutput() {
        StructField[] baseFields = fields();
        StructField[] projectedFields = new StructField[]{baseFields[0]};
        Table table = scanTable(projectedFields);
        FlightScan scan = new FlightScan(
                config(), table, baseFields, projectedFields, null,
                noPartitioning(), "");

        assertEquals(1, scan.filterAttributes().length);
        assertEquals("id", scan.filterAttributes()[0].fieldNames()[0]);
    }

    /**
     * Verifies a runtime key set preserves projection and static filtering.
     */
    @Test
    void preservesProjectionAndStaticFilter() {
        StructField[] baseFields = fields();
        StructField[] projectedFields = new StructField[]{baseFields[0]};
        Table table = scanTable(baseFields);
        String baseWhere = "\"active\" = true";
        table.probe(baseWhere, projectedFields, null, noPartitioning());
        FlightScan scan = new FlightScan(
                config(), table, baseFields, projectedFields, null,
                noPartitioning(), baseWhere);

        scan.filter(new Predicate[]{in("id", 3, 5)});

        String statement = scan.description();
        assertTrue(statement.startsWith("select \"id\" from test_table"));
        assertTrue(statement.contains("\"active\" = true"));
        assertTrue(statement.contains("\"id\" in (3,5)"));
    }

    /**
     * Verifies runtime filtering preserves pushed aggregation and grouping.
     */
    @Test
    void preservesAggregationAndGrouping() {
        StructField[] baseFields = fields();
        Table table = scanTable(baseFields);
        PushAggregation aggregation = new PushAggregation(
                new String[]{"\"active\"", "count(*)"},
                new String[]{"\"active\""});
        String baseWhere = "\"active\" is not null";
        table.probe(baseWhere, new StructField[0], aggregation, noPartitioning());
        table.setSparkSchema(new StructType(new StructField[]{
                baseFields[1],
                new StructField("count(*)", DataTypes.LongType, true, Metadata.empty())
        }));
        FlightScan scan = new FlightScan(
                config(), table, baseFields, new StructField[0], aggregation,
                noPartitioning(), baseWhere);

        assertEquals(1, scan.filterAttributes().length);
        assertEquals("active", scan.filterAttributes()[0].fieldNames()[0]);
        scan.filter(new Predicate[]{new Predicate("=", new Expression[]{
                FieldReference.column("active"),
                new LiteralValue<>(true, DataTypes.BooleanType)
        })});

        String statement = scan.description();
        assertTrue(statement.startsWith(
                "select \"active\",count(*) from test_table"));
        assertTrue(statement.contains("\"active\" is not null"));
        assertTrue(statement.contains("\"active\" = true"));
        assertTrue(statement.contains("group by \"active\""));
    }

    /**
     * Verifies repeated runtime filtering replaces rather than accumulates key sets.
     */
    @Test
    void replacesPreviousRuntimeFilter() {
        StructField[] baseFields = fields();
        Table table = scanTable(baseFields);
        FlightScan scan = new FlightScan(
                config(), table, baseFields, new StructField[0], null,
                noPartitioning(), "");

        scan.filter(new Predicate[]{in("id", 1)});
        scan.filter(new Predicate[]{in("id", 2)});

        assertTrue(scan.description().contains("\"id\" in (2)"));
        assertFalse(scan.description().contains("\"id\" in (1)"));
    }

    /**
     * Verifies scan-local runtime state does not mutate the catalog table.
     */
    @Test
    void leavesCatalogTableUnchanged() {
        StructField[] baseFields = fields();
        Table catalogTable = scanTable(baseFields);
        Table scanTable = catalogTable.newScan();
        FlightScan scan = new FlightScan(
                config(), scanTable, baseFields, new StructField[0], null,
                noPartitioning(), "");

        scan.filter(new Predicate[]{in("id", 13)});

        assertEquals("select * from test_table  ", catalogTable.getQueryStatement());
        assertTrue(scan.description().contains("\"id\" in (13)"));
    }

    /**
     * Creates a scan-local table with a known Spark schema.
     *
     * @param fields base fields
     * @return scan table
     */
    private static Table scanTable(StructField[] fields) {
        Table table = Table.forTable("test_table", "\"");
        table.setSparkSchema(new StructType(fields));
        return table;
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
     * Creates representative base fields.
     *
     * @return base fields
     */
    private static StructField[] fields() {
        return new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("active", DataTypes.BooleanType, true, Metadata.empty())
        };
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
