package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies bounded translation of Spark runtime filters.
 */
@Tag("unit")
class RuntimeFilterTranslatorTest {

    /**
     * Verifies supported scalar base columns are advertised.
     */
    @Test
    void advertisesSupportedBaseColumns() {
        RuntimeFilterTranslator translator = translator();

        String[] attributes = Arrays.stream(translator.filterAttributes())
                .map(NamedReference::fieldNames)
                .map(names -> names[0])
                .toArray(String[]::new);

        assertArrayContains(attributes, "id");
        assertArrayContains(attributes, "name");
        assertArrayContains(attributes, "active");
    }

    /**
     * Verifies literal IN values are translated for a known base column.
     */
    @Test
    void translatesLiteralInPredicate() {
        Predicate predicate = in("id", 1, 2, 3);

        assertEquals("\"id\" in (1,2,3)",
                translator().translate(new Predicate[]{predicate}));
    }

    /**
     * Verifies an empty dynamic key set prunes every source row.
     */
    @Test
    void translatesEmptyInPredicate() {
        Predicate predicate = new Predicate("IN", new Expression[]{
                FieldReference.column("id")
        });

        assertEquals("(1 = 0)",
                translator().translate(new Predicate[]{predicate}));
    }

    /**
     * Verifies literal equality is accepted in either operand order.
     */
    @Test
    void translatesLiteralEquality() {
        Predicate normal = new Predicate("=", new Expression[]{
                FieldReference.column("id"),
                new LiteralValue<>(7, DataTypes.IntegerType)
        });
        Predicate reversed = new Predicate("=", new Expression[]{
                new LiteralValue<>(8, DataTypes.IntegerType),
                FieldReference.column("id")
        });

        assertEquals("\"id\" = 7 and 8 = \"id\"",
                translator().translate(new Predicate[]{normal, reversed}));
    }

    /**
     * Verifies nested, unknown, and type-changing predicates are ignored.
     */
    @Test
    void ignoresUnsafeRuntimePredicates() {
        Predicate nested = new Predicate("IN", new Expression[]{
                new FieldReference(JavaConverters.asScalaBuffer(
                        Arrays.asList("nested", "id")).toSeq()),
                new LiteralValue<>(1, DataTypes.IntegerType)
        });
        Predicate unknown = in("missing", 1);
        Predicate wrongType = new Predicate("IN", new Expression[]{
                FieldReference.column("id"),
                new LiteralValue<>("1", DataTypes.StringType)
        });
        Predicate columnComparison = new Predicate("=", new Expression[]{
                FieldReference.column("id"),
                FieldReference.column("active")
        });

        assertEquals("", translator().translate(
                new Predicate[]{nested, unknown, wrongType, columnComparison}));
    }

    /**
     * Verifies a runtime key set larger than the value cap is ignored.
     */
    @Test
    void ignoresInPredicateAboveValueLimit() {
        Expression[] children = new Expression[RuntimeFilterTranslator.MAX_VALUES + 2];
        children[0] = FieldReference.column("id");
        for (int index = 1; index < children.length; index++) {
            children[index] = new LiteralValue<>(index, DataTypes.IntegerType);
        }

        assertEquals("", translator().translate(
                new Predicate[]{new Predicate("IN", children)}));
    }

    /**
     * Verifies a runtime predicate larger than the serialized byte cap is ignored.
     */
    @Test
    void ignoresInPredicateAboveByteLimit() {
        char[] characters = new char[RuntimeFilterTranslator.MAX_BYTES];
        Arrays.fill(characters, 'x');
        Predicate predicate = new Predicate("IN", new Expression[]{
                FieldReference.column("name"),
                new LiteralValue<>(new String(characters), DataTypes.StringType)
        });

        assertEquals("", translator().translate(new Predicate[]{predicate}));
    }

    /**
     * Verifies accepted predicates never exceed the aggregate value cap.
     */
    @Test
    void enforcesAggregateValueLimit() {
        Predicate first = repeatedIn("id", 6_000, 1);
        Predicate second = repeatedIn("id", 6_000, 2);

        String where = translator().translate(new Predicate[]{first, second});

        assertFalse(where.isEmpty());
        assertFalse(where.contains("2,2"));
    }

    /**
     * Creates a translator over representative scalar fields.
     *
     * @return runtime filter translator
     */
    private static RuntimeFilterTranslator translator() {
        Table table = Table.forTable("test_table", "\"");
        return new RuntimeFilterTranslator(table, fields(), fields());
    }

    /**
     * Creates representative base fields.
     *
     * @return base fields
     */
    private static StructField[] fields() {
        return new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
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

    /**
     * Creates a large integer IN predicate with one repeated value.
     *
     * @param column base column
     * @param count literal count
     * @param value repeated value
     * @return IN predicate
     */
    private static Predicate repeatedIn(String column, int count, int value) {
        int[] values = new int[count];
        Arrays.fill(values, value);
        return in(column, values);
    }

    /**
     * Asserts an array contains a value.
     *
     * @param values source values
     * @param expected expected value
     */
    private static void assertArrayContains(String[] values, String expected) {
        assertTrue(Arrays.asList(values).contains(expected));
    }
}
