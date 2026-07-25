package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Translates bounded Spark runtime filters for a single Flight table.
 */
final class RuntimeFilterTranslator implements Serializable {
    static final int MAX_VALUES = 10_000;
    static final int MAX_BYTES = 262_144;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RuntimeFilterTranslator.class);

    private final Table table;
    private final StructField[] filterFields;

    /**
     * Creates a translator for immutable base-table fields.
     *
     * @param table scan-local table
     * @param baseFields fields available before projection
     * @param outputFields fields visible in the scan result
     */
    RuntimeFilterTranslator(
            Table table, StructField[] baseFields, StructField[] outputFields) {
        this.table = table;
        this.filterFields = Arrays.stream(baseFields)
                .filter(base -> Arrays.stream(outputFields)
                        .anyMatch(output -> output.name().equalsIgnoreCase(base.name())
                                && output.dataType().equals(base.dataType())))
                .toArray(StructField[]::new);
    }

    /**
     * Returns base columns that can safely receive literal runtime filters.
     *
     * @return supported runtime-filter attributes
     */
    NamedReference[] filterAttributes() {
        return Arrays.stream(this.filterFields)
                .filter(field -> isSupportedType(field.dataType()))
                .map(field -> FieldReference.column(field.name()))
                .toArray(NamedReference[]::new);
    }

    /**
     * Translates supported predicates while enforcing aggregate value and byte limits.
     *
     * @param predicates runtime predicates combined by Spark with AND
     * @return SQL clauses combined with AND
     */
    String translate(Predicate[] predicates) {
        if (predicates == null || predicates.length == 0) {
            return "";
        }

        List<String> clauses = new ArrayList<>();
        int acceptedValues = 0;
        int acceptedBytes = 0;
        for (Predicate predicate : predicates) {
            Optional<String> clause = translatePredicate(predicate);
            if (clause.isEmpty()) {
                LOGGER.debug("Ignoring unsupported runtime predicate: {}", predicate);
                continue;
            }

            int valueCount = literalCount(predicate);
            int byteCount = clause.get().getBytes(StandardCharsets.UTF_8).length;
            int separatorBytes = clauses.isEmpty() ? 0 : " and ".length();
            if (acceptedValues + valueCount > MAX_VALUES
                    || acceptedBytes + separatorBytes + byteCount > MAX_BYTES) {
                LOGGER.debug("Ignoring oversized runtime predicate: values={} bytes={}",
                        valueCount, byteCount);
                continue;
            }
            acceptedValues += valueCount;
            acceptedBytes += separatorBytes + byteCount;
            clauses.add(clause.get());
        }
        return String.join(" and ", clauses);
    }

    /**
     * Translates one structurally safe IN or equality predicate.
     *
     * @param predicate runtime predicate
     * @return SQL clause when accepted
     */
    private Optional<String> translatePredicate(Predicate predicate) {
        if (predicate == null || predicate.name() == null) {
            return Optional.empty();
        }
        String name = predicate.name().toUpperCase(Locale.ROOT);
        Expression[] children = predicate.children();
        if ("IN".equals(name)) {
            return translateIn(children, predicate);
        }
        if ("=".equals(name) && children.length == 2) {
            return translateEquality(children, predicate);
        }
        return Optional.empty();
    }

    /**
     * Translates a literal IN predicate on one base column.
     *
     * @param children predicate operands
     * @param predicate complete predicate
     * @return SQL clause when accepted
     */
    private Optional<String> translateIn(Expression[] children, Predicate predicate) {
        if (children.length == 0 || !(children[0] instanceof NamedReference reference)) {
            return Optional.empty();
        }
        Optional<StructField> field = resolveField(reference);
        if (field.isEmpty() || children.length - 1 > MAX_VALUES) {
            return Optional.empty();
        }
        for (int i = 1; i < children.length; i++) {
            if (!(children[i] instanceof Literal<?> literal)
                    || !literalMatches(field.get(), literal, true)) {
                return Optional.empty();
            }
        }
        if (children.length == 1) {
            return Optional.of("(1 = 0)");
        }
        return translateWithTable(predicate);
    }

    /**
     * Translates equality between one base column and one non-null literal.
     *
     * @param children predicate operands
     * @param predicate complete predicate
     * @return SQL clause when accepted
     */
    private Optional<String> translateEquality(
            Expression[] children, Predicate predicate) {
        NamedReference reference;
        Literal<?> literal;
        if (children[0] instanceof NamedReference left
                && children[1] instanceof Literal<?> right) {
            reference = left;
            literal = right;
        } else if (children[1] instanceof NamedReference right
                && children[0] instanceof Literal<?> left) {
            reference = right;
            literal = left;
        } else {
            return Optional.empty();
        }
        Optional<StructField> field = resolveField(reference);
        if (field.isEmpty() || !literalMatches(field.get(), literal, false)) {
            return Optional.empty();
        }
        return translateWithTable(predicate);
    }

    /**
     * Uses the table's typed SQL renderer after runtime-specific validation.
     *
     * @param predicate validated predicate
     * @return rendered SQL clause
     */
    private Optional<String> translateWithTable(Predicate predicate) {
        try {
            return Optional.of(this.table.toWhereClause(predicate));
        } catch (IllegalArgumentException exception) {
            LOGGER.debug("Runtime predicate renderer rejected {}", predicate, exception);
            return Optional.empty();
        }
    }

    /**
     * Resolves a non-nested reference against the base schema.
     *
     * @param reference runtime-filter column
     * @return canonical base field
     */
    private Optional<StructField> resolveField(NamedReference reference) {
        String[] names = reference.fieldNames();
        if (names == null || names.length != 1 || names[0] == null) {
            return Optional.empty();
        }
        return Arrays.stream(this.filterFields)
                .filter(field -> field.name().equalsIgnoreCase(names[0]))
                .findFirst();
    }

    /**
     * Checks literal nullability and Spark type compatibility.
     *
     * @param field referenced base field
     * @param literal runtime literal
     * @param allowNull whether null is safe for this predicate
     * @return true when the literal preserves Spark semantics
     */
    private static boolean literalMatches(
            StructField field, Literal<?> literal, boolean allowNull) {
        return (allowNull || literal.value() != null)
                && field.dataType().equals(literal.dataType())
                && isSupportedType(literal.dataType());
    }

    /**
     * Counts literals in a validated runtime predicate.
     *
     * @param predicate runtime predicate
     * @return literal count
     */
    private static int literalCount(Predicate predicate) {
        return (int) Arrays.stream(predicate.children())
                .filter(Literal.class::isInstance)
                .count();
    }

    /**
     * Checks whether typed literals can be rendered exactly by the Flight SQL layer.
     *
     * @param type Spark data type
     * @return true for supported scalar types
     */
    private static boolean isSupportedType(DataType type) {
        return type instanceof DecimalType
                || type.equals(DataTypes.ByteType)
                || type.equals(DataTypes.ShortType)
                || type.equals(DataTypes.IntegerType)
                || type.equals(DataTypes.LongType)
                || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType)
                || type.equals(DataTypes.BooleanType)
                || type.equals(DataTypes.StringType)
                || type.equals(DataTypes.DateType)
                || type.equals(DataTypes.TimestampType)
                || type.equals(DataTypes.TimestampNTZType);
    }
}
