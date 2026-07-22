package net.surpin.data.arrowflight.client.model;

import net.surpin.data.arrowflight.client.Client;
import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.query.Endpoint;
import net.surpin.data.arrowflight.client.query.PushAggregation;
import net.surpin.data.arrowflight.client.query.QueryEndpoints;
import net.surpin.data.arrowflight.client.query.QueryStatement;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes a flight table
 */
public final class Table implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    //the name of a flight table whose data will be queried/updated
    private final String name;
    //the character for quoting columns in sql statements
    private final String columnQuote;

    //the read-statement
    private QueryStatement stmt;

    //the spark schema
    private StructType sparkSchema = null;
    //the flight schema
    private transient Schema schema = null;
    //the end-points exposed by the remote flight-service for fetching data of this table
    private Endpoint[] endpoints = new Endpoint[0];

    //the container for holding the partitioning queries
    private final java.util.List<String> partitionStmts = new java.util.ArrayList<>();

    /**
     * Construct a Table object
     * @param name - the name of the table
     * @param columnQuote - the character for quoting columns in sql statements
     */
    private Table(String name, String columnQuote) {
        this.name = name;
        this.columnQuote = columnQuote;

        this.prepareQueryStatement(null, null, null, null);
    }

    /**
     * Get the name of the table
     * @return - the name of the table
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the sql-statement for querying the table
     * @return - the physical query which will be submitted to remote flight service
     */
    public String getQueryStatement() {
        if (this.stmt == null) {
            throw new RuntimeException("The read statement is not valid.");
        }
        return this.stmt.getStatement();
    }

    /**
     * Get the partition queries
     * @return - the partition queries with each of which is submitted from a spark executor
     */
    public String[] getPartitionStatements() {
        return this.partitionStmts.toArray(new String[0]);
    }

    /**
     * Get the spark schema
     * @return - the spark schema
     */
    public StructType getSparkSchema() {
        return this.sparkSchema;
    }

    /**
     * Seeds schema metadata restored from Spark's persisted catalog entry.
     * No Flight query is submitted; the actual scan still obtains its Arrow
     * schema and endpoints after pushdown has been applied.
     *
     * @param sparkSchema persisted Spark schema
     */
    public void setSparkSchema(StructType sparkSchema) {
        this.sparkSchema = sparkSchema;
    }

    /**
     * Get the end-points
     * @return - end-points exposed by the remote flight service upon submitted query
     */
    public Endpoint[] getEndpoints() {
        return this.endpoints;
    }

    /**
     * Get the flight schema
     * @return - the flight schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the character for quoting columns
     * @return - the character for quoting columns
     */
    public String getColumnQuote() {
        return this.columnQuote;
    }

    /**
     * Creates an independent table state for one Spark scan.
     *
     * <p>Projection and filter pushdown mutate the physical statement, schema and
     * endpoints. A relation may be planned more than once, so sharing that mutable
     * state between scans can reuse endpoints for an older query.</p>
     *
     * @return a table with the same source and quoting rules and a fresh scan state
     */
    public Table newScan() {
        Table scan = new Table(this.name, this.columnQuote);
        scan.sparkSchema = this.sparkSchema;
        return scan;
    }

    /**
     * Initialize the schema and end-points by submitting the physical query
     * @param config - the connection configuration
     */
    public void initialize(Configuration config) {
        LOGGER.debug("Table.initialize(): config: {}", config);
        try {
            Client client = Client.getOrCreate(config);
            QueryEndpoints eps = client.getQueryEndpoints(this.getQueryStatement());
            LOGGER.debug("Table.initialize(): endpoints: {}", eps);

            this.sparkSchema = new StructType(Arrays.stream(Field.from(eps.getSchema())).map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()), true, Metadata.empty())).toArray(StructField[]::new));
            this.schema = eps.getSchema();
            this.endpoints = eps.getEndpoints();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Initializes result schema without allocating endpoints or server handles.
     * Used during Spark catalog/table schema inference; the real scan is planned
     * only after pushdown is known.
     *
     * @param config connection configuration
     */
    public void initializeSchema(Configuration config) {
        LOGGER.debug("Table.initializeSchema(): config: {}", config);
        try {
            Schema resultSchema = Client.getOrCreate(config).getQuerySchema(this.getQueryStatement());
            this.sparkSchema = new StructType(Arrays.stream(Field.from(resultSchema))
                    .map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()),
                            true, Metadata.empty()))
                    .toArray(StructField[]::new));
            this.schema = resultSchema;
            this.endpoints = new Endpoint[0];
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds or updates the query statement and partition queries.
     *
     * @param aggregation        optional aggregation
     * @param fields             projected fields
     * @param filter             filter clause
     * @param partitionBehavior  partitioning config
     * @return true if query changed
     */
    private boolean prepareQueryStatement(PushAggregation aggregation, StructField[] fields, String filter, PartitionBehavior partitionBehavior) {
        //aggregation mode: 0 -> no aggregation; 1 -> aggregation without group-by; 2 -> aggregation with group-by
        int aggMode = 0;
        String select = "";
        String groupBy = "";
        if (aggregation != null) {
            String[] groupByFields = aggregation.getGroupByColumns();
            if (groupByFields != null && groupByFields.length > 0) {
                aggMode = 2;
                groupBy = String.join(",", groupByFields);
            } else {
                aggMode = 1;
            }
            select = String.format("select %s from %s", String.join(",", aggregation.getColumnExpressions()), this.name);
        } else if (fields != null && fields.length > 0) {
            select = String.format("select %s from %s", String.join(",", Arrays.stream(fields).map(column -> String.format("%s%s%s", this.columnQuote, column.name(), this.columnQuote)).toArray(String[]::new)), this.name);
        } else {
            select = String.format("select * from %s", this.name);
        }
        QueryStatement stmt = new QueryStatement(select, filter, groupBy);
        boolean changed = stmt.different(this.stmt);
        if (changed) {
            this.stmt = stmt;
        }

        this.partitionStmts.clear();
        if (aggMode != 1 && partitionBehavior != null && partitionBehavior.enabled()) {
            String where = (filter != null && !filter.isEmpty()) ? String.format("(%s) and ", filter) : "";
            BiFunction<StructField[], StructField[], StructField[]> merge = (s1, s2) -> {
                Hashtable<String, StructField> s = new Hashtable<String, StructField>();
                for (StructField sf : s1) {
                    s.put(sf.name(), sf);
                }
                for (StructField sf : s2) {
                    s.put(sf.name(), sf);
                }
                return s.values().toArray(new StructField[0]);
            };
            String[] predicates = partitionBehavior.predicateDefined() ? partitionBehavior.getPredicates()
                : partitionBehavior.calculatePredicates(this.sparkSchema == null ? fields : merge.apply(fields, this.sparkSchema.fields()));
            for (String predicate : predicates) {
                QueryStatement s = new QueryStatement(select, String.format("%s(%s)", where, predicate), groupBy);
                this.partitionStmts.add(s.getStatement());
            }
        }
        return changed;
    }

    /**
     * Translates a Spark Filter to a SQL WHERE clause.
     *
     * @param filter Spark filter
     * @return SQL WHERE clause string
     */
    public String toWhereClause(Filter filter) {
        return toWhereClauseIfSupported(filter).orElseThrow(() ->
                new IllegalArgumentException("Unsupported Spark filter: " + filter));
    }

    /**
     * Returns whether a Spark filter can be translated without changing its semantics.
     * Composite filters are accepted only when every child is accepted.
     *
     * @param filter Spark filter
     * @return true when the complete filter can be evaluated by the Flight server
     */
    public boolean canPushFilter(Filter filter) {
        return toWhereClauseIfSupported(filter).isPresent();
    }

    /**
     * Translates a Spark V2 predicate to a SQL WHERE clause.
     *
     * @param predicate Spark V2 predicate
     * @return SQL WHERE clause string
     * @throws IllegalArgumentException if the predicate is unsupported
     */
    public String toWhereClause(Predicate predicate) {
        return toWhereClauseIfSupported(predicate).orElseThrow(() ->
                new IllegalArgumentException("Unsupported Spark V2 predicate: " + predicate));
    }

    /**
     * Returns whether a Spark V2 predicate can be translated exactly.
     *
     * @param predicate Spark V2 predicate
     * @return true when the complete predicate can be evaluated by the Flight server
     */
    public boolean canPushPredicate(Predicate predicate) {
        return toWhereClauseIfSupported(predicate).isPresent();
    }

    /**
     * Attempts to translate a complete Spark V2 predicate.
     *
     * @param predicate Spark V2 predicate
     * @return translated SQL clause when every expression is supported
     */
    private Optional<String> toWhereClauseIfSupported(Predicate predicate) {
        if (predicate == null) {
            return Optional.empty();
        }
        String name = predicate.name().toUpperCase(Locale.ROOT);
        Expression[] children = predicate.children();
        if ("ALWAYS_TRUE".equals(name) && children.length == 0) {
            return Optional.of("(1 = 1)");
        }
        if ("ALWAYS_FALSE".equals(name) && children.length == 0) {
            return Optional.of("(1 = 0)");
        }
        if (("AND".equals(name) || "OR".equals(name)) && children.length == 2
                && children[0] instanceof Predicate left
                && children[1] instanceof Predicate right) {
            Optional<String> leftSql = toWhereClauseIfSupported(left);
            Optional<String> rightSql = toWhereClauseIfSupported(right);
            if (leftSql.isPresent() && rightSql.isPresent()) {
                return Optional.of("(" + leftSql.get() + " "
                        + name.toLowerCase(Locale.ROOT) + " " + rightSql.get() + ")");
            }
            return Optional.empty();
        }
        if ("NOT".equals(name) && children.length == 1
                && children[0] instanceof Predicate child) {
            return toWhereClauseIfSupported(child).map(sql -> "not (" + sql + ")");
        }
        if (("IS_NULL".equals(name) || "IS_NOT_NULL".equals(name))
                && children.length == 1) {
            return predicateOperand(children[0]).map(sql -> sql
                    + ("IS_NULL".equals(name) ? " is null" : " is not null"));
        }
        if (isComparisonPredicate(name) && children.length == 2) {
            Optional<String> left = predicateOperand(children[0]);
            Optional<String> right = predicateOperand(children[1]);
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            if ("<=>".equals(name)) {
                return Optional.of("((" + left.get() + " is not null and "
                        + right.get() + " is not null and " + left.get() + " = "
                        + right.get() + ") or (" + left.get() + " is null and "
                        + right.get() + " is null))");
            }
            return Optional.of(left.get() + " " + name + " " + right.get());
        }
        if ("IN".equals(name) && children.length > 1) {
            Optional<String> value = predicateOperand(children[0]);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            String[] literals = new String[children.length - 1];
            for (int i = 1; i < children.length; i++) {
                if (!(children[i] instanceof Literal<?> literal)) {
                    return Optional.empty();
                }
                Optional<String> sql = sqlLiteral(literal, true);
                if (sql.isEmpty()) {
                    return Optional.empty();
                }
                literals[i - 1] = sql.get();
            }
            return Optional.of(value.get() + " in (" + String.join(",", literals) + ")");
        }
        if (("STARTS_WITH".equals(name) || "ENDS_WITH".equals(name)
                || "CONTAINS".equals(name)) && children.length == 2
                && children[0] instanceof NamedReference reference
                && children[1] instanceof Literal<?> literal) {
            Optional<String> attribute = predicateReference(reference);
            Optional<String> value = stringLiteralValue(literal);
            if (attribute.isEmpty() || value.isEmpty()) {
                return Optional.empty();
            }
            String escaped = escapeLike(value.get());
            String pattern = switch (name) {
                case "STARTS_WITH" -> escaped + "%";
                case "ENDS_WITH" -> "%" + escaped;
                default -> "%" + escaped + "%";
            };
            return Optional.of(attribute.get() + " like " + quoteString(pattern)
                    + " escape '\\'");
        }
        return Optional.empty();
    }

    /**
     * Translates a Spark V2 predicate operand.
     *
     * @param expression predicate operand
     * @return SQL operand when supported
     */
    private Optional<String> predicateOperand(Expression expression) {
        if (expression instanceof NamedReference reference) {
            return predicateReference(reference);
        }
        if (expression instanceof Literal<?> literal) {
            return sqlLiteral(literal, true);
        }
        return Optional.empty();
    }

    /**
     * Translates a single-column Spark reference.
     *
     * @param reference Spark column reference
     * @return quoted SQL identifier when supported
     */
    private Optional<String> predicateReference(NamedReference reference) {
        String[] fields = reference.fieldNames();
        if (fields == null || fields.length != 1 || fields[0] == null
                || fields[0].isBlank()) {
            return Optional.empty();
        }
        return Optional.of(quoteIdentifier(fields[0]));
    }

    /**
     * Translates a typed Spark V2 literal.
     *
     * @param literal typed Spark literal
     * @param allowNull whether null is allowed
     * @return SQL literal when supported
     */
    private Optional<String> sqlLiteral(Literal<?> literal, boolean allowNull) {
        Object value = literal.value();
        if (value == null) {
            return allowNull ? Optional.of("null") : Optional.empty();
        }
        if (literal.dataType().equals(DataTypes.DateType) && value instanceof Number days) {
            return Optional.of(quoteString(LocalDate.ofEpochDay(days.longValue()).toString()));
        }
        if ((literal.dataType().equals(DataTypes.TimestampType)
                || literal.dataType().equals(DataTypes.TimestampNTZType))
                && value instanceof Number micros) {
            long epochMicros = micros.longValue();
            long seconds = Math.floorDiv(epochMicros, 1_000_000L);
            int nanos = Math.toIntExact(Math.floorMod(epochMicros, 1_000_000L) * 1_000L);
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
            return Optional.of(quoteString(timestamp.toString().replace('T', ' ')));
        }
        if (literal.dataType().equals(DataTypes.StringType)) {
            return Optional.of(quoteString(value.toString()));
        }
        if (value instanceof org.apache.spark.sql.types.Decimal decimal) {
            return Optional.of(decimal.toPlainString());
        }
        return sqlLiteral(value, allowNull);
    }

    /**
     * Extracts a non-null Spark string literal.
     *
     * @param literal typed Spark literal
     * @return string value when supported
     */
    private static Optional<String> stringLiteralValue(Literal<?> literal) {
        if (literal.value() == null || !literal.dataType().equals(DataTypes.StringType)) {
            return Optional.empty();
        }
        return Optional.of(literal.value().toString());
    }

    /**
     * Returns whether a predicate name is a supported comparison operator.
     *
     * @param name normalized predicate name
     * @return true for supported comparisons
     */
    private static boolean isComparisonPredicate(String name) {
        return "=".equals(name) || "<>".equals(name) || "<=>".equals(name)
                || "<".equals(name) || "<=".equals(name)
                || ">".equals(name) || ">=".equals(name);
    }

    private Optional<String> toWhereClauseIfSupported(Filter filter) {
        if (filter == null) {
            return Optional.empty();
        }
        if (filter instanceof EqualTo equalTo) {
            return comparison(equalTo.attribute(), "=", equalTo.value());
        }
        if (filter instanceof EqualNullSafe equalNullSafe) {
            if (equalNullSafe.value() == null) {
                return Optional.of(quoteIdentifier(equalNullSafe.attribute()) + " is null");
            }
            Optional<String> equality = comparison(
                    equalNullSafe.attribute(), "=", equalNullSafe.value());
            String identifier = quoteIdentifier(equalNullSafe.attribute());
            // Spark's null-safe equality is a two-valued predicate. Preserve that
            // property inside NOT/OR as well as in a top-level WHERE clause.
            return equality.map(sql -> "(" + identifier + " is not null and " + sql + ")");
        }
        if (filter instanceof LessThan lessThan) {
            return comparison(lessThan.attribute(), "<", lessThan.value());
        }
        if (filter instanceof LessThanOrEqual lessThanOrEqual) {
            return comparison(lessThanOrEqual.attribute(), "<=", lessThanOrEqual.value());
        }
        if (filter instanceof GreaterThan greaterThan) {
            return comparison(greaterThan.attribute(), ">", greaterThan.value());
        }
        if (filter instanceof GreaterThanOrEqual greaterThanOrEqual) {
            return comparison(greaterThanOrEqual.attribute(), ">=", greaterThanOrEqual.value());
        }
        if (filter instanceof And and) {
            return combine(and.left(), "and", and.right());
        }
        if (filter instanceof Or or) {
            return combine(or.left(), "or", or.right());
        }
        if (filter instanceof Not not) {
            return toWhereClauseIfSupported(not.child()).map(sql -> "not (" + sql + ")");
        }
        if (filter instanceof IsNull isNull) {
            return Optional.of(quoteIdentifier(isNull.attribute()) + " is null");
        }
        if (filter instanceof IsNotNull isNotNull) {
            return Optional.of(quoteIdentifier(isNotNull.attribute()) + " is not null");
        }
        if (filter instanceof StringStartsWith startsWith) {
            return Optional.of(like(startsWith.attribute(), escapeLike(startsWith.value()) + "%"));
        }
        if (filter instanceof StringContains contains) {
            return Optional.of(like(contains.attribute(), "%" + escapeLike(contains.value()) + "%"));
        }
        if (filter instanceof StringEndsWith endsWith) {
            return Optional.of(like(endsWith.attribute(), "%" + escapeLike(endsWith.value())));
        }
        if (filter instanceof In in) {
            Object[] values = in.values();
            if (values == null || values.length == 0) {
                return Optional.of("(1 = 0)");
            }
            String[] literals = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                Optional<String> literal = sqlLiteral(values[i], true);
                if (literal.isEmpty()) {
                    return Optional.empty();
                }
                literals[i] = literal.get();
            }
            return Optional.of(String.format("%s in (%s)", quoteIdentifier(in.attribute()), String.join(",", literals)));
        }
        return Optional.empty();
    }

    private Optional<String> comparison(String attribute, String operator, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        return sqlLiteral(value, false).map(literal ->
                String.format("%s %s %s", quoteIdentifier(attribute), operator, literal));
    }

    private Optional<String> combine(Filter left, String operator, Filter right) {
        Optional<String> leftSql = toWhereClauseIfSupported(left);
        Optional<String> rightSql = toWhereClauseIfSupported(right);
        if (leftSql.isEmpty() || rightSql.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.format("(%s %s %s)", leftSql.get(), operator, rightSql.get()));
    }

    private Optional<String> sqlLiteral(Object value, boolean allowNull) {
        if (value == null) {
            return allowNull ? Optional.of("null") : Optional.empty();
        }
        if (value instanceof Double number && !Double.isFinite(number)) {
            return Optional.empty();
        }
        if (value instanceof Float number && !Float.isFinite(number)) {
            return Optional.empty();
        }
        if (value instanceof Number) {
            return Optional.of(value.toString());
        }
        if (value instanceof Boolean) {
            return Optional.of(value.toString());
        }
        if (value instanceof CharSequence || value instanceof Character
                || value instanceof Date || value instanceof Time || value instanceof Timestamp
                || value instanceof TemporalAccessor) {
            return Optional.of(quoteString(value.toString()));
        }
        return Optional.empty();
    }

    private String quoteIdentifier(String identifier) {
        if (this.columnQuote == null || this.columnQuote.isEmpty()) {
            return identifier;
        }
        return this.columnQuote + identifier.replace(this.columnQuote, this.columnQuote + this.columnQuote) + this.columnQuote;
    }

    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private String like(String attribute, String pattern) {
        return quoteIdentifier(attribute) + " like " + quoteString(pattern) + " escape '\\'";
    }

    private static String escapeLike(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    /**
     * Probe if the pushed filter, fields and aggregation would affect the existing schema & end-points
     * @param pushedFilter - the pushed filter
     * @param pushedFields - the pushed fields
     * @param pushedAggregation - the pushed aggregation
     * @param partitionBehavior - the partitioning behavior
     * @return - true if initialization is required
     */
    public Boolean probe(String pushedFilter, StructField[] pushedFields, PushAggregation pushedAggregation, PartitionBehavior partitionBehavior) {
        if ((pushedFilter == null || pushedFilter.isEmpty()) && (pushedFields == null || pushedFields.length == 0) && pushedAggregation == null) {
            return false;
        }
        return this.prepareQueryStatement(pushedAggregation, pushedFields, pushedFilter, partitionBehavior);
    }

    /**
     * Table with name
     * @param tableName - the name of a table
     * @param columnQuote - the character for quoting columns in sql statements
     * @return - a Table object
     */
    public static Table forTable(String tableName, String columnQuote) {
        Function<String, Boolean> isQuery = (t) -> t.replaceAll("[\r|\n]", " ").trim().toLowerCase().matches("^select .+ [from]?.+");
        return new Table(isQuery.apply(tableName) ? String.format("(%s) t", tableName) : tableName, columnQuote);
    }
}
