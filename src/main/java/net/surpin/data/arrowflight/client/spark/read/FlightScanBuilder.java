package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import net.surpin.data.arrowflight.client.query.PushAggregation;
import net.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);

    private final Configuration configuration;
    private final Table table;
    private final PartitionBehavior partitionBehavior;

    //the pushed-down filters
    private Filter[] pdFilters = new Filter[0];
    //the pushed-down columns
    private StructField[] pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation pdAggregation = null;

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior) {
        LOGGER.info("{}()", this.getClass().getName());
        this.configuration = configuration;
        this.table = table;
        this.partitionBehavior = partitionBehavior;
    }

    /**
     * Collection aggregations that will be pushed down
     * @param aggregation - the pushed aggregation
     * @return - pushed aggregation
     */
    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        LOGGER.info("{}.pushAggregation()", this.getClass().getName());

        List<String> pdAggregateColumns = new ArrayList<>();
        for (AggregateFunc agg : aggregation.aggregateExpressions()) {
            if (agg instanceof CountStar) {
                pdAggregateColumns.add("count(*)");
            } else if (agg instanceof Count) {
                Count c = (Count) agg;
                Optional<String> column = simpleColumn(c.column());
                if (c.isDistinct() || column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("count(%s)", quote(column.get())));
            } else if (agg instanceof Min) {
                Min m = (Min) agg;
                Optional<String> column = simpleColumn(m.column());
                if (column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("min(%s)", quote(column.get())));
            } else if (agg instanceof Max) {
                Max m = (Max) agg;
                Optional<String> column = simpleColumn(m.column());
                if (column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("max(%s)", quote(column.get())));
            } else if (agg instanceof Sum) {
                Sum s = (Sum) agg;
                Optional<String> column = simpleColumn(s.column());
                if (s.isDistinct() || column.isEmpty() || !safeSumType(column.get())) {
                    this.pdAggregation = null;
                    return false;
                }
                // The server currently emits Float8 for SUM. That exactly matches
                // Spark only for Float/Double inputs; integer/decimal SUM must stay
                // in Spark to preserve overflow and decimal precision semantics.
                pdAggregateColumns.add(String.format("sum(%s)", quote(column.get())));
            } else {
                this.pdAggregation = null;
                return false;
            }
        }

        String[] pdGroupByColumns = new String[aggregation.groupByExpressions().length];
        for (int i = 0; i < aggregation.groupByExpressions().length; i++) {
            Optional<String> column = simpleColumn(aggregation.groupByExpressions()[i]);
            if (column.isEmpty()) {
                this.pdAggregation = null;
                return false;
            }
            pdGroupByColumns[i] = quote(column.get());
        }
        pdAggregateColumns.addAll(0, Arrays.asList(pdGroupByColumns));
        this.pdAggregation = pdGroupByColumns.length > 0
                ? new PushAggregation(pdAggregateColumns.toArray(new String[0]), pdGroupByColumns)
                : new PushAggregation(pdAggregateColumns.toArray(new String[0]));
        return true;
    }

    private Optional<String> simpleColumn(Expression expression) {
        if (!(expression instanceof NamedReference reference)
                || reference.fieldNames().length != 1) {
            return Optional.empty();
        }
        String name = reference.fieldNames()[0];
        return Arrays.stream(this.table.getSparkSchema().fields())
                .map(StructField::name)
                .filter(field -> field.equalsIgnoreCase(name))
                .findFirst();
    }

    private boolean safeSumType(String column) {
        return Arrays.stream(this.table.getSparkSchema().fields())
                .filter(field -> field.name().equalsIgnoreCase(column))
                .map(StructField::dataType)
                .anyMatch(type -> type.equals(DataTypes.FloatType)
                        || type.equals(DataTypes.DoubleType));
    }

    private String quote(String column) {
        String delimiter = this.table.getColumnQuote();
        if (delimiter == null || delimiter.isEmpty()) {
            return column;
        }
        return delimiter + column.replace(delimiter, delimiter + delimiter) + delimiter;
    }

    /**
     * For SupportsPushDownFilters interface
     * @param filters - the pushed-down filters
     * @return - not-accepted filters
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        LOGGER.info("{}.pushFilters()", this.getClass().getName());

        List<Filter> pushed = new ArrayList<>();
        List<Filter> unhandled = new ArrayList<>();
        for (Filter filter : filters) {
            if (this.table.canPushFilter(filter)) {
                pushed.add(filter);
            } else {
                unhandled.add(filter);
            }
        }
        this.pdFilters = pushed.toArray(new Filter[0]);
        return unhandled.toArray(new Filter[0]);
    }

    /**
     * For SupportsPushDownFilters interface
     * @return - the pushed-down filters
     */
    @Override
    public Filter[] pushedFilters() {
        return this.pdFilters;
    }

    /**
     * For SupportsPushDownRequiredColumns interface
     * @param columns - the schema containing the required columns
     */
    @Override
    public void pruneColumns(StructType columns) {
        this.pdColumns = columns.fields();
    }

    /**
     * To build a flight-scan
     * @return - A flight scan
     */
    @Override
    public Scan build() {
        LOGGER.info("{}.build()", this.getClass().getName());

        //adjust flight-table upon pushed filters & columns
        String where = String.join(" and ", Arrays.stream(this.pdFilters).map(this.table::toWhereClause).toArray(String[]::new));
        this.table.probe(where, this.pdColumns, this.pdAggregation, this.partitionBehavior);
        // Plan exactly once, after Spark has supplied every accepted pushdown.
        // Earlier schema-only planning creates unused server handles and makes
        // Flight latency look worse than the actual scan.
        this.table.initialize(this.configuration);
        return new FlightScan(this.configuration, this.table);
    }
}
