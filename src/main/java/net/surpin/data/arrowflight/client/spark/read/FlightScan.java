package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import net.surpin.data.arrowflight.client.query.PushAggregation;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.OptionalLong;

/**
 * Describes the data-structure of FlightScan
 */
public final class FlightScan implements Scan, SupportsRuntimeV2Filtering,
        SupportsReportStatistics, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScan.class);

    private final Configuration configuration;
    private final Table table;
    private final StructField[] projectedFields;
    private final PushAggregation aggregation;
    private final PartitionBehavior partitionBehavior;
    private final String baseWhere;
    private final RuntimeFilterTranslator runtimeFilterTranslator;

    /**
     * Constructs a statistics-only scan for callers that do not use pushdown state.
     *
     * @param configuration configuration of the remote Flight service
     * @param table Flight table
     */
    public FlightScan(Configuration configuration, Table table) {
        this.configuration = configuration;
        this.table = table;
        this.projectedFields = new StructField[0];
        this.aggregation = null;
        this.partitionBehavior = null;
        this.baseWhere = "";
        this.runtimeFilterTranslator = null;
    }

    /**
     * Constructs a Flight scan with immutable base pushdown state.
     *
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param baseFields fields available before projection
     * @param projectedFields pushed projection
     * @param aggregation pushed aggregation
     * @param partitionBehavior client partitioning behavior
     * @param baseWhere pushed static filter
     */
    public FlightScan(
            Configuration configuration,
            Table table,
            StructField[] baseFields,
            StructField[] projectedFields,
            PushAggregation aggregation,
            PartitionBehavior partitionBehavior,
            String baseWhere) {
        LOGGER.debug("{}()", this.getClass().getName());

        this.configuration = configuration;
        this.table = table;
        this.projectedFields = projectedFields.clone();
        this.aggregation = aggregation;
        this.partitionBehavior = partitionBehavior;
        this.baseWhere = baseWhere;
        this.runtimeFilterTranslator = new RuntimeFilterTranslator(
                table, baseFields, table.getSparkSchema().fields());
    }

    /**
     * Get the schema of the scan
     * @return - the scan for the scan
     */
    @Override
    public StructType readSchema() {
        return this.table.getSparkSchema();
    }

    /**
     * The description of the scan
     * @return - description
     */
    @Override
    public String description() {
        return this.table.getQueryStatement();
    }

    /**
     * Reports columnar support from the schema without materializing input partitions.
     *
     * @return schema-level columnar support mode
     */
    @Override
    public ColumnarSupportMode columnarSupportMode() {
        return FlightPartitionReaderFactory.supportsColumnarSchema(
                this.table.getSchema())
                ? ColumnarSupportMode.SUPPORTED
                : ColumnarSupportMode.UNSUPPORTED;
    }

    /**
     * Returns base-table attributes eligible for Spark runtime filtering.
     *
     * @return supported filter attributes
     */
    @Override
    public NamedReference[] filterAttributes() {
        if (this.runtimeFilterTranslator == null) {
            return new NamedReference[0];
        }
        return this.runtimeFilterTranslator.filterAttributes();
    }

    /**
     * Applies bounded runtime predicates without changing static pushdowns.
     *
     * @param predicates runtime predicates combined with AND
     */
    @Override
    public synchronized void filter(Predicate[] predicates) {
        if (this.runtimeFilterTranslator == null) {
            return;
        }
        String runtimeWhere = this.runtimeFilterTranslator.translate(predicates);
        String where;
        if (this.baseWhere.isEmpty()) {
            where = runtimeWhere;
        } else if (runtimeWhere.isEmpty()) {
            where = this.baseWhere;
        } else {
            where = "(" + this.baseWhere + ") and (" + runtimeWhere + ")";
        }
        this.table.probe(
                where, this.projectedFields, this.aggregation, this.partitionBehavior);
    }

    /**
     * Reports Parquet input estimates transported in the standard FlightInfo fields.
     *
     * @return Spark scan statistics
     */
    @Override
    public Statistics estimateStatistics() {
        long bytes = this.table.getEstimatedBytes();
        long rows = this.table.getEstimatedRows();
        return new Statistics() {
            @Override
            public OptionalLong sizeInBytes() {
                return bytes >= 0 ? OptionalLong.of(bytes) : OptionalLong.empty();
            }

            @Override
            public OptionalLong numRows() {
                return rows >= 0 ? OptionalLong.of(rows) : OptionalLong.empty();
            }
        };
    }

    /**
     * Translate the scan to batch
     * @return - the batch desribes the scan
     */
    @Override
    public Batch toBatch() {
        LOGGER.debug("{}.toBatch()", this.getClass().getName());
        return new FlightBatch(this.configuration, this.table);
    }
}
