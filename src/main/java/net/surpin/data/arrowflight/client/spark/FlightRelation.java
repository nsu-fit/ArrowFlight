package net.surpin.data.arrowflight.client.spark;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import net.surpin.data.arrowflight.client.spark.read.FlightBatch;
import net.surpin.data.arrowflight.client.spark.read.FlightPartitionReader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Spark V1 relation used by Spark Thrift Server for Hive metastore data-source tables.
 */
public final class FlightRelation extends BaseRelation implements TableScan {
    private final SQLContext sqlContext;
    private final Configuration configuration;
    private final Table table;
    private final StructType schema;

    public FlightRelation(SQLContext sqlContext, Configuration configuration, Table table) {
        this.sqlContext = sqlContext;
        this.configuration = configuration;
        this.table = table;
        this.schema = table.getSparkSchema();
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public RDD<Row> buildScan() {
        InputPartition[] partitions = new FlightBatch(this.configuration, this.table).planInputPartitions();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(this.sqlContext.sparkContext());
        if (partitions.length == 0) {
            JavaRDD<Row> empty = sparkContext.emptyRDD();
            return empty.rdd();
        }

        final Configuration scanConfiguration = this.configuration;
        final StructType scanSchema = this.schema;
        JavaRDD<InputPartition> partitionRdd = sparkContext.parallelize(Arrays.asList(partitions), partitions.length);
        JavaRDD<Row> rows = partitionRdd.mapPartitions(partitionIterator ->
            new FlightRowIterator(scanConfiguration, scanSchema, partitionIterator)
        );
        return rows.rdd();
    }

    private static final class FlightRowIterator implements Iterator<Row> {
        private final Configuration configuration;
        private final StructType schema;
        private final Iterator<InputPartition> partitions;

        private FlightPartitionReader reader;
        private Row nextRow;
        private boolean loaded;

        private FlightRowIterator(Configuration configuration, StructType schema, Iterator<InputPartition> partitions) {
            this.configuration = configuration;
            this.schema = schema;
            this.partitions = partitions;
        }

        @Override
        public boolean hasNext() {
            if (!this.loaded) {
                this.advance();
            }
            return this.nextRow != null;
        }

        @Override
        public Row next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Row current = this.nextRow;
            this.nextRow = null;
            this.loaded = false;
            return current;
        }

        private void advance() {
            this.loaded = true;
            this.nextRow = null;

            while (true) {
                try {
                    if (this.reader != null && this.reader.next()) {
                        this.nextRow = toExternalRow(this.reader.get(), this.schema);
                        return;
                    }
                    this.closeReader();
                    if (!this.partitions.hasNext()) {
                        return;
                    }
                    this.reader = new FlightPartitionReader(this.configuration, this.partitions.next());
                } catch (IOException e) {
                    this.closeReader();
                    throw new RuntimeException("Failed to read Flight partition", e);
                }
            }
        }

        private void closeReader() {
            if (this.reader != null) {
                try {
                    this.reader.close();
                } finally {
                    this.reader = null;
                }
            }
        }
    }

    private static Row toExternalRow(InternalRow internalRow, StructType schema) {
        StructField[] fields = schema.fields();
        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            values[i] = toExternalValue(rawValue(internalRow, i, fields[i].dataType()), fields[i].dataType());
        }
        return RowFactory.create(values);
    }

    private static Object rawValue(InternalRow row, int ordinal, DataType dataType) {
        if (row.isNullAt(ordinal)) {
            return null;
        }
        if (row instanceof GenericInternalRow genericRow) {
            return genericRow.genericGet(ordinal);
        }
        return row.get(ordinal, dataType);
    }

    private static Object toExternalValue(Object raw, DataType dataType) {
        if (raw == null) {
            return null;
        }
        if (dataType instanceof DecimalType) {
            return toBigDecimal(raw);
        }
        if (DataTypes.StringType.equals(dataType) && raw instanceof UTF8String utf8) {
            return utf8.toString();
        }
        if (DataTypes.DateType.equals(dataType)) {
            return toDate(raw);
        }
        if (DataTypes.TimestampType.equals(dataType) || DataTypes.TimestampNTZType.equals(dataType)) {
            return toTimestamp(raw);
        }
        if (raw instanceof Decimal decimal) {
            return decimal.toJavaBigDecimal();
        }
        if (raw instanceof UTF8String utf8) {
            return utf8.toString();
        }
        return raw;
    }

    private static BigDecimal toBigDecimal(Object raw) {
        if (raw instanceof BigDecimal decimal) {
            return decimal;
        }
        if (raw instanceof Decimal decimal) {
            return decimal.toJavaBigDecimal();
        }
        if (raw instanceof scala.math.BigDecimal decimal) {
            return decimal.bigDecimal();
        }
        return new BigDecimal(raw.toString());
    }

    private static Date toDate(Object raw) {
        if (raw instanceof Date date) {
            return date;
        }
        if (raw instanceof LocalDate date) {
            return Date.valueOf(date);
        }
        if (raw instanceof Integer days) {
            return Date.valueOf(LocalDate.ofEpochDay(days));
        }
        if (raw instanceof Long millis) {
            return new Date(millis);
        }
        return Date.valueOf(raw.toString());
    }

    private static Timestamp toTimestamp(Object raw) {
        if (raw instanceof Timestamp timestamp) {
            return timestamp;
        }
        if (raw instanceof Number number) {
            long value = number.longValue();
            long abs = Math.abs(value);
            long millis;
            if (abs > 100_000_000_000_000L) {
                millis = value / 1_000L;
            } else if (abs > 100_000_000_000L) {
                millis = value;
            } else {
                millis = value * 1_000L;
            }
            return new Timestamp(millis);
        }
        return Timestamp.valueOf(raw.toString());
    }
}
