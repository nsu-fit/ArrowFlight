package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The flight partition-reader factory for creating flight partition-readers
 */
public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReaderFactory.class);

    private final Configuration configuration;

    /**
     * Construct a flight partition-reader factory
     * @param configuration - the configuration of remote flight service
     */
    public FlightPartitionReaderFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Create a reader
     * @param inputPartition - the input-partition for the reader
     * @return - a partition-reader
     */
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        LOGGER.info("{}.createReader()", this.getClass().getName());
        return new FlightPartitionReader(this.configuration, inputPartition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition inputPartition) {
        if (!(inputPartition instanceof FlightInputPartition flightPartition)) {
            return false;
        }
        try {
            Schema schema = flightPartition.getSchema();
            return schema.getFields().stream().allMatch(FlightPartitionReaderFactory::isColumnarType);
        } catch (IOException e) {
            LOGGER.warn("Cannot inspect Flight partition schema for columnar read: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition inputPartition) {
        LOGGER.info("{}.createColumnarReader()", this.getClass().getName());
        return new FlightColumnarPartitionReader(this.configuration, inputPartition);
    }

    private static boolean isColumnarType(Field field) {
        ArrowType type = field.getType();
        return switch (type.getTypeID()) {
            case Bool, Utf8, LargeUtf8, Binary, LargeBinary, Null -> true;
            case FloatingPoint -> {
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
                yield precision == FloatingPointPrecision.SINGLE
                        || precision == FloatingPointPrecision.DOUBLE;
            }
            case Int -> ((ArrowType.Int) type).getIsSigned();
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) type;
                yield decimal.getBitWidth() == 128 && decimal.getPrecision() <= 38;
            }
            case Date -> ((ArrowType.Date) type).getUnit() == DateUnit.DAY;
            case Timestamp -> {
                ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
                // FieldType currently maps timestamps to Spark TimestampType, while
                // Arrow timestamps without a zone map to TimestampNTZType.
                yield timestamp.getUnit() == TimeUnit.MICROSECOND
                        && timestamp.getTimezone() != null;
            }
            default -> false;
        };
    }
}
