package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.model.FieldType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;

/**
 * Spark column adapter backed by the Arrow vectors owned by a Flight stream.
 *
 * <p>This deliberately does not use Spark's {@code ArrowColumnVector}. Spark
 * 3.5.1 was compiled against Arrow 12, while this connector and Flight use
 * Arrow 18. Passing an Arrow 18 {@code ValueVector} into Spark's adapter crosses
 * class-loader/version boundaries and fails at runtime even though the source
 * compiles. This adapter keeps every Arrow type on the connector side and
 * exposes only Spark's stable scalar {@link ColumnVector} API.</p>
 */
final class FlightArrowColumnVector extends ColumnVector {
    private final FieldVector vector;

    FlightArrowColumnVector(FieldVector vector) {
        super(toSparkType(vector));
        this.vector = vector;
    }

    /**
     * The FlightStream owns the vector and releases it after the batch is consumed.
     */
    @Override
    public void close() {
        // no-op
    }

    @Override
    public boolean hasNull() {
        return this.vector.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return this.vector.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return this.vector.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return require(BitVector.class).get(rowId) != 0;
    }

    @Override
    public byte getByte(int rowId) {
        return require(TinyIntVector.class).get(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return require(SmallIntVector.class).get(rowId);
    }

    @Override
    public int getInt(int rowId) {
        if (this.vector instanceof IntVector intVector) {
            return intVector.get(rowId);
        }
        if (this.vector instanceof DateDayVector dateVector) {
            return dateVector.get(rowId);
        }
        throw unsupported("getInt");
    }

    @Override
    public long getLong(int rowId) {
        if (this.vector instanceof BigIntVector longVector) {
            return longVector.get(rowId);
        }
        if (this.vector instanceof TimeStampMicroTZVector timestampVector) {
            return timestampVector.get(rowId);
        }
        throw unsupported("getLong");
    }

    @Override
    public float getFloat(int rowId) {
        return require(Float4Vector.class).get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return require(Float8Vector.class).get(rowId);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        BigDecimal value = require(DecimalVector.class).getObject(rowId);
        return Decimal.apply(value, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        if (this.vector instanceof VarCharVector stringVector) {
            return UTF8String.fromBytes(stringVector.get(rowId));
        }
        if (this.vector instanceof LargeVarCharVector stringVector) {
            return UTF8String.fromBytes(stringVector.get(rowId));
        }
        throw unsupported("getUTF8String");
    }

    @Override
    public byte[] getBinary(int rowId) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        if (this.vector instanceof VarBinaryVector binaryVector) {
            return binaryVector.get(rowId);
        }
        if (this.vector instanceof LargeVarBinaryVector binaryVector) {
            return binaryVector.get(rowId);
        }
        throw unsupported("getBinary");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        throw unsupported("getArray");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        throw unsupported("getMap");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        throw unsupported("getChild");
    }

    private static DataType toSparkType(FieldVector vector) {
        if (vector == null || !matchesArrowType(vector)) {
            String actual = vector == null ? "null" : vector.getClass().getName();
            throw new IllegalArgumentException("Unsupported Arrow vector for Spark columnar read: "
                    + actual);
        }
        return FieldType.toSpark(FieldType.fromArrow(
                vector.getField().getType(), vector.getField().getChildren()));
    }

    private static boolean matchesArrowType(FieldVector vector) {
        ArrowType type = vector.getField().getType();
        return switch (type.getTypeID()) {
            case Bool -> vector instanceof BitVector;
            case Utf8 -> vector instanceof VarCharVector;
            case LargeUtf8 -> vector instanceof LargeVarCharVector;
            case Binary -> vector instanceof VarBinaryVector;
            case LargeBinary -> vector instanceof LargeVarBinaryVector;
            case Null -> vector instanceof NullVector;
            case FloatingPoint -> {
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
                yield (precision == FloatingPointPrecision.SINGLE && vector instanceof Float4Vector)
                        || (precision == FloatingPointPrecision.DOUBLE
                        && vector instanceof Float8Vector);
            }
            case Int -> {
                ArrowType.Int integer = (ArrowType.Int) type;
                yield integer.getIsSigned() && switch (integer.getBitWidth()) {
                    case 8 -> vector instanceof TinyIntVector;
                    case 16 -> vector instanceof SmallIntVector;
                    case 32 -> vector instanceof IntVector;
                    case 64 -> vector instanceof BigIntVector;
                    default -> false;
                };
            }
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) type;
                int precision = decimal.getPrecision();
                int scale = decimal.getScale();
                yield decimal.getBitWidth() == 128
                        && precision >= 1 && precision <= 38
                        && scale >= 0 && scale <= precision
                        && vector instanceof DecimalVector;
            }
            case Date -> ((ArrowType.Date) type).getUnit() == DateUnit.DAY
                    && vector instanceof DateDayVector;
            case Timestamp -> {
                ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
                yield timestamp.getUnit() == TimeUnit.MICROSECOND
                        && timestamp.getTimezone() != null
                        && vector instanceof TimeStampMicroTZVector;
            }
            default -> false;
        };
    }

    private <T extends FieldVector> T require(Class<T> expectedType) {
        if (!expectedType.isInstance(this.vector)) {
            throw unsupported(expectedType.getSimpleName());
        }
        return expectedType.cast(this.vector);
    }

    private UnsupportedOperationException unsupported(String operation) {
        return new UnsupportedOperationException(operation + " is not supported for Arrow vector "
                + this.vector.getClass().getName());
    }
}
