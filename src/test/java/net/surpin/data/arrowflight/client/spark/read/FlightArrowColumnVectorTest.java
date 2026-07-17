package net.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlightArrowColumnVectorTest {

    private static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    @Test
    void getBoolean_fromBitVector() {
        try (BitVector vec = new BitVector("b", allocator)) {
            vec.allocateNew(3);
            vec.set(0, 1);
            vec.set(1, 0);
            vec.set(2, 0);
            vec.setValueCount(3);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertTrue(col.getBoolean(0));
            assertFalse(col.getBoolean(1));
            assertFalse(col.getBoolean(2));
        }
    }

    @Test
    void getByte_fromTinyIntVector() {
        try (TinyIntVector vec = new TinyIntVector("ti", allocator)) {
            vec.allocateNew(2);
            vec.set(0, (byte) 42);
            vec.set(1, (byte) -1);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals((byte) 42, col.getByte(0));
            assertEquals((byte) -1, col.getByte(1));
        }
    }

    @Test
    void getShort_fromSmallIntVector() {
        try (SmallIntVector vec = new SmallIntVector("si", allocator)) {
            vec.allocateNew(2);
            vec.set(0, (short) 100);
            vec.set(1, (short) -200);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals((short) 100, col.getShort(0));
            assertEquals((short) -200, col.getShort(1));
        }
    }

    @Test
    void getShort_fromUInt1Vector() {
        try (UInt1Vector vec = new UInt1Vector("u1", allocator)) {
            vec.allocateNew(2);
            vec.set(0, (byte) 200);
            vec.set(1, (byte) 255);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals((short) 200, col.getShort(0));
            assertEquals((short) 255, col.getShort(1));
        }
    }

    @Test
    void getInt_fromIntVector() {
        try (IntVector vec = new IntVector("i", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 42);
            vec.set(1, -999);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(42, col.getInt(0));
            assertEquals(-999, col.getInt(1));
        }
    }

    @Test
    void getInt_fromDateDayVector() {
        try (DateDayVector vec = new DateDayVector("dd", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 19500); // 2023-05-23
            vec.set(1, 0);     // 1970-01-01
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(19500, col.getInt(0));
            assertEquals(0, col.getInt(1));
        }
    }

    @Test
    void getInt_fromDateMilliVector() {
        try (DateMilliVector vec = new DateMilliVector("dm", allocator)) {
            vec.allocateNew(1);
            long epochMillis = 1_684_800_000_000L; // 2023-05-23 00:00:00 UTC
            vec.set(0, epochMillis);
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(19500, col.getInt(0));
        }
    }

    @Test
    void getInt_fromUInt2Vector() {
        try (UInt2Vector vec = new UInt2Vector("u2", allocator)) {
            vec.allocateNew(2);
            vec.set(0, (char) 42);
            vec.set(1, (char) 65535);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(42, col.getInt(0));
            assertEquals(65535, col.getInt(1));
        }
    }

    @Test
    void getLong_fromBigIntVector() {
        try (BigIntVector vec = new BigIntVector("bi", allocator)) {
            vec.allocateNew(2);
            vec.set(0, Long.MAX_VALUE);
            vec.set(1, Long.MIN_VALUE);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(Long.MAX_VALUE, col.getLong(0));
            assertEquals(Long.MIN_VALUE, col.getLong(1));
        }
    }

    @Test
    void getLong_fromTimeStampMicroTZVector() {
        try (TimeStampMicroTZVector vec = new TimeStampMicroTZVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1_684_800_000_000L);
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1_684_800_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampMicroVector() {
        try (TimeStampMicroVector vec = new TimeStampMicroVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1_684_800_000_000L);
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1_684_800_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampMilliVector_convertsToMicros() {
        try (TimeStampMilliVector vec = new TimeStampMilliVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1_684_800_000L); // 1_684_800_000_000 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1_684_800_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampMilliTZVector_convertsToMicros() {
        try (TimeStampMilliTZVector vec = new TimeStampMilliTZVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1_000L); // 1_000_000 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampSecVector_convertsToMicros() {
        try (TimeStampSecVector vec = new TimeStampSecVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1L); // 1_000_000 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampSecTZVector_convertsToMicros() {
        try (TimeStampSecTZVector vec = new TimeStampSecTZVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 2L); // 2_000_000 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(2_000_000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampNanoVector_convertsToMicros() {
        try (TimeStampNanoVector vec = new TimeStampNanoVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 5_000L); // 5 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(5L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromTimeStampNanoTZVector_convertsToMicros() {
        try (TimeStampNanoTZVector vec = new TimeStampNanoTZVector("ts",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")), allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1_000_500L); // 1000 micros
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(1000L, col.getLong(0));
        }
    }

    @Test
    void getLong_fromUInt4Vector() {
        try (UInt4Vector vec = new UInt4Vector("u4", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 42);
            vec.set(1, -1); // 0xFFFFFFFF = 4294967295 unsigned
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(42L, col.getLong(0));
            assertEquals(0xFFFFFFFFL, col.getLong(1));
        }
    }

    @Test
    void getLong_fromUInt8Vector() {
        try (UInt8Vector vec = new UInt8Vector("u8", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 42L);
            vec.set(1, Long.MAX_VALUE);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(42L, col.getLong(0));
            assertEquals(Long.MAX_VALUE, col.getLong(1));
        }
    }

    @Test
    void getFloat_fromFloat4Vector() {
        try (Float4Vector vec = new Float4Vector("f4", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 3.14f);
            vec.set(1, -0.0f);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(3.14f, col.getFloat(0));
            assertEquals(-0.0f, col.getFloat(1));
        }
    }

    @Test
    void getDouble_fromFloat8Vector() {
        try (Float8Vector vec = new Float8Vector("f8", allocator)) {
            vec.allocateNew(2);
            vec.set(0, 2.718281828);
            vec.set(1, Double.POSITIVE_INFINITY);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals(2.718281828, col.getDouble(0));
            assertEquals(Double.POSITIVE_INFINITY, col.getDouble(1));
        }
    }

    @Test
    void getDecimal_fromDecimalVector() {
        try (DecimalVector vec = new DecimalVector("dec",
                FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, new BigDecimal("123.45"));
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            var dec = col.getDecimal(0, 10, 2);
            assertEquals(new BigDecimal("123.45"), dec.toJavaBigDecimal());
        }
    }

    @Test
    void getUTF8String_fromVarCharVector() {
        try (VarCharVector vec = new VarCharVector("vc", allocator)) {
            vec.allocateNew(2);
            vec.set(0, "hello".getBytes());
            vec.set(1, "world".getBytes());
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals("hello", col.getUTF8String(0).toString());
            assertEquals("world", col.getUTF8String(1).toString());
        }
    }

    @Test
    void getUTF8String_fromLargeVarCharVector() {
        try (LargeVarCharVector vec = new LargeVarCharVector("lvc", allocator)) {
            vec.allocateNew(1);
            vec.set(0, "large".getBytes());
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertEquals("large", col.getUTF8String(0).toString());
        }
    }

    @Test
    void getBinary_fromVarBinaryVector() {
        try (VarBinaryVector vec = new VarBinaryVector("vb", allocator)) {
            vec.allocateNew(2);
            vec.set(0, new byte[] {1, 2, 3});
            vec.set(1, new byte[] {});
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertArrayEquals(new byte[] {1, 2, 3}, col.getBinary(0));
            assertArrayEquals(new byte[] {}, col.getBinary(1));
        }
    }

    @Test
    void getBinary_fromLargeVarBinaryVector() {
        try (LargeVarBinaryVector vec = new LargeVarBinaryVector("lvb", allocator)) {
            vec.allocateNew(1);
            vec.set(0, new byte[] {4, 5, 6});
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertArrayEquals(new byte[] {4, 5, 6}, col.getBinary(0));
        }
    }

    @Test
    void getBinary_fromFixedSizeBinaryVector() {
        try (FixedSizeBinaryVector vec = new FixedSizeBinaryVector("fsb",
                FieldType.nullable(new ArrowType.FixedSizeBinary(3)), allocator)) {
            vec.allocateNew(1);
            vec.set(0, new byte[] {7, 8, 9});
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertArrayEquals(new byte[] {7, 8, 9}, col.getBinary(0));
        }
    }

    @Test
    void isNullAt_returnsTrueForNullValue() {
        try (IntVector vec = new IntVector("i", allocator)) {
            vec.allocateNew(2);
            vec.setNull(0);
            vec.set(1, 42);
            vec.setValueCount(2);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertTrue(col.isNullAt(0));
            assertFalse(col.isNullAt(1));
        }
    }

    @Test
    void hasNull_and_numNulls() {
        try (BitVector vec = new BitVector("b", allocator)) {
            vec.allocateNew(4);
            vec.setNull(0);
            vec.set(1, 1);
            vec.setNull(2);
            vec.set(3, 0);
            vec.setValueCount(4);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            assertTrue(col.hasNull());
            assertEquals(2, col.numNulls());
        }
    }

    @Test
    void close_isNoOp() {
        try (IntVector vec = new IntVector("i", allocator)) {
            vec.allocateNew(1);
            vec.set(0, 1);
            vec.setValueCount(1);

            FlightArrowColumnVector col = new FlightArrowColumnVector(vec);
            col.close(); // should not throw, should not close the vector
            assertEquals(1, col.getInt(0));
        }
    }

    @Test
    void toSparkType_rejectsUnsupportedVector() {
        try (TimeSecVector vec = new TimeSecVector("ts",
                FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), allocator)) {
            vec.allocateNew(1);
            vec.setValueCount(1);
            assertThrows(IllegalArgumentException.class, () -> new FlightArrowColumnVector(vec));
        }
    }
}
