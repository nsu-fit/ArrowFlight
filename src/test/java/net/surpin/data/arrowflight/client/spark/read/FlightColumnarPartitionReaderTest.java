package net.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FlightColumnarPartitionReaderTest {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
    private static final Schema SCHEMA = new Schema(List.of(
            org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true))));

    @Test
    void next_returnsTrueForFirstBatch() throws IOException {
        IntVector vec = intVector(1);
        VectorSchemaRoot root = new VectorSchemaRoot(SCHEMA.getFields(), List.of(vec), 1);

        FlightPartitionReader mockReader = mock(FlightPartitionReader.class);
        when(mockReader.nextBatch()).thenReturn(true);
        when(mockReader.currentBatch()).thenReturn(root);

        FlightColumnarPartitionReader reader = new FlightColumnarPartitionReader(mockReader, SCHEMA);
        assertTrue(reader.next());
        reader.close();
        root.close();
    }

    @Test
    void get_returnsColumnarBatchWithCorrectRowCount() throws IOException {
        IntVector vec = intVector(42, 99);
        VectorSchemaRoot root = new VectorSchemaRoot(SCHEMA.getFields(), List.of(vec), 2);

        FlightPartitionReader mockReader = mock(FlightPartitionReader.class);
        when(mockReader.nextBatch()).thenReturn(true);
        when(mockReader.currentBatch()).thenReturn(root);

        FlightColumnarPartitionReader reader = new FlightColumnarPartitionReader(mockReader, SCHEMA);
        assertTrue(reader.next());
        ColumnarBatch batch = reader.get();
        assertEquals(2, batch.numRows());
        assertEquals(1, batch.numCols());
        reader.close();
        root.close();
    }

    @Test
    void schemaMismatchThrowsIOException() throws IOException {
        IntVector vec = intVector(1);
        Schema mismatchedSchema = new Schema(List.of(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(64, true))));
        VectorSchemaRoot root = new VectorSchemaRoot(mismatchedSchema.getFields(), List.of(vec), 1);

        FlightPartitionReader mockReader = mock(FlightPartitionReader.class);
        when(mockReader.nextBatch()).thenReturn(true);
        when(mockReader.currentBatch()).thenReturn(root);

        FlightColumnarPartitionReader reader = new FlightColumnarPartitionReader(mockReader, SCHEMA);
        assertThrows(IOException.class, reader::next);
        reader.close();
        root.close();
    }

    private static IntVector intVector(int... values) {
        IntVector vec = new IntVector("int_col", ALLOCATOR);
        vec.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vec.set(i, values[i]);
        }
        vec.setValueCount(values.length);
        return vec;
    }
}
