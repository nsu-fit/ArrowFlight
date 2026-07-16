package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Field;
import net.surpin.data.arrowflight.client.query.Endpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FlightPartitionReaderTest {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    @Test
    void get_returnsCorrectValues() throws Exception {
        IntVector idVec = intVector(42, 99);
        VarCharVector nameVec = stringVector("Alice", "Bob");
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)),
                org.apache.arrow.vector.types.pojo.Field.nullable("name", ArrowType.Utf8.INSTANCE));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(idVec, nameVec), 2);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        assertTrue(reader.next());
        InternalRow row = reader.get();
        assertEquals(42, row.getInt(0));
        assertEquals("Alice", row.getString(1));

        assertTrue(reader.next());
        row = reader.get();
        assertEquals(99, row.getInt(0));
        assertEquals("Bob", row.getString(1));

        assertFalse(reader.next());
        reader.close();
        root.close();
    }

    @Test
    void get_coercesIntWidth_whenAceroReturnsNarrowerType() throws Exception {
        TinyIntVector vec = new TinyIntVector("id", ALLOCATOR);
        vec.allocateNew(1);
        vec.set(0, (byte) 5);
        vec.setValueCount(1);

        Schema flightSchema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        Schema streamSchema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(8, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(streamSchema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, flightSchema);

        assertTrue(reader.next());
        InternalRow row = reader.get();
        assertEquals(5, row.getInt(0));
        reader.close();
    }

    @Test
    void next_returnsFalseWhenBatchExhausted() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        assertTrue(reader.next());
        assertFalse(reader.next());
        reader.close();
    }

    @Test
    void next_throwsWhenMidStreamError() throws Exception {
        IntVector vec = intVector(1, 2);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 2);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);
        when(mockStream.next()).thenThrow(new RuntimeException("stream broken"));

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        assertTrue(reader.next());
        assertEquals(1, reader.get().getInt(0));

        assertTrue(reader.next());
        assertEquals(2, reader.get().getInt(0));

        assertThrows(java.io.IOException.class, reader::next);
        reader.close();
    }

    @Test
    void get_throwsWithoutNext() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);
        assertThrows(IllegalStateException.class, reader::get);
        reader.close();
    }

    @Test
    void nextBatch_returnsTrueForFirstBatch() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        assertTrue(reader.nextBatch());
        assertNotNull(reader.currentBatch());
        assertEquals(1, reader.currentBatch().getRowCount());
        reader.close();
    }

    @Test
    void nextBatch_columnarModeBlocksRowIteration() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        reader.nextBatch(); // enter columnar mode
        assertThrows(IllegalStateException.class, reader::next);
        reader.close();
    }

    @Test
    void currentBatch_throwsWithoutNextBatch() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);
        assertThrows(IllegalStateException.class, reader::currentBatch);
        reader.close();
    }

    @Test
    void close_clearsState() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);
        assertTrue(reader.next());
        reader.close();

        assertThrows(IllegalStateException.class, reader::get);
        assertThrows(IllegalStateException.class, reader::currentBatch);
    }

    @Test
    void get_handlesNullValues() throws Exception {
        IntVector vec = new IntVector("id", ALLOCATOR);
        vec.allocateNew(3);
        vec.set(0, 1);
        vec.setNull(1);
        vec.set(2, 3);
        vec.setValueCount(3);

        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 3);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);

        assertTrue(reader.next());
        assertEquals(1, reader.get().getInt(0));

        assertTrue(reader.next());
        assertTrue(reader.get().isNullAt(0));

        assertTrue(reader.next());
        assertEquals(3, reader.get().getInt(0));
        reader.close();
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

    private VarCharVector stringVector(String... values) {
        VarCharVector vec = new VarCharVector("str_col", ALLOCATOR);
        vec.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vec.set(i, values[i].getBytes());
        }
        vec.setValueCount(values.length);
        return vec;
    }

    private static Schema schema(org.apache.arrow.vector.types.pojo.Field... fields) {
        return new Schema(List.of(fields));
    }

    @Test
    void next_advancesThroughSecondBatch() throws Exception {
        IntVector batch1 = intVector(1);
        IntVector batch2 = intVector(2);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));

        VectorSchemaRoot root1 = new VectorSchemaRoot(schema.getFields(), List.of(batch1), 1);
        VectorSchemaRoot root2 = new VectorSchemaRoot(schema.getFields(), List.of(batch2), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.next()).thenReturn(true, false);
        when(mockStream.getRoot()).thenReturn(root2);
        when(mockStream.getSchema()).thenReturn(schema);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root1, schema);
        assertTrue(reader.next());
        assertEquals(1, reader.get().getInt(0));
        assertTrue(reader.next());
        assertEquals(2, reader.get().getInt(0));
        assertFalse(reader.next());
        reader.close();
        root1.close();
        root2.close();
    }

    @Test
    void close_canBeCalledMultipleTimes() throws Exception {
        IntVector vec = intVector(1);
        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);
        reader.close();
        reader.close();
        root.close();
    }

    @Test
    void get_handlesMultipleColumnTypes() throws Exception {
        IntVector intVec = intVector(42);
        VarCharVector strVec = stringVector("hello");
        BitVector boolVec = new BitVector("bool_col", ALLOCATOR);
        boolVec.allocateNew(1);
        boolVec.set(0, 1);
        boolVec.setValueCount(1);

        Schema schema = schema(
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new ArrowType.Int(32, true)),
                org.apache.arrow.vector.types.pojo.Field.nullable("name", ArrowType.Utf8.INSTANCE),
                org.apache.arrow.vector.types.pojo.Field.nullable("flag", ArrowType.Bool.INSTANCE));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(intVec, strVec, boolVec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);

        FlightPartitionReader reader = new FlightPartitionReader(mockStream, root, schema);
        assertTrue(reader.next());
        InternalRow row = reader.get();
        assertEquals(42, row.getInt(0));
        assertEquals("hello", row.getString(1));
        assertTrue(row.getBoolean(2));
        reader.close();
        root.close();
    }

    // ── openStream via Client mock ────────────────────────────────────

    @Test
    void openEndpointStream_succeedsOnFirstTry() throws Exception {
        IntVector vec = intVector(42);
        Schema schema = schema(field("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);
        when(mockStream.next()).thenReturn(true);
        when(mockStream.getSchema()).thenReturn(schema);

        net.surpin.data.arrowflight.client.Client mockClient =
                mock(net.surpin.data.arrowflight.client.Client.class);
        when(mockClient.openStream(any(org.apache.arrow.flight.FlightEndpoint.class)))
                .thenReturn(mockStream);

        net.surpin.data.arrowflight.client.Configuration config =
                new net.surpin.data.arrowflight.client.Configuration(
                        "localhost", 32010, null, null, "test-token");
        config.setMaxRetries(0);

        Endpoint ep = new Endpoint(new java.net.URI[]{
                java.net.URI.create("grpc://localhost:32010")}, new byte[]{1, 2, 3});

        FlightPartitionReader reader = new FlightPartitionReader(mockClient, config,
                new FlightInputPartition.FlightEndpointInputPartition(schema, ep));

        assertTrue(reader.next());
        assertEquals(42, reader.get().getInt(0));
        reader.close();
        root.close();
    }

    @Test
    void openEndpointStream_retriesOnFailure() throws Exception {
        IntVector vec = intVector(99);
        Schema schema = schema(field("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(vec), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root);
        when(mockStream.next()).thenReturn(true);
        when(mockStream.getSchema()).thenReturn(schema);

        net.surpin.data.arrowflight.client.Client mockClient =
                mock(net.surpin.data.arrowflight.client.Client.class);
        when(mockClient.openStream(any(org.apache.arrow.flight.FlightEndpoint.class)))
                .thenThrow(new RuntimeException("first attempt fails"))
                .thenReturn(mockStream);

        net.surpin.data.arrowflight.client.Configuration config =
                new net.surpin.data.arrowflight.client.Configuration(
                        "localhost", 32010, null, null, "test-token");
        config.setMaxRetries(1);
        config.setRetryBackoffMs(1);

        Endpoint ep = new Endpoint(new java.net.URI[]{
                java.net.URI.create("grpc://localhost:32010")}, new byte[]{1, 2, 3});

        FlightPartitionReader reader = new FlightPartitionReader(mockClient, config,
                new FlightInputPartition.FlightEndpointInputPartition(schema, ep));

        assertTrue(reader.next());
        assertEquals(99, reader.get().getInt(0));
        reader.close();
        root.close();
    }

    @Test
    void openEndpointStream_throwsAfterMaxRetries() throws Exception {
        net.surpin.data.arrowflight.client.Client mockClient =
                mock(net.surpin.data.arrowflight.client.Client.class);
        when(mockClient.openStream(any(org.apache.arrow.flight.FlightEndpoint.class)))
                .thenThrow(new RuntimeException("always fails"));

        net.surpin.data.arrowflight.client.Configuration config =
                new net.surpin.data.arrowflight.client.Configuration(
                        "localhost", 32010, null, null, "test-token");
        config.setMaxRetries(0);
        config.setRetryBackoffMs(1);

        Schema schema = schema(field("id", new ArrowType.Int(32, true)));
        Endpoint ep = new Endpoint(new java.net.URI[]{
                java.net.URI.create("grpc://localhost:32010")}, new byte[]{1, 2, 3});

        FlightPartitionReader reader = new FlightPartitionReader(mockClient, config,
                new FlightInputPartition.FlightEndpointInputPartition(schema, ep));

        assertThrows(Exception.class, reader::next);
        reader.close();
    }

    @Test
    void reopenStream_isCalled_whenStreamFails() throws Exception {
        IntVector batch1 = intVector(1, 2);
        Schema schema = schema(field("id", new ArrowType.Int(32, true)));
        VectorSchemaRoot root1 = new VectorSchemaRoot(schema.getFields(), List.of(batch1), 2);
        IntVector batch2 = intVector(3);
        VectorSchemaRoot root2 = new VectorSchemaRoot(schema.getFields(), List.of(batch2), 1);

        FlightStream mockStream = mock(FlightStream.class);
        when(mockStream.getRoot()).thenReturn(root1).thenReturn(root2);
        when(mockStream.getSchema()).thenReturn(schema);
        when(mockStream.next())
                .thenReturn(true)    // advance after first batch exhausted
                .thenThrow(new RuntimeException("mid-stream broken"));

        net.surpin.data.arrowflight.client.Client mockClient =
                mock(net.surpin.data.arrowflight.client.Client.class);
        when(mockClient.openStream(any(org.apache.arrow.flight.FlightEndpoint.class)))
                .thenReturn(mockStream)          // first open
                .thenThrow(new RuntimeException("reopen fails")); // reopen attempt

        net.surpin.data.arrowflight.client.Configuration config =
                new net.surpin.data.arrowflight.client.Configuration(
                        "localhost", 32010, null, null, "test-token");
        config.setMaxRetries(0);

        Endpoint ep = new Endpoint(new java.net.URI[]{
                java.net.URI.create("grpc://localhost:32010")}, new byte[]{1, 2, 3});

        FlightPartitionReader reader = new FlightPartitionReader(mockClient, config,
                new FlightInputPartition.FlightEndpointInputPartition(schema, ep));

        assertTrue(reader.next());
        assertEquals(1, reader.get().getInt(0));
        assertTrue(reader.next());
        assertEquals(2, reader.get().getInt(0));

        assertThrows(java.io.IOException.class, reader::next);
        reader.close();
        root1.close();
        root2.close();
    }

    private static org.apache.arrow.vector.types.pojo.Field field(
            String name, ArrowType type) {
        return org.apache.arrow.vector.types.pojo.Field.nullable(name, type);
    }
}
