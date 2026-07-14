package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Client;
import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Field;
import net.surpin.data.arrowflight.client.model.FieldType;
import net.surpin.data.arrowflight.client.query.Endpoint;
import net.surpin.data.arrowflight.client.query.QueryEndpoints;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming partition reader that reads data from remote Flight service
 * without accumulating all rows in memory.
 *
 * Each batch from FlightStream is consumed row-by-row and converted to Spark InternalRow
 * on the fly. Only one batch is held in memory at a time.
 */
public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReader.class);

    private final Client client;
    private final Configuration configuration;
    private final InputPartition inputPartition;

    // current FlightStream and its VectorSchemaRoot
    private FlightStream stream;
    private VectorSchemaRoot root;
    private Field[] fields;

    // Schema from FlightInfo (what Spark's codegen was compiled against).
    // Kept separately because `fields` is overwritten per-batch with Acero's stream schema,
    // which may differ in integer width (e.g. Acero returns Int8, FlightInfo said Int32).
    // getValue() uses sparkFields to return the exact Java type Spark's getInt()/getByte() expects.
    private Field[] sparkFields;

    // current row position within the current batch
    private int rowIdx = 0;
    private int batchRowCount = 0;

    // whether we have a valid current row
    private boolean hasCurrent = false;
    // batch iteration is a separate PartitionReader mode and must not mix with rows
    private boolean columnarMode = false;
    private boolean firstColumnarBatch = true;

    // retry state
    private int openRetryCount = 0;
    private int nextRetryCount = 0;
    private static final int MAX_RETRIES = 3;
    private static final long BACKOFF_BASE_MS = 1000;
    private static final int MAX_NEXT_RETRIES = 2;

    /**
     * Construct a streaming partition reader
     * @param configuration - the configuration of remote flight service
     * @param inputPartition - the input partition to read
     */
    public FlightPartitionReader(Configuration configuration, InputPartition inputPartition) {
        this.configuration = configuration;
        this.inputPartition = inputPartition;
        this.client = Client.getOrCreate(configuration);
    }

    @Override
    public boolean next() throws IOException {
        LOGGER.debug("FlightPartitionReader.next()");
        if (this.columnarMode) {
            throw new IllegalStateException("Cannot mix row and columnar iteration");
        }
        try {
            if (stream == null) {
                if (!openStream()) {
                    LOGGER.info("FlightPartitionReader.next(): openStream() returned false");
                    return false;
                }
                nextRetryCount = 0;
            }

            LOGGER.debug("FlightPartitionReader.next(): rowIdx = {}, batchRowCount = {}", rowIdx, batchRowCount);
            // move to next row in current batch
            rowIdx++;
            if (rowIdx < batchRowCount) {
                hasCurrent = true;
                return true;
            }

            LOGGER.info("FlightPartitionReader.next(): try to read next batch");
            // current batch exhausted, try next batch
            while (true) {
                try {
                    if (!stream.next()) {
                        break;
                    }
                    root = stream.getRoot();
                    batchRowCount = root.getRowCount();
                    fields = Field.from(stream.getSchema());
                    rowIdx = 0;
                    LOGGER.info("FlightPartitionReader.next(): started new batch rowIdx = {}, batchRowCount = {}", rowIdx, batchRowCount);
                    nextRetryCount = 0;

                    if (batchRowCount > 0) {
                        hasCurrent = true;
                        return true;
                    }
                } catch (Exception midStreamError) {
                    // Restarting this stream here would replay rows already returned from
                    // the partition. Let Spark retry the whole task with the retained ticket.
                    LOGGER.warn("Mid-stream read error: {}", midStreamError.getMessage());
                    throw midStreamError;
                }
            }

            LOGGER.info("FlightPartitionReader.next(): no more batches");
            // no more batches
            hasCurrent = false;
            closeStream();
            return false;

        } catch (Exception e) {
            LOGGER.error("Error reading from Flight stream: " + e.getMessage(), e);
            closeStream();
            throw new IOException(e);
        }
    }

    /**
     * Advances by one non-empty Arrow record batch without materializing rows.
     * Used by the DataSource V2 columnar reader.
     *
     * @return true when {@link #currentBatch()} contains a batch
     * @throws IOException on Flight stream failure
     */
    public boolean nextBatch() throws IOException {
        this.columnarMode = true;
        try {
            if (this.stream == null && !openStream()) {
                return false;
            }

            if (this.firstColumnarBatch) {
                this.firstColumnarBatch = false;
                if (this.batchRowCount > 0) {
                    return true;
                }
            }

            while (this.stream.next()) {
                this.root = this.stream.getRoot();
                this.batchRowCount = this.root.getRowCount();
                this.fields = Field.from(this.stream.getSchema());
                if (this.batchRowCount > 0) {
                    return true;
                }
            }

            closeStream();
            return false;
        } catch (Exception e) {
            closeStream();
            throw new IOException("Error reading Arrow batch from Flight stream", e);
        }
    }

    /**
     * Returns the current Flight-owned batch. It remains valid until the next call
     * to {@link #nextBatch()} or {@link #close()}.
     */
    public VectorSchemaRoot currentBatch() {
        if (!this.columnarMode || this.root == null || this.batchRowCount <= 0) {
            throw new IllegalStateException("No current Arrow batch. Call nextBatch() first.");
        }
        return this.root;
    }

    @Override
    public InternalRow get() {
        LOGGER.debug("FlightPartitionReader.get():");

        if (!hasCurrent || root == null) {
            throw new IllegalStateException("No current row. Call next() first.");
        }

        List<org.apache.arrow.vector.FieldVector> fieldVectors = root.getFieldVectors();
        int numCols = fieldVectors.size();
        Object[] values = new Object[numCols];

        Field[] expectedFields = (sparkFields != null && sparkFields.length == numCols) ? sparkFields : fields;
        for (int col = 0; col < numCols; col++) {
            values[col] = getValue(fieldVectors.get(col), rowIdx, expectedFields[col]);
        }

        return new GenericInternalRow(values);
    }

    @Override
    public void close() {
        closeStream();
    }

    /**
     * Open the appropriate stream based on input partition type
     * @return - true if stream opened successfully with data available
     * @throws Exception - if stream opening fails
     */
    private boolean openStream() throws Exception {
        if (this.inputPartition instanceof FlightInputPartition.FlightEndpointInputPartition) {
            return openEndpointStreamWithRetry((FlightInputPartition.FlightEndpointInputPartition) this.inputPartition);
        } else if (this.inputPartition instanceof FlightInputPartition.FlightQueryInputPartition) {
            return openQueryStreamWithRetry((FlightInputPartition.FlightQueryInputPartition) this.inputPartition);
        } else {
            LOGGER.error("FlightPartitionReader.openStream(): Unsupported partition, no data read: {}", this.inputPartition);
            return false;
        }
    }

    /**
     * Open endpoint stream with exponential backoff retry
     * @param dePartition - the endpoint input partition
     * @return - true if stream opened successfully
     * @throws Exception - if all retries fail
     */
    private boolean openEndpointStreamWithRetry(FlightInputPartition.FlightEndpointInputPartition dePartition) throws Exception {
        int maxRetries = this.configuration.getMaxRetries();
        long backoffMs = this.configuration.getRetryBackoffMs();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                closeStream();
                return openEndpointStream(dePartition);
            } catch (Exception e) {
                LOGGER.warn("Failed to open endpoint stream (attempt {} of {}): {}", attempt, maxRetries + 1, e.getMessage());
                if (attempt >= maxRetries) {
                    throw e;
                }
                long delay = backoffMs * (1L << attempt);
                LOGGER.info("Retrying in {}ms", delay);
                Thread.sleep(delay);
            }
        }
        return false;
    }

    /**
     * Open query stream with endpoint fallback and retry logic
     * @param dqPartition - the query input partition
     * @return - true if stream opened successfully
     * @throws Exception - if all retries and endpoints fail
     */
    private boolean openQueryStreamWithRetry(FlightInputPartition.FlightQueryInputPartition dqPartition) throws Exception {
        int maxRetries = this.configuration.getMaxRetries();
        long backoffMs = this.configuration.getRetryBackoffMs();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                closeStream();
                QueryEndpoints qeps = this.client.getQueryEndpoints(dqPartition.getQuery());
                LOGGER.info("FlightPartitionReader.openQueryStream(): endpoints: {}", qeps);
                if (qeps.getEndpoints().length == 0) {
                    return false;
                }

                // try each endpoint in sequence
                Exception lastEpError = null;
                for (int epIdx = 0; epIdx < qeps.getEndpoints().length; epIdx++) {
                    Endpoint endpoint = qeps.getEndpoints()[epIdx];
                    try {
                        if (openSingleEndpoint(endpoint, qeps.getSchema())) {
                            return true;
                        }
                    } catch (Exception epErr) {
                        lastEpError = epErr;
                        LOGGER.warn("Failed to open query endpoint {} of {}: {}", epIdx + 1, qeps.getEndpoints().length, epErr.getMessage());
                    }
                }

                if (lastEpError instanceof RuntimeException) {
                    throw lastEpError;
                }
                if (lastEpError != null) {
                    throw lastEpError;
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to open query stream (attempt {} of {}): {}", attempt, maxRetries + 1, e.getMessage());
                if (attempt >= maxRetries) {
                    throw e;
                }
                long delay = backoffMs * (1L << attempt);
                LOGGER.info("Retrying in {}ms", delay);
                Thread.sleep(delay);
            }
        }
        return false;
    }

    /**
     * Open a single Flight endpoint and populate the stream
     * @param endpoint - the endpoint to connect to
     * @param schema - the expected schema
     * @return - true if data is available from the endpoint
     * @throws Exception - if connection fails
     */
    private boolean openSingleEndpoint(Endpoint endpoint, Schema schema) throws Exception {
        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        this.stream = this.client.openStream(fep);
        this.root = stream.getRoot();
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;
        this.rowIdx = -1;
        this.batchRowCount = 0;

        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }
        return batchRowCount > 0;
    }

    /**
     * Attempt to reopen the stream after a mid-stream failure.
     * Reconnects from the beginning of the partition.
     */
    private boolean reopenStream() {
        if (nextRetryCount >= MAX_NEXT_RETRIES) {
            LOGGER.error("Max mid-stream retries ({}) exceeded", MAX_NEXT_RETRIES);
            return false;
        }
        nextRetryCount++;
        long delay = BACKOFF_BASE_MS * (1L << (nextRetryCount - 1));
        LOGGER.info("Reopening stream after mid-stream failure, retry {}/{}, waiting {}ms",
                nextRetryCount, MAX_NEXT_RETRIES, delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        closeStream();
        try {
            return openStream();
        } catch (Exception e) {
            LOGGER.error("Reopen stream failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Open a stream for a direct endpoint partition
     * @param dePartition - the endpoint input partition
     * @return - true if stream opened successfully
     * @throws Exception - if stream opening fails
     */
    private boolean openEndpointStream(FlightInputPartition.FlightEndpointInputPartition dePartition) throws Exception {
        LOGGER.info("FlightPartitionReader.openEndpointStream(): partition: {}", dePartition);

        Endpoint endpoint = dePartition.getEndpoint();
        Schema schema = dePartition.getSchema();
        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        LOGGER.info("FlightPartitionReader.openEndpointStream(): endpoint: {}", fep);
        this.stream = this.client.openStream(fep);
        LOGGER.info("FlightPartitionReader.openEndpointStream(): stream: {}", stream);
        this.root = stream.getRoot();
        LOGGER.info("FlightPartitionReader.openEndpointStream(): vector schema root: {}", root);
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;  // FlightInfo schema = what Spark's codegen expects
        LOGGER.info("FlightPartitionReader.openEndpointStream(): fields: {}", fields);
        this.rowIdx = -1;
        this.batchRowCount = 0;

        // try first batch
        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }

        LOGGER.info("FlightPartitionReader.openEndpointStream(): first batch row count: {}", batchRowCount);
        return true;
    }

    /**
     * Close the current stream and reset reader state
     */
    private void closeStream() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).warn("Error closing Flight stream: " + e.getMessage());
        } finally {
            stream = null;
            root = null;
            fields = null;
            sparkFields = null;
            batchRowCount = 0;
            rowIdx = 0;
            hasCurrent = false;
        }
    }

    /**
     * Extract a single value from a FieldVector at the given row index,
     * converting it to a Spark-compatible object.
     * @param vector - the field vector to read from
     * @param rowIndex - the row index to extract
     * @param field - the expected field metadata for type coercion
     * @return - the Spark-compatible value, or null if the field is null
     */
    private static Object getValue(org.apache.arrow.vector.FieldVector vector, int rowIndex, Field field) {
        if (vector.isNull(rowIndex)) {
            return null;
        }

        // Int-based vectors — coerce to the Java type Spark's codegen expects.
        // The `field` parameter carries the FlightInfo schema type (what Spark compiled against).
        // Acero may return a narrower vector (e.g. TinyIntVector for a column the old server
        // reported as Int32); without coercion Spark's getInt() would ClassCastException on Byte.
        if (vector instanceof TinyIntVector) {
            byte b = ((TinyIntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) b;
                    case SHORT: return (short) b;
                    case LONG: case BIGINT: return (long) b;
                    default: break;
                }
            }
            return b;
        }
        if (vector instanceof SmallIntVector) {
            short s = ((SmallIntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) s;
                    case LONG: case BIGINT: return (long) s;
                    default: break;
                }
            }
            return s;
        }
        if (vector instanceof IntVector) {
            int i = ((IntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case LONG: case BIGINT: return (long) i;
                    case SHORT: return (short) i;
                    case BYTE:  return (byte) i;
                    default: break;
                }
            }
            return i;
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(rowIndex);
        }

        // Float-based
        if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(rowIndex);
        }
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(rowIndex);
        }

        // Decimal
        if (vector instanceof DecimalVector) {
            BigDecimal bd = ((DecimalVector) vector).getObject(rowIndex);
            return toSparkDecimal(bd, field);
        }
        if (vector instanceof Decimal256Vector) {
            BigDecimal bd = ((Decimal256Vector) vector).getObject(rowIndex);
            return toSparkDecimal(bd, field);
        }

        // String
        if (vector instanceof VarCharVector) {
            return UTF8String.fromString(
                    ((VarCharVector) vector).getObject(rowIndex).toString()
            );
        }
        if (vector instanceof LargeVarCharVector) {
            return UTF8String.fromString(
                    ((LargeVarCharVector) vector).getObject(rowIndex).toString()
            );
        }

        // Boolean
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(rowIndex) != 0;
        }

        // Date
        if (vector instanceof DateDayVector) {
            // Epoch days
            return ((DateDayVector) vector).get(rowIndex);
        }
        if (vector instanceof DateMilliVector) {
            long millis = ((DateMilliVector) vector).get(rowIndex);
            return Math.toIntExact(Math.floorDiv(millis, 86_400_000L));
        }

        // Time
        if (vector instanceof TimeSecVector) {
            return ((TimeSecVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMilliVector) {
            return ((TimeMilliVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMicroVector) {
            return ((TimeMicroVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeNanoVector) {
            return ((TimeNanoVector) vector).getObject(rowIndex).toString();
        }

        // Timestamp
        if (vector instanceof TimeStampSecVector) {
            return Math.multiplyExact(((TimeStampSecVector) vector).get(rowIndex), 1_000_000L);
        }
        if (vector instanceof TimeStampSecTZVector) {
            return Math.multiplyExact(((TimeStampSecTZVector) vector).get(rowIndex), 1_000_000L);
        }
        if (vector instanceof TimeStampMilliVector) {
            return Math.multiplyExact(((TimeStampMilliVector) vector).get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampMilliTZVector) {
            return Math.multiplyExact(((TimeStampMilliTZVector) vector).get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampMicroVector) {
            return ((TimeStampMicroVector) vector).get(rowIndex);
        }
        if (vector instanceof TimeStampMicroTZVector) {
            return ((TimeStampMicroTZVector) vector).get(rowIndex);
        }
        if (vector instanceof TimeStampNanoVector) {
            return Math.floorDiv(((TimeStampNanoVector) vector).get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampNanoTZVector) {
            return Math.floorDiv(((TimeStampNanoTZVector) vector).get(rowIndex), 1_000L);
        }

        // Binary
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).getObject(rowIndex);
        }
        if (vector instanceof LargeVarBinaryVector) {
            return ((LargeVarBinaryVector) vector).getObject(rowIndex);
        }
        if (vector instanceof FixedSizeBinaryVector) {
            return ((FixedSizeBinaryVector) vector).getObject(rowIndex);
        }

        // UInt
        if (vector instanceof UInt1Vector) {
            return (int) ((UInt1Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt2Vector) {
            return (int) ((UInt2Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt4Vector) {
            return (int) ((UInt4Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt8Vector) {
            return ((UInt8Vector) vector).get(rowIndex);
        }

        // Duration, Interval
        if (vector instanceof DurationVector) {
            return ((DurationVector) vector).getObject(rowIndex);
        }
        if (vector instanceof IntervalYearVector) {
            return ((IntervalYearVector) vector).get(rowIndex);
        }
        if (vector instanceof IntervalDayVector) {
            return ((IntervalDayVector) vector).getObject(rowIndex);
        }

        // Null
        if (vector instanceof NullVector) {
            return null;
        }

        // Default fallback — getObject
        return vector.getObject(rowIndex);
    }

    private static Decimal toSparkDecimal(BigDecimal value, Field field) {
        if (value == null) {
            return null;
        }
        if (field != null && field.getType() instanceof FieldType.DecimalType decimalType) {
            return Decimal.apply(value, decimalType.getPrecision(), decimalType.getScale());
        }
        return Decimal.apply(value);
    }
}
