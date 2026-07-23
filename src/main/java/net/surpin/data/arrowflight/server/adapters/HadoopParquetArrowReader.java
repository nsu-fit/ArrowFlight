package net.surpin.data.arrowflight.server.adapters;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** Reads Hadoop Parquet files into bounded Arrow record batches. */
public final class HadoopParquetArrowReader extends ArrowReader {

    private final FileSystem fileSystem;
    private final List<Path> files;
    private final Set<String> requestedColumns;
    private final int batchSize;
    private int fileIndex;
    private ParquetFileReader fileReader;
    private MessageType parquetSchema;
    private Schema arrowSchema;
    private List<ColumnReader> columnReaders;
    private long rowsRemaining;
    private long bytesRead;

    /**
     * Creates a reader for ordered Parquet files.
     *
     * @param allocator Arrow allocator
     * @param fileSystem Hadoop filesystem
     * @param files Parquet files
     * @param batchSize maximum rows per Arrow batch
     * @throws IOException when the first file cannot be opened
     */
    public HadoopParquetArrowReader(BufferAllocator allocator, FileSystem fileSystem,
            List<Path> files, int batchSize) throws IOException {
        this(allocator, fileSystem, files, batchSize, Set.of());
    }

    /**
     * Creates reader with optional top-level column projection.
     *
     * @param allocator Arrow allocator
     * @param fileSystem Hadoop filesystem
     * @param files Parquet files
     * @param batchSize maximum rows per Arrow batch
     * @param requestedColumns requested top-level columns, or empty for all
     * @throws IOException when the first file cannot be opened
     */
    public HadoopParquetArrowReader(BufferAllocator allocator, FileSystem fileSystem,
            List<Path> files, int batchSize, Set<String> requestedColumns) throws IOException {
        super(allocator);
        if (files.isEmpty()) {
            throw new IOException("Cannot create Arrow stream from an empty Parquet file list");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        }
        this.fileSystem = fileSystem;
        this.files = List.copyOf(files);
        this.batchSize = batchSize;
        this.requestedColumns = Set.copyOf(requestedColumns);
        openNextFile();
    }

    @Override
    protected Schema readSchema() throws IOException {
        return arrowSchema;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        VectorSchemaRoot root = getVectorSchemaRoot();
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(batchSize);
            vector.allocateNew();
        }
        if (!ensureColumnReaders()) {
            root.setRowCount(0);
            return false;
        }
        int rowCount = (int) Math.min(batchSize, rowsRemaining);
        for (int field = 0; field < root.getFieldVectors().size(); field++) {
            ColumnReader columnReader = columnReaders.get(field);
            PrimitiveType parquetType = parquetSchema.getType(field).asPrimitiveType();
            FieldVector vector = root.getVector(field);
            for (int row = 0; row < rowCount; row++) {
                writeValue(vector, columnReader, parquetType, row);
                columnReader.consume();
            }
        }
        rowsRemaining -= rowCount;
        root.setRowCount(rowCount);
        return true;
    }

    @Override
    public long bytesRead() {
        return bytesRead;
    }

    @Override
    protected void closeReadSource() throws IOException {
        if (fileReader != null) {
            fileReader.close();
            fileReader = null;
        }
    }

    /**
     * Advances to a row group containing rows.
     *
     * @return true when a row can be read
     * @throws IOException on Parquet read failure
     */
    private boolean ensureColumnReaders() throws IOException {
        while (rowsRemaining == 0) {
            if (fileReader == null) {
                return false;
            }
            PageReadStore pages = fileReader.readNextRowGroup();
            if (pages != null) {
                rowsRemaining = pages.getRowCount();
                GroupRecordConverter converter = new GroupRecordConverter(parquetSchema);
                ColumnReadStoreImpl columns = new ColumnReadStoreImpl(pages,
                        converter.getRootConverter(), parquetSchema,
                        fileReader.getFileMetaData().getCreatedBy());
                columnReaders = parquetSchema.getColumns().stream()
                        .map(columns::getColumnReader)
                        .toList();
                if (rowsRemaining != 0) {
                    return true;
                }
                continue;
            }
            if (!openNextFile()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Opens the next file and verifies schema compatibility.
     *
     * @return true when a file was opened
     * @throws IOException on open or schema failure
     */
    private boolean openNextFile() throws IOException {
        if (fileReader != null) {
            fileReader.close();
            fileReader = null;
        }
        if (fileIndex >= files.size()) {
            return false;
        }
        Path path = files.get(fileIndex++);
        long fileLength = fileSystem.getFileStatus(path).getLen();
        try {
            fileReader = ParquetFileReader.open(new InputFile() {
                @Override
                public long getLength() {
                    return fileLength;
                }

                @Override
                public SeekableInputStream newStream() throws IOException {
                    return HadoopStreams.wrap(fileSystem.open(path));
                }
            });
            bytesRead += fileLength;
            MessageType fullSchema = fileReader.getFooter().getFileMetaData().getSchema();
            validateFlatSchema(fullSchema);
            MessageType nextSchema = projectedSchema(fullSchema);
            fileReader.setRequestedSchema(nextSchema);
            Schema nextArrowSchema = new SchemaConverter().fromParquet(nextSchema).getArrowSchema();
            if (arrowSchema != null && !arrowSchema.equals(nextArrowSchema)) {
                throw new IOException("Parquet schema mismatch in " + path);
            }
            parquetSchema = nextSchema;
            arrowSchema = nextArrowSchema;
            columnReaders = null;
            rowsRemaining = 0;
            return true;
        } catch (IOException | RuntimeException e) {
            if (fileReader != null) {
                fileReader.close();
                fileReader = null;
            }
            throw e;
        }
    }

    /**
     * Builds projected flat schema while retaining all columns when projection is unknown.
     *
     * @param schema full Parquet schema
     * @return projected schema
     */
    private MessageType projectedSchema(MessageType schema) {
        if (requestedColumns.isEmpty() || requestedColumns.contains("*")) {
            return schema;
        }
        List<Type> fields = schema.getFields().stream()
                .filter(field -> requestedColumns.contains(field.getName()))
                .toList();
        if (fields.isEmpty()) {
            return schema;
        }
        return new MessageType(schema.getName(), fields);
    }

    /**
     * Rejects structures not supported by the flat column decoder.
     *
     * @param schema Parquet schema
     * @throws IOException when nested or repeated data is present
     */
    private static void validateFlatSchema(MessageType schema) throws IOException {
        for (Type field : schema.getFields()) {
            if (!field.isPrimitive() || field.isRepetition(Type.Repetition.REPEATED)) {
                throw new IOException("Unsupported nested or repeated Parquet field: "
                        + field.getName());
            }
        }
    }

    /**
     * Writes one Parquet field into its Arrow vector.
     *
     * @param vector target vector
     * @param columnReader source Parquet column reader
     * @param parquetType source Parquet type
     * @param row target row index
     * @throws IOException when the Arrow type is unsupported
     */
    private static void writeValue(FieldVector vector, ColumnReader columnReader,
            PrimitiveType parquetType, int row)
            throws IOException {
        if (columnReader.getCurrentDefinitionLevel()
                < columnReader.getDescriptor().getMaxDefinitionLevel()) {
            vector.setNull(row);
            return;
        }
        if (vector instanceof BitVector value) {
            value.setSafe(row, columnReader.getBoolean() ? 1 : 0);
        } else if (vector instanceof TinyIntVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof SmallIntVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof IntVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof BigIntVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof Float4Vector value) {
            value.setSafe(row, columnReader.getFloat());
        } else if (vector instanceof Float8Vector value) {
            value.setSafe(row, columnReader.getDouble());
        } else if (vector instanceof VarCharVector value) {
            java.nio.ByteBuffer bytes = columnReader.getBinary().toByteBuffer();
            value.setSafe(row, bytes, bytes.position(), bytes.remaining());
        } else if (vector instanceof VarBinaryVector value) {
            java.nio.ByteBuffer bytes = columnReader.getBinary().toByteBuffer();
            value.setSafe(row, bytes, bytes.position(), bytes.remaining());
        } else if (vector instanceof FixedSizeBinaryVector value) {
            value.setSafe(row, columnReader.getBinary().getBytesUnsafe());
        } else if (vector instanceof DecimalVector value) {
            BigInteger unscaled = switch (parquetType.getPrimitiveTypeName()) {
                case INT32 -> BigInteger.valueOf(columnReader.getInteger());
                case INT64 -> BigInteger.valueOf(columnReader.getLong());
                case BINARY, FIXED_LEN_BYTE_ARRAY ->
                    new BigInteger(columnReader.getBinary().getBytesUnsafe());
                default -> throw new IOException("Unsupported Parquet decimal storage for field "
                        + vector.getName());
            };
            value.setSafe(row, new java.math.BigDecimal(unscaled, value.getScale()));
        } else if (vector instanceof DateDayVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof TimeSecVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof TimeMilliVector value) {
            value.setSafe(row, columnReader.getInteger());
        } else if (vector instanceof TimeMicroVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof TimeNanoVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof TimeStampSecVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof TimeStampMilliVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof TimeStampMicroVector value) {
            value.setSafe(row, columnReader.getLong());
        } else if (vector instanceof TimeStampNanoVector value) {
            value.setSafe(row, columnReader.getLong());
        } else {
            ArrowType type = vector.getField().getType();
            throw new IOException("Unsupported Arrow type " + type + " for field "
                    + vector.getName());
        }
    }
}
