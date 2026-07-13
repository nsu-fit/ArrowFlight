package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import static net.surpin.data.arrowflight.server.adapters.HostUtils.LOOPBACK_HOSTS;

/**
 * Reads Parquet metadata and file listings from HDFS.
 * Maintains caches for schemas, tables, and DDL strings.
 */
public class ParquetAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetAdapter.class);

    private final FileSystem fileSystem;
    private final String dataDirectory;
    private final String localhost;
    private final JavaTypeFactoryImpl typeFactory;

    private Map<String, Path> tableSchemaCache;
    private Map<String, Map<String, Path>> tableCache;
    private final Map<String, Map<String, String>> tableDdlCache = new HashMap<>();

    /**
     * Creates a ParquetAdapter for the given Hadoop filesystem and data directory.
     *
     * @param appConfig  server configuration
     * @param fileSystem Hadoop FileSystem instance
     */
    public ParquetAdapter(AppConfig appConfig, FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        this.dataDirectory = appConfig.dataDir();
        this.localhost = "localhost";
        this.typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        try {
            initSchemaCache();
            initTableCache();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a ParquetAdapter with an explicit localhost override.
     *
     * @param appConfig  server configuration
     * @param fileSystem Hadoop FileSystem instance
     * @param localhost  local hostname for block locality resolution
     */
    public ParquetAdapter(AppConfig appConfig, FileSystem fileSystem, String localhost) {
        this.fileSystem = fileSystem;
        this.dataDirectory = appConfig.dataDir();
        this.localhost = localhost;
        this.typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        try {
            initSchemaCache();
            initTableCache();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Lists all schemas (top-level directories) matching an optional filter pattern.
     *
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of schema name to path
     * @throws IOException on HDFS read failure
     */
    public Map<String, Path> getSchemas(String filterExpression) throws IOException {
        java.util.function.Predicate<String> schemaPredicate = createLikePredicate(filterExpression);
        return tableSchemaCache.entrySet().stream()
                .filter(entry -> filterExpression == null || schemaPredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Lists all tables in a schema matching an optional filter pattern.
     *
     * @param schema           schema name
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of table name to path
     */
    public Map<String, Path> getTables(String schema, String filterExpression) {
        Map<String, Path> tables = tableCache.getOrDefault(schema, Collections.emptyMap());
        java.util.function.Predicate<String> tablePredicate = createLikePredicate(filterExpression);
        return tables.entrySet().stream()
                .filter(entry -> filterExpression == null || tablePredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns the Arrow schema for a table.
     *
     * @param schema schema name
     * @param table  table name
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table) {
        return getTableSchema(schema, table, null);
    }

    /**
     * Returns the Arrow schema for a table, optionally filtering to specific columns.
     *
     * @param schema  schema name
     * @param table   table name
     * @param columns columns to include, null or empty for all
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table, List<String> columns) {
        validateName(schema);
        validateName(table);
        try {
            Path tableDirectoryPath = new Path(dataDirectory, schema + "/" + table);
            RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(tableDirectoryPath, true);

            Path parquetPath = null;
            while (it.hasNext()) {
                LocatedFileStatus lfs = it.next();
                if (lfs.isFile() && lfs.getPath().getName().endsWith(".parquet")) {
                    parquetPath = lfs.getPath();
                    break;
                }
            }

            if (parquetPath == null) {
                return new Schema(Collections.emptyList(), null);
            }

            MessageType parquetSchema;
            final long fileLen = fileSystem.getFileStatus(parquetPath).getLen();
            final Path finalPath = parquetPath;

            try (ParquetFileReader reader = ParquetFileReader.open(new org.apache.parquet.io.InputFile() {
                @Override
                public long getLength() {
                    return fileLen;
                }

                @Override
                public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                    return org.apache.parquet.hadoop.util.HadoopStreams.wrap(fileSystem.open(finalPath));
                }
            })) {
                parquetSchema = reader.getFooter().getFileMetaData().getSchema();
            }

            return SchemaConverter.convert(
                    parquetSchema,
                    cd -> columns == null || columns.isEmpty()
                            || cd.getPath().length == 1 && columns.contains(cd.getPath()[0]));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts an Arrow schema to a DuckDB-compatible DDL statement.
     *
     * @param tableSchema schema name
     * @param tableName   table name
     * @param schema      Arrow schema
     * @return DDL string
     */
    public String arrowSchemaToDDL(String tableSchema, String tableName, Schema schema) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(schema);

        StringBuilder result = new StringBuilder("CREATE TABLE ");
        if (tableSchema != null) {
            result.append(tableSchema).append(".");
        }
        result.append(tableName).append("(\n");

        List<Field> fields = schema.getFields();
        IntStream.range(0, fields.size()).forEach(i -> {
            Field field = fields.get(i);
            RelDataType relDataType;
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                relDataType = ArrowFieldTypeFactory.toType(new ArrowType.Int(64, true), typeFactory);
            } else {
                relDataType = ArrowFieldTypeFactory.toType(field.getType(), typeFactory);
            }
            if (i > 0) {
                result.append(",\n");
            }
            result.append("\t\"").append(field.getName()).append("\" ").append(relDataType);
        });
        result.append(")");
        return result.toString();
    }

    /**
     * Builds DDL for all tables and populates the DDL cache.
     * Also warms up Acero JNI and DuckDB connections.
     */
    public void initCatalogReader() {
        Objects.requireNonNull(tableSchemaCache, "Initialize schema cache first");
        Objects.requireNonNull(tableCache, "Initialize table cache first");

        StringBuilder ddlBuilder = new StringBuilder();
        tableCache.forEach((schemaName, tablesMap) -> {
            tableDdlCache.putIfAbsent(schemaName, new HashMap<>());
            tablesMap.forEach((tableName, path) -> {
                Schema schema = getTableSchema(schemaName, tableName);
                String ddl = arrowSchemaToDDL(schemaName, tableName, schema);
                tableDdlCache.get(schemaName).put(tableName, ddl);
                ddlBuilder.append(ddl).append(";\n");
            });
        });

        LOGGER.info("Parsed DDL: {}", ddlBuilder.toString());
    }

    /**
     * Returns the DDL cache (schema → table → DDL string).
     *
     * @return DDL map
     */
    public Map<String, Map<String, String>> tableDdlCache() {
        return tableDdlCache;
    }

    /**
     * Resolves file locations for a SQL query across all relevant Parquet files.
     *
     * @param query SQL query
     * @return map of relative file path to FileAssignment with locality info
     * @throws IOException on HDFS read failure
     */
    public Map<String, FileAssignment> locationsForQuery(String query) throws IOException {
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        Map<String, FileAssignment> result = new HashMap<>();
        URI dataDirectoryURI = fileSystem.getFileStatus(new Path(dataDirectory)).getPath().toUri();

        if (parsedQuery.isJoin) {
            for (ParquetQueryParser.JoinTable jt : parsedQuery.joinTables) {
                validateName(jt.schema());
                validateName(jt.table());
                Path parquetPath = new Path(dataDirectory, jt.schema() + "/" + jt.table());
                RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(parquetPath, true);
                while (filesIter.hasNext()) {
                    LocatedFileStatus file = filesIter.next();
                    if (file.isDirectory() || !file.getPath().getName().toLowerCase().endsWith(".parquet")) {
                        continue;
                    }
                    String relativePath = dataDirectoryURI.relativize(file.getPath().toUri()).toString();
                    result.putIfAbsent(relativePath, new FileAssignment(file.getLen(), fileLocality(file).keySet()));
                }
            }
            return result;
        }

        validateName(parsedQuery.schema);
        validateName(parsedQuery.table);
        Path parquetPath = new Path(dataDirectory, parsedQuery.schema + "/" + parsedQuery.table);
        RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(parquetPath, true);
        while (filesIter.hasNext()) {
            LocatedFileStatus file = filesIter.next();
            if (file.isDirectory() || !file.getPath().getName().toLowerCase().endsWith(".parquet")) {
                continue;
            }
            String relativePath = dataDirectoryURI.relativize(file.getPath().toUri()).toString();
            result.put(relativePath, new FileAssignment(file.getLen(), fileLocality(file).keySet()));
        }
        return result;
    }

    /**
     * Lists every Parquet file physically visible to this server. Paths are relative
     * to the server data root so the same ticket can be resolved against the target
     * node's own local root.
     *
     * @return relative Parquet path to file size
     * @throws IOException on file-system access failure
     */
    public Map<String, Long> localFileInventory() throws IOException {
        Map<String, Long> result = new LinkedHashMap<>();
        Path root = new Path(this.dataDirectory);
        if (!this.fileSystem.exists(root)) {
            return result;
        }

        URI rootUri = this.fileSystem.getFileStatus(root).getPath().toUri();
        RemoteIterator<LocatedFileStatus> files = this.fileSystem.listFiles(root, true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.isFile() && file.getPath().getName().toLowerCase().endsWith(".parquet")) {
                String relativePath = rootUri.relativize(file.getPath().toUri()).toString();
                result.put(relativePath, file.getLen());
            }
        }
        return result;
    }

    /**
     * Returns block locality for a file, mapping host to block count.
     *
     * @param locatedFileStatus file status
     * @return ordered map of host to block count
     */
    public LinkedHashMap<String, Long> fileLocality(LocatedFileStatus locatedFileStatus) {
        Objects.requireNonNull(locatedFileStatus);
        return Arrays.stream(locatedFileStatus.getBlockLocations())
                .flatMap(bl -> {
                    try {
                        return Stream.of(bl.getHosts())
                                .map(host -> LOOPBACK_HOSTS.contains(host) ? localhost : host);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.groupingBy(h -> h, Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * Returns the data directory path.
     *
     * @return data directory path string
     */
    public String dataDirectory() {
        return dataDirectory;
    }

    /**
     * Validates that a schema or table name contains only safe characters.
     * Prevents path traversal via names like ".." or "./etc".
     */
    public static String validateName(String name) {
        if (name == null) {
            return null;
        }
        if (!name.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid name: '" + name
                    + "'. Must match [a-zA-Z_][a-zA-Z0-9_]*");
        }
        return name;
    }

    /**
     * Returns the Hadoop filesystem instance.
     *
     * @return FileSystem
     */
    public FileSystem fileSystem() {
        return fileSystem;
    }

    /**
     * Scans data directory for schema directories and caches them.
     *
     * @throws IOException on HDFS read failure
     */
    private void initSchemaCache() throws IOException {
        LOGGER.info("Initializing schema cache for data directory: {}", dataDirectory);
        Path dirPath = new Path(dataDirectory);

        if (!fileSystem.exists(dirPath)) {
            tableSchemaCache = Collections.emptyMap();
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return;
        }

        tableSchemaCache = Arrays.stream(fileSystem.listStatus(dirPath))
                .filter(FileStatus::isDirectory)
                .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));

        LOGGER.info("Collected schemas: {}", tableSchemaCache);
    }

    /**
     * Scans each schema directory for table directories and caches them.
     *
     * @throws IOException on HDFS read failure
     */
    private void initTableCache() throws IOException {
        LOGGER.info("Initializing table cache for data directory: {}", dataDirectory);
        Path schemaPath = new Path(dataDirectory);

        if (!fileSystem.exists(schemaPath)) {
            tableCache = Collections.emptyMap();
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return;
        }

        tableCache = new HashMap<>();
        getSchemas(null).forEach((key, value) -> {
            LOGGER.info("Collecting tables for schema: {} at path {}", key, value);
            try {
                Map<String, Path> tables = Arrays.stream(fileSystem.listStatus(value))
                        .filter(FileStatus::isDirectory)
                        .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));
                tableCache.put(key, tables);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        LOGGER.info("Collected tables: {}", tableCache);
    }

    /**
     * Creates a predicate from a SQL LIKE pattern.
     *
     * @param pattern SQL LIKE pattern
     * @return predicate
     */
    private static java.util.function.Predicate<String> createLikePredicate(String pattern) {
        if (pattern == null || pattern.isEmpty() || pattern.equals("%")) {
            return s -> true;
        }
        String regex = pattern
                .replace("\\", "\\\\")
                .replace(".", "\\.")
                .replace("%", ".*")
                .replace("_", ".");
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex,
                java.util.regex.Pattern.CASE_INSENSITIVE);
        return s -> p.matcher(s).matches();
    }
}
