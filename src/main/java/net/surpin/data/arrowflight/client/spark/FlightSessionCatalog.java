package net.surpin.data.arrowflight.client.spark;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.JavaConverters;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Session-catalog bridge that rehydrates persisted Flight data-source tables as
 * DataSource V2 tables while delegating every other catalog operation to Spark.
 *
 * <p>Spark's built-in session catalog loads persisted {@code CREATE TABLE USING}
 * metadata as {@link V1Table}, even when the provider implements TableProvider.
 * Spark Thrift therefore cannot otherwise reach Flight's V2 projection, filter,
 * aggregate and columnar scan interfaces.</p>
 */
public final class FlightSessionCatalog extends DelegatingCatalogExtension {
    private final ConcurrentMap<String, Schema> remoteSchemas = new ConcurrentHashMap<>();

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
            throws NoSuchTableException {
        org.apache.spark.sql.connector.catalog.Table loaded = super.loadTable(ident);
        if (!(loaded instanceof V1Table v1Table) || !isFlightProvider(v1Table)) {
            return loaded;
        }

        Map<String, String> options = new HashMap<>(
                JavaConverters.mapAsJavaMap(v1Table.options()));
        FlightSource source = new FlightSource();
        CaseInsensitiveStringMap sourceOptions = new CaseInsensitiveStringMap(options);
        // Spark's persisted StructType cannot preserve Arrow dictionary encoding,
        // decimal width, floating-point precision, or time units. Resolve exact
        // metadata once per remote table and reuse it across scan planning.
        Schema schema = this.remoteSchemas.computeIfAbsent(
                schemaCacheKey(ident, sourceOptions), ignored -> {
                    FlightSource schemaSource = new FlightSource();
                    Schema inferred = schemaSource.inferArrowSchema(sourceOptions);
                    if (inferred.getFields().isEmpty()) {
                        throw new IllegalStateException(
                                "Flight returned an empty schema for "
                                        + sourceOptions.getOrDefault("table", "<unknown>")
                                        + "; verify that the configured Flight node has a local Parquet shard");
                    }
                    return inferred;
                });
        return source.getTableFromCatalog(sourceOptions, schema);
    }

    /**
     * Builds an opaque schema-cache key isolated by catalog identifier and options.
     *
     * @param ident local persisted-table identifier
     * @param options remote connection and routing options
     * @return deterministic opaque cache key
     */
    static String schemaCacheKey(
            Identifier ident, CaseInsensitiveStringMap options) {
        String identifier = String.join("\u0000", ident.namespace())
                + "\u0000" + ident.name();
        String optionIdentity = options.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "\u0000" + entry.getValue())
                .collect(Collectors.joining("\u0000"));
        return sha256(identifier + "\u0000" + optionIdentity);
    }

    /**
     * Hashes cache identity material without retaining credentials in map keys.
     *
     * @param value identity material
     * @return lowercase SHA-256 digest
     */
    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static boolean isFlightProvider(V1Table table) {
        Option<String> provider = table.catalogTable().provider();
        if (provider.isEmpty()) {
            return false;
        }
        String name = provider.get();
        return FlightSource.class.getName().equals(name) || "flight".equalsIgnoreCase(name);
    }
}
