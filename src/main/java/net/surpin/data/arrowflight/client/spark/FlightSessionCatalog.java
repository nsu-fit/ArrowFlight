package net.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

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
        return source.getTableFromCatalog(sourceOptions, v1Table.schema());
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
