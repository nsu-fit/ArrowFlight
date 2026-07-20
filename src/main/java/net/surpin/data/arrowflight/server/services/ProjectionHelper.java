package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;

/**
 * Static helpers for building column projections from parsed queries.
 */
public final class ProjectionHelper {

    private ProjectionHelper() {
    }

    /**
     * Builds column projection array from parsed query, including filter columns.
     *
     * @param pq             parsed query
     * @param parquetAdapter parquet adapter for schema lookup
     * @return projected column names, empty if none needed
     */
    public static Optional<String[]> buildProjection(ParquetQueryParser pq,
            ParquetAdapter parquetAdapter) {
        Set<String> scanCols = projectedColumns(pq);
        if (scanCols.isEmpty()) {
            Schema tSchema = parquetAdapter.getTableSchema(pq.schema, pq.table);
            if (!tSchema.getFields().isEmpty()) {
                scanCols.add(tSchema.getFields().get(0).getName());
            }
        }
        return scanCols.isEmpty() ? Optional.empty()
                : Optional.of(scanCols.toArray(new String[0]));
    }

    /**
     * Collects physical columns needed by the projection, aggregation, and filter.
     *
     * @param pq parsed query
     * @return columns that must be present in the Acero stream
     */
    public static Set<String> projectedColumns(ParquetQueryParser pq) {
        Set<String> scanCols = new LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr e : pq.selectExprs) {
            scanCols.addAll(e.inputColumns);
        }
        if (pq.filter != null && !pq.filter.isBlank()) {
            Matcher m = Pattern.compile("\"([^\"]+)\"").matcher(pq.filter);
            while (m.find()) {
                scanCols.add(m.group(1));
            }
        }
        return scanCols;
    }
}
