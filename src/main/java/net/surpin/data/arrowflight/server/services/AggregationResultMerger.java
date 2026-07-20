package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.MetadataService;

/**
 * Static helpers for merging partial aggregation results and emitting them as Arrow batches.
 */
public final class AggregationResultMerger {

    private AggregationResultMerger() {
    }

    /**
     * Emits aggregation result rows as Arrow batches through the listener.
     *
     * @param allocator       Arrow buffer allocator
     * @param pq              parsed query
     * @param rows            result rows
     * @param listener        Flight stream listener
     * @param startListener   whether to call listener.start()
     * @param metadataService metadata service for schema lookup
     * @param appConfig       server configuration
     * @throws InterruptedException if interrupted during send
     */
    public static void emitRowsAsArrow(BufferAllocator allocator, ParquetQueryParser pq,
            List<Object[]> rows, FlightProducer.ServerStreamListener listener,
            boolean startListener, MetadataService metadataService,
            AppConfig appConfig) throws InterruptedException {

        Schema aggSchema = aggregationSchema(pq, metadataService);
        int numGbCols = pq.groupByColumnNames.size();

        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : aggSchema.getFields()) {
            FieldVector v = field.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(rows.size());
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(rows.size() * 16);
            }
            vectors.add(v);
        }

        try (VectorSchemaRoot root = new VectorSchemaRoot(aggSchema.getFields(), vectors)) {
            root.setRowCount(rows.size());
            int vecIdx = 0;
            int aggIdx = 0;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                FieldVector vec = vectors.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    for (int r = 0; r < rows.size(); r++) {
                        VectorUtils.setVectorValue(vec, r, rows.get(r)[gbPos]);
                    }
                } else {
                    int pos = numGbCols + aggIdx++;
                    for (int r = 0; r < rows.size(); r++) {
                        VectorUtils.setVectorValue(vec, r, rows.get(r)[pos]);
                    }
                }
            }
            if (startListener) {
                listener.start(root);
            }
            if (!rows.isEmpty()) {
                if (DuckDbAdapter.awaitListenerReady(
                        listener, appConfig.flightListenerReadyTimeoutMillis())) {
                    listener.putNext();
                }
            }
        }
    }

    /**
     * Builds a SELECT * query for schema inference.
     *
     * @param pq parsed query
     * @return SELECT * query string
     */
    public static String buildSelectExprQuery(ParquetQueryParser pq) {
        if (pq.schema != null && pq.table != null) {
            return "SELECT * FROM " + pq.schema + "." + pq.table;
        }
        throw new IllegalArgumentException("Cannot build schema query: missing schema/table");
    }

    /**
     * Returns the aggregation result schema.
     *
     * @param pq              parsed query
     * @param metadataService metadata service for schema resolution
     * @return aggregation schema
     */
    public static Schema aggregationSchema(ParquetQueryParser pq,
            MetadataService metadataService) {
        if (pq.selectExprs.isEmpty()) {
            return metadataService.getQuerySchema(buildSelectExprQuery(pq));
        }
        return metadataService.buildAggregationSchema(pq);
    }

    /**
     * Creates output vectors for the given schema.
     *
     * @param allocator  Arrow buffer allocator
     * @param outSchema  output schema
     * @param totalRows  number of rows to allocate
     * @return list of field vectors
     */
    public static List<FieldVector> createOutputVectors(BufferAllocator allocator,
            Schema outSchema, int totalRows) {
        List<FieldVector> outVecs = new ArrayList<>();
        for (Field f : outSchema.getFields()) {
            FieldVector v = f.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(Math.max(totalRows, 1));
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(Math.max(totalRows, 1) * 16);
            }
            outVecs.add(v);
        }
        return outVecs;
    }

    /**
     * Merges partial aggregation results from parallel file groups.
     *
     * @param allocator       Arrow buffer allocator
     * @param pq              parsed query
     * @param partials        partial VSRs from each group
     * @param metadataService metadata service for schema resolution
     * @return merged VectorSchemaRoot
     */
    public static VectorSchemaRoot mergeVsrPartials(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials,
            MetadataService metadataService) {
        if (pq.groupByColumnNames.isEmpty()) {
            return mergeWithoutGroupBy(allocator, pq, partials, metadataService);
        }
        return mergeWithGroupBy(allocator, pq, partials, metadataService);
    }

    private static VectorSchemaRoot mergeWithoutGroupBy(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials,
            MetadataService metadataService) {
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
        Long[] longAccum = new Long[exprs.size()];
        Object[] sumAccum = new Object[exprs.size()];
        Object[] objAccum = new Object[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            longAccum[i] = 0L;
        }
        boolean any = false;

        for (VectorSchemaRoot partial : partials) {
            if (partial.getRowCount() == 0) {
                continue;
            }
            any = true;
            int col = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector vec = partial.getVector(col);
                if (!vec.isNull(0)) {
                    switch (expr.func) {
                        case COUNT_STAR, COUNT -> longAccum[col] = (Long) VectorUtils.addLongs(longAccum[col], VectorUtils.toLong(vec, 0));
                        case SUM -> sumAccum[col] = VectorUtils.addNumbers(
                                sumAccum[col], vec.getObject(0));
                        case MIN -> objAccum[col] = objAccum[col] == null
                                ? vec.getObject(0) : VectorUtils.minOf(objAccum[col], vec.getObject(0));
                        case MAX -> objAccum[col] = objAccum[col] == null
                                ? vec.getObject(0) : VectorUtils.maxOf(objAccum[col], vec.getObject(0));
                        default -> {
                        }
                    }
                }
                col++;
            }
        }

        Schema outSchema = aggregationSchema(pq, metadataService);
        List<FieldVector> outVecs = createOutputVectors(allocator, outSchema, 1);
        if (any) {
            int col = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector v = outVecs.get(col);
                switch (expr.func) {
                    case COUNT_STAR, COUNT -> {
                        if (longAccum[col] != null) {
                            ((BigIntVector) v).setSafe(0, longAccum[col]);
                        }
                    }
                    case SUM -> {
                        if (sumAccum[col] != null) {
                            VectorUtils.setVectorValue(v, 0, sumAccum[col]);
                        }
                    }
                    case MIN, MAX -> VectorUtils.setVectorValue(v, 0, objAccum[col]);
                    default -> {
                    }
                }
                col++;
            }
        }
        VectorSchemaRoot r = new VectorSchemaRoot(outSchema.getFields(), outVecs);
        r.setRowCount(any ? 1 : 0);
        return r;
    }

    /**
     * Merges partial results with group-by columns.
     */
    private static VectorSchemaRoot mergeWithGroupBy(BufferAllocator allocator,
            ParquetQueryParser pq, List<VectorSchemaRoot> partials,
            MetadataService metadataService) {
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
        int numGbCols = pq.groupByColumnNames.size();
        int numAggExprs = (int) exprs.stream()
                .filter(e -> e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN).count();
        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();

        for (VectorSchemaRoot partial : partials) {
            Schema partialSchema = partial.getSchema();
            List<Field> partialFields = partialSchema.getFields();

            Map<String, Integer> colIndexByName = new LinkedHashMap<>();
            for (int i = 0; i < partialFields.size(); i++) {
                colIndexByName.put(partialFields.get(i).getName()
                        .toLowerCase(java.util.Locale.ROOT), i);
            }

            int rowCount = partial.getRowCount();
            for (int r = 0; r < rowCount; r++) {
                List<Object> key = new ArrayList<>(numGbCols);
                for (int c = 0; c < numGbCols; c++) {
                    FieldVector gv = numGbCols > c && c < partialFields.size()
                            ? partial.getVector(c) : null;
                    key.add(gv != null ? gv.getObject(r) : null);
                }
                Object[] accum = byKey.computeIfAbsent(key, k -> new Object[numAggExprs]);
                int ai = 0;
                for (ParquetQueryParser.SelectExpr expr : exprs) {
                    if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                        continue;
                    }
                    int colPos = numGbCols + ai;
                    String expectedName = expr.outputName != null ? expr.outputName
                            : expr.inputColumn;
                    Integer namedIdx = expectedName != null
                            ? colIndexByName.get(expectedName
                                    .toLowerCase(java.util.Locale.ROOT))
                            : null;
                    FieldVector vec = namedIdx != null
                            ? partial.getVector(namedIdx)
                            : (colPos < partialFields.size() ? partial.getVector(colPos) : null);
                    if (vec != null && !vec.isNull(r)) {
                        Object val = vec.getObject(r);
                        switch (expr.func) {
                            case COUNT_STAR, COUNT -> accum[ai] = VectorUtils.addLongs(accum[ai], val);
                            case SUM -> accum[ai] = VectorUtils.addNumbers(accum[ai], val);
                            case MIN -> accum[ai] = accum[ai] == null ? val : VectorUtils.minOf(accum[ai], val);
                            case MAX -> accum[ai] = accum[ai] == null ? val : VectorUtils.maxOf(accum[ai], val);
                            default -> {
                            }
                        }
                    }
                    ai++;
                }
            }
        }

        Schema outSchema = aggregationSchema(pq, metadataService);
        int totalRows = byKey.size();
        List<FieldVector> outVecs = createOutputVectors(allocator, outSchema, totalRows);
        int row = 0;
        for (Map.Entry<List<Object>, Object[]> entry : byKey.entrySet()) {
            List<Object> key = entry.getKey();
            Object[] accum = entry.getValue();
            int vecIdx = 0;
            int ai = 0;
            for (ParquetQueryParser.SelectExpr expr : exprs) {
                FieldVector v = outVecs.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    VectorUtils.setVectorValue(v, row, key.get(gbPos));
                } else {
                    VectorUtils.setVectorValue(v, row, accum[ai++]);
                }
            }
            row++;
        }
        for (FieldVector v : outVecs) {
            v.setValueCount(totalRows);
        }
        VectorSchemaRoot result = new VectorSchemaRoot(outSchema.getFields(), outVecs);
        result.setRowCount(totalRows);
        return result;
    }

    /**
     * Merges partial aggregation rows from parallel file scans.
     *
     * @param exprs              select expressions
     * @param groupByColumnNames group-by columns
     * @param futures            partial row futures
     * @return merged rows
     * @throws Exception on future resolution or merge failure
     */
    public static List<Object[]> mergePartialRows(
            List<ParquetQueryParser.SelectExpr> exprs,
            List<String> groupByColumnNames,
            List<Future<List<Object[]>>> futures) throws Exception {

        int numGbCols = groupByColumnNames.size();
        if (numGbCols == 0) {
            Object[] merged = null;
            for (Future<List<Object[]>> f : futures) {
                List<Object[]> rows = f.get();
                if (rows.isEmpty()) {
                    continue;
                }
                Object[] row = rows.get(0);
                if (merged == null) {
                    merged = row.clone();
                } else {
                    mergeAggCols(exprs, merged, row, 0);
                }
            }
            return merged != null ? Collections.singletonList(merged) : Collections.emptyList();
        }

        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();
        for (Future<List<Object[]>> f : futures) {
            for (Object[] row : f.get()) {
                List<Object> key = new ArrayList<>(Arrays.asList(row).subList(0, numGbCols));
                Object[] existing = byKey.get(key);
                if (existing == null) {
                    byKey.put(key, row.clone());
                } else {
                    mergeAggCols(exprs, existing, row, numGbCols);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /**
     * Merges aggregation columns from one row into another.
     *
     * @param exprs     select expressions
     * @param into      target accumulators
     * @param from      source row
     * @param numGbCols number of group-by columns
     */
    public static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                continue;
            }
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = VectorUtils.addLongs(into[pos], from[pos]);
                case SUM -> into[pos] = VectorUtils.addNumbers(into[pos], from[pos]);
                case MIN -> into[pos] = VectorUtils.minOf(into[pos], from[pos]);
                case MAX -> into[pos] = VectorUtils.maxOf(into[pos], from[pos]);
                default -> {
                }
            }
        }
    }
}
