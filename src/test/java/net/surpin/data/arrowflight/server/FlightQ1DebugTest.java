package net.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test for Q1 query behavior through the Flight protocol (server side).
 *
 * <p>Starts an embedded Flight server with a tiny TPC-H lineitem Parquet file
 * and runs Q1 directly via Flight SQL. This shows what the server can do
 * natively (DuckDB handles AVG, complex expressions, decimal SUM) vs.
 * what the Spark FlightSource pushAggregation() actually pushes down.
 *
 * <p>Usage:
 * <pre>
 * python generate_test_lineitem.py
 * mvn test -Dtest=FlightQ1DebugTest -DexcludedGroups="" -Dorg.slf4j.simpleLogger.defaultLogLevel=info
 * </pre>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightQ1DebugTest {

    private static TestFlightServerHelper helper;
    private static FlightSqlClient sqlClient;

    @BeforeAll
    static void startAll() throws Exception {
        String dataDir = Paths.get("src/test/resources/lineitem_tiny").toAbsolutePath().toString();
        System.out.println("=== TestFlightServerHelper dataDir=" + dataDir);
        helper = TestFlightServerHelper.builder()
                .dataDir(dataDir)
                .start();
        sqlClient = helper.sqlClient();
        System.out.println("=== Server started at " + helper.location.getUri());
        System.out.println();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) helper.close();
        System.out.println("=== Server stopped");
    }

    /**
     * Full Q1 as written by TPC-H benchmark.
     * Server receives it as-is via Flight SQL protocol.
     * DuckDB can execute this fully (AVG, complex expressions, decimal SUM).
     */
    @Test
    @Order(1)
    void fullQ1ViaDirectFlightSql() throws Exception {
        String query = "SELECT\n"
                + "  l_returnflag,\n"
                + "  l_linestatus,\n"
                + "  SUM(l_quantity) AS sum_qty,\n"
                + "  SUM(l_extendedprice) AS sum_base_price,\n"
                + "  SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price,\n"
                + "  SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge,\n"
                + "  AVG(l_quantity) AS avg_qty,\n"
                + "  AVG(l_extendedprice) AS avg_price,\n"
                + "  AVG(l_discount) AS avg_disc,\n"
                + "  COUNT(*) AS count_order\n"
                + "FROM tpch.lineitem\n"
                + "WHERE l_shipdate <= '1998-12-01'\n"
                + "GROUP BY l_returnflag, l_linestatus\n"
                + "ORDER BY l_returnflag, l_linestatus";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 1: Full Q1 via direct Flight SQL          ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println();

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);

        System.out.println("FlightInfo:");
        System.out.println("  schema fields: " + info.getSchema().getFields().size());
        System.out.println("  endpoints: " + info.getEndpoints().size());
        for (int i = 0; i < info.getEndpoints().size(); i++) {
            FlightEndpoint ep = info.getEndpoints().get(i);
            System.out.println("    [" + i + "] ticket=" + bytesToHex(ep.getTicket().getBytes())
                    + " locations=" + ep.getLocations());
        }
        System.out.println();

        long totalRows = 0;
        int epIdx = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            long epStart = System.nanoTime();
            System.out.println("--- Endpoint " + (epIdx++) + " ---");
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                int batchIdx = 0;
                int epRows = 0;
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    int rows = root.getRowCount();
                    epRows += rows;
                    totalRows += rows;
                    if (batchIdx < 3) {
                        System.out.println("  batch " + batchIdx + ": " + rows + " rows, "
                                + root.getFieldVectors().size() + " cols");
                    }
                    batchIdx++;
                }
                double epTime = (System.nanoTime() - epStart) / 1e9;
                System.out.println("  " + epRows + " rows across " + batchIdx + " batches, "
                        + String.format("%.3f s", epTime));
            }
        }

        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println();
        System.out.println("RESULT: " + totalRows + " rows in " + String.format("%.3f s", totalTime));
        assertTrue(totalRows > 0, "Q1 must return rows");

        // Print the actual aggregated values
        System.out.println();
        printResults(info, "Q1 full results");
    }

    /**
     * Simplified Q1 with only pushdown-eligible aggregates (no AVG, no complex expressions).
     * This is what Spark FlightSource would push to the server after pushAggregation().
     * Note: SUM of decimal(15,2) is still rejected by safeSumType(), so even this
     * simplified query wouldn't be pushed by the current client.
     */
    @Test
    @Order(2)
    void simplifiedQ1PushdownEligible() throws Exception {
        // MIN/MAX/COUNT(*)/COUNT(col) are the only fully pushdown-eligible functions.
        // SUM(col) only for Float/Double columns.
        // AVG is not supported at all.
        // Complex expressions are rejected.
        String query = "SELECT\n"
                + "  l_returnflag,\n"
                + "  l_linestatus,\n"
                + "  COUNT(*) AS count_order,\n"
                + "  COUNT(l_quantity) AS count_qty,\n"
                + "  MIN(l_quantity) AS min_qty,\n"
                + "  MAX(l_quantity) AS max_qty\n"
                + "FROM tpch.lineitem\n"
                + "WHERE l_shipdate <= '1998-12-01'\n"
                + "GROUP BY l_returnflag, l_linestatus\n"
                + "ORDER BY l_returnflag, l_linestatus";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 2: Simplified Q1 (pushdown-eligible)      ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println("NOTE: Even COUNT/MIN/MAX are only pushdown-eligible\n"
                + "when Spark's pushAggregation() accepts them.\n"
                + "This test shows the server CAN execute them.\n");

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);
        long totalRows = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println("RESULT: " + totalRows + " rows in " + String.format("%.3f s", totalTime));

        printResults(info, "Simplified Q1 results");
    }

    /**
     * Raw SELECT * - shows what Spark falls back to when pushdown is rejected.
     * All rows, all columns streamed from server to client.
     */
    @Test
    @Order(3)
    void selectAllRawData() throws Exception {
        String query = "SELECT l_orderkey, l_quantity, l_extendedprice, l_discount, l_tax,\n"
                + "  l_returnflag, l_linestatus, l_shipdate\n"
                + "FROM tpch.lineitem\n"
                + "WHERE l_shipdate <= '1998-12-01'";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 3: Raw SELECT (what Spark falls back to)  ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println("This is what Spark reads when pushAggregation() returns false.\n"
                + "All rows streamed, Spark aggregates locally.\n");

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);
        long totalRows = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println("RESULT: " + totalRows + " rows"
                + " (" + info.getSchema().getFields().size() + " cols)"
                + " in " + String.format("%.3f s", totalTime));
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private void printResults(FlightInfo info, String label) throws Exception {
        System.out.println("--- " + label + " ---");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    List<String> colNames = root.getSchema().getFields().stream()
                            .map(f -> f.getName()).toList();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int c = 0; c < root.getFieldVectors().size(); c++) {
                            FieldVector vec = root.getVector(c);
                            row.put(colNames.get(c), vec.isNull(r) ? null : vec.getObject(r));
                        }
                        rows.add(row);
                    }
                }
            }
        }
        for (Map<String, Object> row : rows) {
            System.out.println("  " + row);
        }
        System.out.println("  (" + rows.size() + " rows total)");
        System.out.println();
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "empty";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
