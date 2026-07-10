package net.surpin.data.arrowflight.server;

import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;

/**
 * Standalone benchmark comparing Parquet direct reads vs Arrow Flight for the same queries.
 * Run via:
 *   java -cp <fat-jar> server.net.surpin.data.arrowflight.SparkPerfTest [data-dir]
 *
 * The benchmark expects parquet data at:
 *   <data-dir>/<schema>/<table>/<files>.parquet
 *
 * By default uses src/test/resources/test_db from the working directory.
 */
public class SparkPerfTest {

    private static final int WARMUP_RUNS = 1;
    private static final int BENCH_RUNS = 3;

    record BenchResult(String label, String query, long parquetMs, long flightMs, long rowCount) {
        @Override
        public String toString() {
            return String.format("%-50s | %8d ms | %8d ms | %+8d ms | %6d rows",
                    label, parquetMs, flightMs, flightMs - parquetMs, rowCount);
        }
    }

    public static void main(String[] args) throws Exception {
        String dataDir = args.length > 0
                ? args[0]
                : Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        String schema = args.length > 1 ? args[1] : "test_schema";
        String table  = args.length > 2 ? args[2] : "test_table";

        System.out.println("Data directory : " + dataDir);
        System.out.println("Table          : " + schema + "." + table);
        System.out.println();

        // Java 21+ workaround
        injectSparkUser(System.getProperty("user.name", "root"));

        // ── start embedded Arrow Flight server via helper ────────────────
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dataDir)
                .allocator(allocator)
                .start();
        int port = helper.location.getUri().getPort();
        System.out.println("Arrow Flight server started on port " + port);

        // ── start Spark ─────────────────────────────────────────────────
        SparkSession spark = SparkSession.builder()
                .appName("ArrowFlightPerfTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        String parquetPath = dataDir + "/" + schema + "/" + table;
        String flightHost = "localhost";
        String flightTable = schema + "." + table;

        // ── define benchmarks ────────────────────────────────────────────
        List<BenchScenario> scenarios = List.of(
            new BenchScenario(
                "Full scan",
                spark.read().parquet(parquetPath),
                flightRead(spark, flightHost, port, "SELECT * FROM " + flightTable)
            ),
            new BenchScenario(
                "Filtered (tinyint_col = 0)",
                spark.read().parquet(parquetPath).where("tinyint_col = 0"),
                flightRead(spark, flightHost, port, "SELECT * FROM " + flightTable + " WHERE tinyint_col = 0")
            ),
            new BenchScenario(
                "Column projection (id, bool_col, double_col)",
                spark.read().parquet(parquetPath).select("id", "bool_col", "double_col"),
                flightRead(spark, flightHost, port, "SELECT id, bool_col, double_col FROM " + flightTable)
            ),
            new BenchScenario(
                "Aggregation (COUNT, AVG double_col)",
                spark.read().parquet(parquetPath).selectExpr("count(*)", "avg(double_col)"),
                flightRead(spark, flightHost, port, "SELECT * FROM " + flightTable)
                        .selectExpr("count(*)", "avg(double_col)")
            ),
            new BenchScenario(
                "Filter + project (id, string_col where int_col < 5)",
                spark.read().parquet(parquetPath)
                        .where("int_col < 5").select("id", "string_col"),
                flightRead(spark, flightHost, port,
                        "SELECT * FROM " + flightTable + " WHERE int_col < 5")
                        .select("id", "string_col")
            )
        );

        // ── run warmup ───────────────────────────────────────────────────
        System.out.println("Warming up (" + WARMUP_RUNS + " run(s))...");
        for (BenchScenario s : scenarios) {
            for (int i = 0; i < WARMUP_RUNS; i++) {
                s.parquet.count();
                s.flight.count();
            }
        }

        // ── run benchmark ────────────────────────────────────────────────
        System.out.println("Benchmarking (" + BENCH_RUNS + " run(s) each)...\n");
        List<BenchResult> results = new ArrayList<>();
        for (BenchScenario s : scenarios) {
            long[] parquetTimes = new long[BENCH_RUNS];
            long[] flightTimes  = new long[BENCH_RUNS];
            long rowCount = 0;

            for (int i = 0; i < BENCH_RUNS; i++) {
                long t0 = System.currentTimeMillis();
                rowCount = s.parquet.count();
                parquetTimes[i] = System.currentTimeMillis() - t0;

                t0 = System.currentTimeMillis();
                s.flight.count();
                flightTimes[i] = System.currentTimeMillis() - t0;
            }

            results.add(new BenchResult(
                    s.label,
                    s.label,
                    median(parquetTimes),
                    median(flightTimes),
                    rowCount
            ));
        }

        // ── print results ────────────────────────────────────────────────
        System.out.println("=".repeat(100));
        System.out.printf("%-50s | %10s | %10s | %10s | %8s%n",
                "Scenario", "Parquet ms", "Flight ms", "Delta ms", "Rows");
        System.out.println("-".repeat(100));
        for (BenchResult r : results) {
            System.out.println(r);
        }
        System.out.println("=".repeat(100));
        System.out.println("Delta: positive = Flight slower, negative = Flight faster");

        // ── cleanup ──────────────────────────────────────────────────────
        spark.stop();
        helper.close();
    }

    private static Dataset<Row> flightRead(SparkSession spark, String host, int port, String query) {
        return spark.read()
                .format("flight")
                .option("host", host)
                .option("port", String.valueOf(port))
                .option("table", query)
                .load();
    }

    private static long median(long[] times) {
        java.util.Arrays.sort(times);
        return times[times.length / 2];
    }

    /**
     * Injects SPARK_USER into the live process environment via reflection.
     */
    @SuppressWarnings("unchecked")
    private static void injectSparkUser(String user) {
        try {
            Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
            Field theUnmod = pe.getDeclaredField("theUnmodifiableEnvironment");
            theUnmod.setAccessible(true);
            Map<String, String> unmodEnv = (Map<String, String>) theUnmod.get(null);
            Field m = unmodEnv.getClass().getDeclaredField("m");
            m.setAccessible(true);
            Map<String, String> strEnv = (Map<String, String>) m.get(unmodEnv);
            strEnv.put("SPARK_USER", user);
            System.out.println("Injected SPARK_USER=" + user + " for Java 21+ UGI workaround");
        } catch (Exception e) {
            System.err.println("Warning: could not inject SPARK_USER (" + e + "). " +
                    "Set SPARK_USER=" + user + " in the environment before launching.");
        }
    }

    record BenchScenario(String label, Dataset<Row> parquet, Dataset<Row> flight) {}
}
