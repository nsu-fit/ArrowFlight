package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import net.surpin.data.arrowflight.server.db.ParquetManager;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke test for the Spark DataSource V2 "flight" connector.
 */
@Tag("integration")
@Tag("smoke")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkFlightSmokeTest {

    private static FlightServer flightServer;
    private static HazelcastInstance hz;
    private static RootAllocator allocator;
    private static SparkSession spark;
    private static String host;
    private static int port;
    private static String flightTable;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        // Java 21+ UGI workaround — inject SPARK_USER before Spark initialises (same as the other
        // Spark integration tests in this module).
        injectSparkUser(System.getProperty("user.name", "root"));

        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        allocator = new RootAllocator(Long.MAX_VALUE);

        Config hzCfg = new Config();
        hzCfg.setClusterName("spark-smoke-" + UUID.randomUUID());
        hzCfg.getNetworkConfig().setPort(findFreePort());
        hzCfg.getNetworkConfig().setPortAutoIncrement(false);
        hzCfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hzCfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(hzCfg);

        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        ParquetManager parquetManager = new ParquetManager(localFs, dataDir, "localhost");

        host = "localhost";
        port = findFreePort();
        Location location = Location.forGrpcInsecure(host, port);
        HadoopFlightSqlService sqlService =
                new HadoopFlightSqlService(location, parquetManager, allocator, hz);
        flightServer = FlightServer.builder(allocator, location, sqlService).build();
        flightServer.start();

        spark = SparkSession.builder()
                .appName("SparkFlightSmokeTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        flightTable = "test_schema.test_table";
    }

    @AfterAll
    static void stopAll() {
        if (spark != null) spark.stop();
        if (flightServer != null) flightServer.shutdown();
        if (hz != null) hz.shutdown();
        if (allocator != null) allocator.close();
    }

    // ── helpers ────────────────────────────────────────────────────────────

    /** Mirrors the minimal Spark script from the task: format/host/port/user/password/table. */
    private Dataset<Row> flightRead() {
        return spark.read()
                .format("flight")
                .option("host", host)
                .option("port", String.valueOf(port))
                .option("user", "test")
                .option("password", "test")
                .option("table", flightTable)
                .load();
    }

    private static String physicalPlan(Dataset<Row> df) {
        return df.queryExecution().executedPlan().toString();
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    // ── 1. minimal read ───────────────────────────────────────────────────

    @Test
    @Order(1)
    void fullTableRead_worksAndPrintsSchema() {
        Dataset<Row> df = flightRead();

        df.printSchema();
        df.show(10);

        List<String> fields = Arrays.asList(df.schema().fieldNames());
        assertTrue(fields.contains("id"), "schema must contain 'id'");
        assertTrue(fields.contains("string_col"), "schema must contain 'string_col'");
        assertEquals(1000L, df.count(), "sample table has 1000 rows (id 0..999)");
    }

    // ── 2. projection pushdown ────────────────────────────────────────────

    @Test
    @Order(2)
    void projectionPushdown_selectIdAndStringColSendsProjectedSql() {
        Dataset<Row> projected = flightRead().select("id", "string_col");

        String plan = physicalPlan(projected);
        System.out.println("[projection] physical plan:\n" + plan);

        // The connector's Scan#description() == Table.getQueryStatement(), which Spark embeds
        // into the physical plan node -> this *is* the SQL sent to the Flight server.
        assertTrue(plan.toLowerCase().contains("select id,string_col from " + flightTable.toLowerCase())
                        || plan.toLowerCase().contains("select id, string_col from " + flightTable.toLowerCase()),
                "expected a projected 'select id,string_col from " + flightTable + "' statement in the plan, got:\n" + plan);

        assertArrayEquals(new String[]{"id", "string_col"}, projected.schema().fieldNames(),
                "only the requested columns should be part of the read schema");
        assertEquals(1000L, projected.count());

        List<Row> sample = projected.limit(10).collectAsList();
        assertFalse(sample.isEmpty());
        sample.forEach(r -> assertEquals(2, r.size()));
    }

    // ── 3. filter pushdown ────────────────────────────────────────────────

    @Test
    @Order(3)
    void filterPushdown_whereIdGreaterThan100SendsWhereClause() {
        Dataset<Row> filtered = flightRead().where("id > 100").select("id", "string_col");

        String plan = physicalPlan(filtered);
        System.out.println("[filter] physical plan:\n" + plan);

        String lower = plan.toLowerCase();
        assertTrue(lower.contains("where"), "expected a 'where' clause pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("id > 100") || lower.contains("id>100"),
                "expected 'id > 100' pushed into the scan, got:\n" + plan);

        // ids are 0..999 -> id > 100 selects ids 101..999 inclusive = 899 rows
        assertEquals(899L, filtered.count());

        long minId = filtered.selectExpr("min(id)").first().getInt(0);
        assertEquals(101L, minId, "smallest surviving id must be 101, confirming the filter really ran server-side/pruned rows");
    }

    // ── 4. aggregation pushdown ───────────────────────────────────────────

    @Test
    @Order(4)
    void aggregationPushdown_groupByTinyintColSendsGroupByCountSql() {
        // tinyint_col = id % 10 -> 10 groups of 100 rows each; used as the "category" analogue
        // since this schema has no textual low-cardinality column.
        Dataset<Row> grouped = flightRead().groupBy("tinyint_col").count();

        String plan = physicalPlan(grouped);
        System.out.println("[aggregation] physical plan:\n" + plan);

        String lower = plan.toLowerCase();
        assertTrue(lower.contains("group by"), "expected a 'group by' clause pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("count(*)"), "expected 'count(*)' pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("tinyint_col"), "expected grouping column pushed into the scan, got:\n" + plan);

        List<Row> rows = grouped.collectAsList();
        assertEquals(10, rows.size(), "tinyint_col has 10 distinct values (0..9)");
        rows.forEach(r -> assertEquals(100L, r.getLong(1), "each group should have 100 rows"));

        System.out.println("[aggregation] groupBy(tinyint_col).count() results: "
                + rows.stream().map(Row::toString).collect(Collectors.joining(", ")));
    }

    // ── 5. auth requirement / blocker ─────────────────────────────────────

    @Test
    @Order(5)
    void missingCredentials_isBlockedByMandatoryUserPasswordCheck() {
        // Same as flightRead() but without user/password/bearerToken -> documents the blocker:
        // there is no "auth=none" mode, the client refuses to even open a connection.
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                spark.read()
                        .format("flight")
                        .option("host", host)
                        .option("port", String.valueOf(port))
                        .option("table", flightTable)
                        .load()
        );

        System.out.println("[auth] expected failure without credentials: " + ex.getMessage());
        assertTrue(ex.getMessage() != null && ex.getMessage().toLowerCase().contains("mandatory"),
                "FlightSource.probeOptions() should fail fast complaining that host/user/password are mandatory");
    }

    // ── util (copied from the sibling Spark integration tests in this module) ─

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
        } catch (Exception e) {
            System.err.println("Warning: could not inject SPARK_USER (" + e + ")");
        }
    }
}
