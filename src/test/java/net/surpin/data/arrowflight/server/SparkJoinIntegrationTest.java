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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying that Spark reads from Arrow Flight and performs
 * a self-join in Spark (not pushed down to the server). Each test reads the
 * table twice as separate DataFrames and joins them on the `id` column.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkJoinIntegrationTest {

    private static FlightServer flightServer;
    private static HazelcastInstance hz;
    private static RootAllocator allocator;
    private static SparkSession spark;
    private static String flightTable;
    private static String host;
    private static int port;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        // Java 21+ UGI workaround — inject SPARK_USER before Spark initialises
        injectSparkUser(System.getProperty("user.name", "root"));

        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        allocator = new RootAllocator(Long.MAX_VALUE);

        // Standalone Hazelcast (no cluster peer needed for single-node join test)
        Config hzCfg = new Config();
        hzCfg.setClusterName("spark-join-" + UUID.randomUUID());
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
                .appName("SparkJoinIntegrationTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        flightTable = "test_schema.test_table";
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) spark.stop();
        if (flightServer != null) flightServer.shutdown();
        if (hz != null) hz.shutdown();
        if (allocator != null) allocator.close();
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private Dataset<Row> flightRead(String query) {
        return spark.read()
                .format("flight")
                .option("host", host)
                .option("port", String.valueOf(port))
                .option("user", "test")
                .option("bearerToken", "test-token")
                .option("table", query)
                .option("column.quote", "\"")
                .load();
    }

    // ── tests ──────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void selfJoinOnIdPreservesRowCount() {
        Dataset<Row> df1 = flightRead("SELECT * FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT * FROM " + flightTable);

        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner");

        long originalCount = df1.count();
        long joinedCount   = joined.count();

        assertTrue(originalCount > 0, "Source table must be non-empty");
        assertEquals(originalCount, joinedCount,
                "Self-join on unique key `id` must preserve row count");
    }

    @Test
    @Order(2)
    void joinedSchemaContainsColumnsFromBothSides() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        // After join on id, select non-ambiguous columns by alias
        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner")
                .select(col("L.id"), col("L.bool_col").as("bool_L"), col("R.bool_col").as("bool_R"));

        List<String> fields = List.of(joined.schema().fieldNames());
        assertTrue(fields.contains("id"),     "Joined schema must contain 'id'");
        assertTrue(fields.contains("bool_L"), "Joined schema must contain 'bool_L'");
        assertTrue(fields.contains("bool_R"), "Joined schema must contain 'bool_R'");
    }

    @Test
    @Order(3)
    void joinIsExecutedBySparkNotPushedDown() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner");

        // Force physical plan resolution without executing the join
        String plan = joined.queryExecution().executedPlan().toString();
        assertTrue(plan.contains("Join"),
                "Physical plan must contain 'Join' (Spark-side); plan was:\n" + plan);
    }

    @Test
    @Order(4)
    void joinedValuesAreConsistentAcrossBothSides() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        // For each matching id, bool_col must be identical on both sides
        long mismatches = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner")
                .filter(col("L.bool_col").notEqual(col("R.bool_col")))
                .count();

        assertEquals(0, mismatches,
                "bool_col must be identical on both sides of the self-join — found mismatches: " + mismatches);
    }

    @Test
    @Order(5)
    void crossTypeJoinIntegerColumnToByteColumnReturnsExpectedCount() {
        // Regression: ClassCastException: Byte cannot be cast to Integer
        //
        // tinyint_col is encoded as INT32 + INT_8 (OriginalType) in the Parquet file.
        // Before the fix, ParquetSchemaConverter mapped it to ArrowType.Int(32) → IntegerType,
        // but Acero returned TinyIntVector whose get() yields byte → auto-boxed Byte.
        // Spark's whole-stage join codegen called getInt() on the Byte value:
        //   GenericInternalRow.getInt() → (Integer) values[ordinal] → ClassCastException.
        //
        // After the fix the schema correctly reports ByteType for tinyint_col, so Spark
        // adds an implicit Cast(ByteType→IntegerType) and the join completes without error.
        //
        // Expected count: id ∈ {0..999} (1 row each); tinyint_col ∈ {0..9} (1000 rows).
        // Only id ∈ {0..9} can match; each of those 10 rows joins all tinyint_col rows with
        // that value → total = 1000 (every t2 row matches exactly one t1 row).
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        long count = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.tinyint_col")),
                        "inner")
                .count();

        assertEquals(1000L, count,
                "Join id(IntegerType) = tinyint_col(ByteType) must return 1000 pairs without ClassCastException");
    }

    // ── cross-type join: INT32 = INT16 ──────────────────────────────────────

    @Test
    @Order(6)
    void crossTypeJoinIntegerToSmallintReturnsExpectedCount() {
        // smallint_col stored as INT32+OriginalType.INT_16 → Arrow Int(16) → Spark ShortType.
        // Same distribution as tinyint_col: pmod(id, 10) → 0-9 cycling (100 rows each).
        // Only id ∈ {0..9} can match; 10 * 100 = 1000 pairs.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.smallint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(1000L, count,
                "Join id(IntegerType) = smallint_col(ShortType) must return 1000 pairs without ClassCastException");
    }

    // ── cross-type join: INT32 = INT64 ──────────────────────────────────────

    @Test
    @Order(7)
    void crossTypeJoinIntegerToBigintReturnsExpectedCount() {
        // bigint_col = pmod(id, 10) * 10 → values 0, 10, 20, ..., 90 (100 rows each).
        // id ∈ {0, 10, 20, ..., 90} each match 100 rows → 10 * 100 = 1000 pairs.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.bigint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(1000L, count,
                "Join id(IntegerType) = bigint_col(LongType) must return 1000 pairs without ClassCastException");
    }

    // ── cross-type join: INT32 = FLOAT ─────────────────────────────────────

    @Test
    @Order(8)
    void crossTypeJoinIntegerToFloatCompletesWithoutClassCastException() {
        // float_col ∈ [0, 1) from rand(47) — comparison with integer id tests
        // implicit Float-to-Int coercion without ClassCastException.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.float_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join id(IntegerType) = float_col(FloatType) must not throw ClassCastException");
    }

    // ── cross-type join: INT32 = DOUBLE ────────────────────────────────────

    @Test
    @Order(9)
    void crossTypeJoinIntegerToDoubleCompletesWithoutClassCastException() {
        // double_col ∈ [0, 1) from rand(48) — comparison with integer id tests
        // implicit Double-to-Int coercion without ClassCastException.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.double_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join id(IntegerType) = double_col(DoubleType) must not throw ClassCastException");
    }

    // ── cross-type join: FLOAT = DOUBLE ────────────────────────────────────

    @Test
    @Order(10)
    void crossTypeJoinFloatToDoubleCompletesWithoutClassCastException() {
        // FLOAT vs DOUBLE join key — tests widening without ClassCastException.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.float_col").equalTo(col("t2.double_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join float_col(FloatType) = double_col(DoubleType) must not throw ClassCastException");
    }

    // ── cross-type join: BOOL = INT8 ───────────────────────────────────────

    @Test
    @Order(11)
    void crossTypeJoinBoolToTinyintReturnsExpectedCount() {
        // bool_col (BOOLEAN → Spark BooleanType) = tinyint_col (INT32+INT_8 → Spark ByteType).
        // bool_col: true for even id (500 rows), false for odd id (500 rows).
        // tinyint_col: pmod(id, 10) → 0-9 (100 rows each).
        // Spark casts BOOL→INT in comparison: false→0, true→1.
        //   true=1  → 500 × 100 = 50000 pairs
        //   false=0 → 500 × 100 = 50000 pairs
        // → 100000 matching rows, must not throw ClassCastException.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.bool_col").equalTo(col("t2.tinyint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(100000L, count,
                "Join bool_col(BooleanType) = tinyint_col(ByteType) must return 100000 pairs without ClassCastException");
    }

    // ── cross-type join: STRING = STRING ───────────────────────────────────

    @Test
    @Order(12)
    void crossTypeJoinStringColumnsReturnsExpectedCount() {
        // date_string_col ("YYYY-MM-DD") vs string_col ("str_XXXXXX") — different formats
        // never overlap, but must not produce ClassCastException on string comparisons.
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.date_string_col").equalTo(col("t2.string_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(0L, count,
                "Join date_string_col(StringType) = string_col(StringType) must return 0 rows without ClassCastException");
    }

    // ── util ────────────────────────────────────────────────────────────────

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

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
