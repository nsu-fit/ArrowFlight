package net.surpin.benchbase;

import java.lang.reflect.Method;

public final class HiveExecuteDriverSelfTest {
    private static final Method REWRITE_SQL;

    static {
        try {
            Class<?> rewriteClass = Class.forName(
                    "net.surpin.benchbase.HiveExecuteDriver$SqlRewrite");
            REWRITE_SQL = rewriteClass.getDeclaredMethod("rewriteSql", String.class);
            REWRITE_SQL.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private HiveExecuteDriverSelfTest() {
    }

    public static void main(String[] args) throws Exception {
        assertRewrite(
                "SELECT 'engine' AS name, 'Spark SQL' AS setting, 'BenchBase JDBC target' AS description",
                "SHOW ALL");
        assertRewrite(
                "SELECT CAST(NULL AS STRING) AS unsupported_metric WHERE FALSE",
                "SELECT * FROM pg_stat_archiver");
        assertRewrite(
                "SELECT CAST(NULL AS STRING) AS unsupported_metric WHERE FALSE",
                "select * from pg_catalog.pg_statio_user_indexes;");
        assertRewrite(
                "SELECT DATE '1998-12-01' - INTERVAL ? day",
                "SELECT DATE '1998-12-01' - concat(?,' day')::interval");
        assertRewrite(
                "SELECT DATE '1998-12-01' - INTERVAL '62' day",
                "SELECT DATE '1998-12-01' - concat('62', ' days')::interval");
        assertRewrite(
                "SELECT o_orderkey FROM orders WHERE EXISTS (SELECT 1 FROM lineitem "
                        + "WHERE l_orderkey = o_orderkey)",
                "SELECT o_orderkey FROM orders WHERE EXISTS (SELECT * FROM lineitem "
                        + "WHERE l_orderkey = o_orderkey)");
        assertRewrite(
                "SELECT 0.2 * AVG(CAST(l_quantity AS DOUBLE)) FROM lineitem",
                "SELECT 0.2 * AVG(l_quantity) FROM lineitem");
        assertRewrite(
                "SELECT AVG(l_quantity) FROM lineitem",
                "SELECT AVG(l_quantity) FROM lineitem");
        assertRewrite(
                "CREATE OR REPLACE GLOBAL TEMPORARY VIEW revenue0 "
                        + "(supplier_no, total_revenue) AS SELECT 1, 2",
                "CREATE view revenue0 (supplier_no, total_revenue) AS SELECT 1, 2");
        assertRewrite(
                "SELECT * FROM global_temp.revenue0 WHERE total_revenue = "
                        + "(SELECT MAX(total_revenue) FROM global_temp.revenue0)",
                "SELECT * FROM revenue0 WHERE total_revenue = "
                        + "(SELECT MAX(total_revenue) FROM revenue0)");
        assertRewrite(
                "DROP VIEW IF EXISTS global_temp.revenue0",
                "DROP VIEW revenue0");
    }

    private static void assertRewrite(String expected, String input) throws Exception {
        String actual = (String) REWRITE_SQL.invoke(null, input);
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected [" + expected + "] but got [" + actual + "]");
        }
    }
}
