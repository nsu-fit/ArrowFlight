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
                "SELECT DATE '1998-12-01' - INTERVAL ? day",
                "SELECT DATE '1998-12-01' - concat(?,' day')::interval");
        assertRewrite(
                "SELECT DATE '1998-12-01' - INTERVAL '62' day",
                "SELECT DATE '1998-12-01' - concat('62', ' days')::interval");
    }

    private static void assertRewrite(String expected, String input) throws Exception {
        String actual = (String) REWRITE_SQL.invoke(null, input);
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected [" + expected + "] but got [" + actual + "]");
        }
    }
}
