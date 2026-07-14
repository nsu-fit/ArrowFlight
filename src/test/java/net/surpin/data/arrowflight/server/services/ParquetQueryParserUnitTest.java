package net.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class ParquetQueryParserUnitTest {

    // ── flattenSubqueryWrapper (private) ──────────────────────────────────

    @Test
    void flattenSubqueryWrapperUnchangedWhenNoSubquery() throws Exception {
        String result = invokeFlatten("SELECT * FROM s.t WHERE id > 0");
        assertEquals("SELECT * FROM s.t WHERE id > 0", result.trim());
    }

    @Test
    void flattenSubqueryWrapperInlinesSimpleSubquery() throws Exception {
        String result = invokeFlatten(
                "SELECT * FROM (SELECT id FROM s.t WHERE id > 0) sub WHERE id < 100");
        assertTrue(result.contains("from s.t"),
                "Original table ref should be preserved, got: " + result);
        assertFalse(result.contains("sub"),
                "Alias should be removed, got: " + result);
    }

    @Test
    void flattenSubqueryWrapperNoFromParenReturnsOriginal() throws Exception {
        String result = invokeFlatten("SELECT * FROM s.t");
        assertEquals("SELECT * FROM s.t", result.trim());
    }

    private static String invokeFlatten(String query) throws Exception {
        Method m = ParquetQueryParser.class.getDeclaredMethod(
                "flattenSubqueryWrapper", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, query);
    }
}
