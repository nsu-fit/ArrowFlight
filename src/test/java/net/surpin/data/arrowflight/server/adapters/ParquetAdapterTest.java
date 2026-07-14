package net.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class ParquetAdapterTest {

    @Test
    void hasLocalBlockMatchesResolvedFlightAndDataNodeAliases() {
        assertTrue(ParquetAdapter.hasLocalBlock(Set.of("127.0.0.1"), "localhost"));
        assertFalse(ParquetAdapter.hasLocalBlock(Set.of("127.0.0.2"), "localhost"));
    }

    @Test
    void validateNameValid() {
        assertEquals("test_table", ParquetAdapter.validateName("test_table"));
        assertEquals("a", ParquetAdapter.validateName("a"));
        assertEquals("_leading_underscore", ParquetAdapter.validateName("_leading_underscore"));
        assertEquals("camelCase_name123", ParquetAdapter.validateName("camelCase_name123"));
    }

    @Test
    void validateNameNull() {
        assertNull(ParquetAdapter.validateName(null));
    }

    @Test
    void validateNameStartsWithDigit() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("0abc"));
    }

    @Test
    void validateNameContainsDot() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("table.name"));
    }

    @Test
    void validateNamePathTraversal() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName(".."));
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("../etc"));
    }

    @Test
    void validateNameSqlInjection() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("x'; DROP TABLE"));
    }

    @Test
    void validateNameEmpty() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName(""));
    }

    @Test
    void createLikePredicateMatchAll() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("%");
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
        assertTrue(p.test("very_long_table_name"));
    }

    @Test
    void createLikePredicateNullReturnsMatchAll() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate(null);
        assertTrue(p.test("abc"));
    }

    @Test
    void createLikePredicateExactMatch() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("test_table");
        assertTrue(p.test("test_table"));
        assertFalse(p.test("other_table"));
    }

    @Test
    void createLikePredicateWildcard() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("test_%");
        assertTrue(p.test("test_table"));
        assertTrue(p.test("test_other"));
        assertFalse(p.test("prod_table"));
    }

    @Test
    void createLikePredicateSingleCharWildcard() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("toast"));
    }

    @Test
    void createLikePredicateCaseInsensitive() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("TEST");
        assertTrue(p.test("test"));
        assertTrue(p.test("TEST"));
    }

    private static Predicate<String> invokeCreateLikePredicate(String pattern) throws Exception {
        Method m = ParquetAdapter.class.getDeclaredMethod("createLikePredicate", String.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Predicate<String> result = (Predicate<String>) m.invoke(null, pattern);
        return result;
    }
}
