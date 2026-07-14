package net.surpin.data.arrowflight.client.model;

import org.apache.spark.sql.sources.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private static Table newTable() {
        return Table.forTable("test_table", "\"");
    }

    // ── toWhereClause ───────────────────────────────────────────────────────

    @Test
    void equalToNumber() {
        EqualTo f = new EqualTo("id", 42);
        String clause = newTable().toWhereClause(f);
        assertEquals("\"id\" = 42", clause);
    }

    @Test
    void equalToString() {
        EqualTo f = new EqualTo("name", "hello");
        String clause = newTable().toWhereClause(f);
        assertEquals("\"name\" = 'hello'", clause);
    }

    @Test
    void equalNullSafe() {
        EqualNullSafe f = new EqualNullSafe("col", "val");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("is not null"));
        assertTrue(clause.contains("="));
    }

    @Test
    void lessThan() {
        LessThan f = new LessThan("score", 100);
        assertEquals("\"score\" < 100", newTable().toWhereClause(f));
    }

    @Test
    void lessThanOrEqual() {
        LessThanOrEqual f = new LessThanOrEqual("score", 100);
        assertEquals("\"score\" <= 100", newTable().toWhereClause(f));
    }

    @Test
    void greaterThan() {
        GreaterThan f = new GreaterThan("age", 18);
        assertEquals("\"age\" > 18", newTable().toWhereClause(f));
    }

    @Test
    void greaterThanOrEqual() {
        GreaterThanOrEqual f = new GreaterThanOrEqual("age", 18);
        assertEquals("\"age\" >= 18", newTable().toWhereClause(f));
    }

    @Test
    void lessThanString() {
        LessThan f = new LessThan("name", "john");
        assertEquals("\"name\" < 'john'", newTable().toWhereClause(f));
    }

    @Test
    void andTwoFilters() {
        And f = new And(new EqualTo("id", 1), new EqualTo("status", "active"));
        String clause = newTable().toWhereClause(f);
        assertEquals("(\"id\" = 1 and \"status\" = 'active')", clause);
    }

    @Test
    void orTwoFilters() {
        Or f = new Or(new EqualTo("x", 1), new EqualTo("x", 2));
        String clause = newTable().toWhereClause(f);
        assertEquals("(\"x\" = 1 or \"x\" = 2)", clause);
    }

    @Test
    void nestedAndOr() {
        And inner = new And(new EqualTo("a", 1), new EqualTo("b", 2));
        Or f = new Or(inner, new EqualTo("c", 3));
        String clause = newTable().toWhereClause(f);
        assertEquals("((\"a\" = 1 and \"b\" = 2) or \"c\" = 3)", clause);
    }

    @Test
    void isNullFilter() {
        IsNull f = new IsNull("deleted_at");
        assertEquals("\"deleted_at\" is null", newTable().toWhereClause(f));
    }

    @Test
    void isNotNullFilter() {
        IsNotNull f = new IsNotNull("email");
        assertEquals("\"email\" is not null", newTable().toWhereClause(f));
    }

    @Test
    void stringStartsWith() {
        StringStartsWith f = new StringStartsWith("name", "foo");
        assertEquals("\"name\" like 'foo%' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void stringContains() {
        StringContains f = new StringContains("desc", "bar");
        assertEquals("\"desc\" like '%bar%' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void stringEndsWith() {
        StringEndsWith f = new StringEndsWith("file", ".parquet");
        assertEquals("\"file\" like '%.parquet' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void notFilter() {
        Not f = new Not(new EqualTo("active", 1));
        String clause = newTable().toWhereClause(f);
        assertEquals("not (\"active\" = 1)", clause);
    }

    @Test
    void inFilterNumbers() {
        In f = new In("id", new Object[] { 1, 2, 3 });
        assertEquals("\"id\" in (1,2,3)", newTable().toWhereClause(f));
    }

    @Test
    void inFilterStrings() {
        In f = new In("code", new Object[] { "A", "B" });
        assertEquals("\"code\" in ('A','B')", newTable().toWhereClause(f));
    }

    @Test
    void inFilterMixed() {
        In f = new In("col", new Object[] { 1, "two" });
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.startsWith("\"col\" in ("));
    }

    // ── forTable ────────────────────────────────────────────────────────────

    @Test
    void forTableSimpleName() {
        Table t = Table.forTable("my_table", "`");
        assertEquals("my_table", t.getName());
        assertEquals("`", t.getColumnQuote());
    }

    @Test
    void forTableQueryWrapsInSubquery() {
        Table t = Table.forTable("SELECT * FROM remote_table", "\"");
        assertEquals("(SELECT * FROM remote_table) t", t.getName());
    }
}
