package net.surpin.data.arrowflight.client.write;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteStatementTest {

    // ── numberToDecimal256 ──────────────────────────────────────────────────

    @Test
    void numberToDecimal256NonNull() {
        String result = WriteStatement.numberToDecimal256.apply(42, null, null, "decimal(10,2)");
        assertEquals("cast(42 as decimal(10,2))", result);
    }

    @Test
    void numberToDecimal256Null() {
        String result = WriteStatement.numberToDecimal256.apply(null, null, null, "decimal");
        assertEquals("cast(null as decimal)", result);
    }

    // ── primitiveToVarchar ──────────────────────────────────────────────────

    @Test
    void primitiveToVarcharNonNull() {
        String result = WriteStatement.primitiveToVarchar.apply("hello", null, null, "varchar");
        assertEquals("'hello'", result);
    }

    @Test
    void primitiveToVarcharNull() {
        String result = WriteStatement.primitiveToVarchar.apply(null, null, null, "varchar");
        assertEquals("cast(null as varchar)", result);
    }

    @Test
    void primitiveToVarcharEscapesQuote() {
        String result = WriteStatement.primitiveToVarchar.apply("it's", null, null, "varchar");
        assertEquals("'it''s'", result);
    }

    // ── booleanToBit ────────────────────────────────────────────────────────

    @Test
    void booleanToBitTrue() {
        String result = WriteStatement.booleanToBit.apply(true, null, null, "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void booleanToBitNull() {
        String result = WriteStatement.booleanToBit.apply(null, null, null, "bit");
        assertEquals("cast(null as bit)", result);
    }

    // ── numberToBit ─────────────────────────────────────────────────────────

    @Test
    void numberToBitNonZero() {
        String result = WriteStatement.numberToBit.apply(1, null, null, "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void numberToBitZero() {
        String result = WriteStatement.numberToBit.apply(0, null, null, "bit");
        assertEquals("cast(false as bit)", result);
    }

    @Test
    void numberToBitNull() {
        String result = WriteStatement.numberToBit.apply(null, null, null, "bit");
        assertEquals("cast(null as bit)", result);
    }

    // ── stringToBit ─────────────────────────────────────────────────────────

    @Test
    void stringToBitTrue() {
        String result = WriteStatement.stringToBit.apply("true", null, null, "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void stringToBitFalse() {
        String result = WriteStatement.stringToBit.apply("FALSE", null, null, "bit");
        assertEquals("cast(false as bit)", result);
    }

    @Test
    void stringToBitArbitraryText() {
        String result = WriteStatement.stringToBit.apply("xyz", null, null, "bit");
        assertEquals("cast(false as bit)", result);
    }
}
