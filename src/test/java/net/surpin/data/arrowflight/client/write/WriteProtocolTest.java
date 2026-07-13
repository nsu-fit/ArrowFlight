package net.surpin.data.arrowflight.client.write;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteProtocolTest {

    @Test
    void literalSql() {
        assertEquals("LITERAL_SQL", WriteProtocol.LITERAL_SQL.name());
    }

    @Test
    void preparedSql() {
        assertEquals("PREPARED_SQL", WriteProtocol.PREPARED_SQL.name());
    }

    @Test
    void values() {
        assertEquals(2, WriteProtocol.values().length);
    }

    @Test
    void valueOf() {
        assertSame(WriteProtocol.LITERAL_SQL, WriteProtocol.valueOf("LITERAL_SQL"));
        assertSame(WriteProtocol.PREPARED_SQL, WriteProtocol.valueOf("PREPARED_SQL"));
    }
}
