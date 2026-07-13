package net.surpin.data.arrowflight.server.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HandleStateTest {

    @Test
    void forQuery() {
        HandleState state = HandleState.forQuery("SELECT * FROM t");

        assertEquals("SELECT * FROM t", state.query());
        assertNull(state.filePaths());
        assertNull(state.serverUri());
        assertEquals(0L, state.bytes());
    }

    @Test
    void forServerFiles() {
        String[] files = {"f1.parquet", "f2.parquet"};
        HandleState state = HandleState.forServerFiles("SELECT count(*) FROM t", files, "localhost:32010", 1024);

        assertEquals("SELECT count(*) FROM t", state.query());
        assertArrayEquals(files, state.filePaths());
        assertEquals("localhost:32010", state.serverUri());
        assertEquals(1024L, state.bytes());
    }

    @Test
    void equalsSameData() {
        HandleState a = HandleState.forQuery("SELECT 1");
        HandleState b = HandleState.forQuery("SELECT 1");
        assertEquals(a, b);
    }

    @Test
    void notEqualsDifferentQuery() {
        HandleState a = HandleState.forQuery("SELECT 1");
        HandleState b = HandleState.forQuery("SELECT 2");
        assertNotEquals(a, b);
    }
}
