package net.surpin.data.arrowflight.client.write;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WriteBehaviorTest {

    @Test
    void constructorAndGetters() {
        String[] mergeBy = {"id", "region"};
        Map<String, String> typeMapping = Map.of("int", "integer");
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 1024, mergeBy, typeMapping);

        assertEquals(WriteProtocol.LITERAL_SQL, wb.getProtocol());
        assertEquals(1024, wb.getBatchSize());
        assertArrayEquals(mergeBy, wb.getMergeByColumns());
        assertEquals(typeMapping, wb.getTypeMapping());
        assertFalse(wb.isTruncate());
    }

    @Test
    void truncateSetsFlag() {
        WriteBehavior wb = new WriteBehavior(WriteProtocol.PREPARED_SQL, 512, new String[0], Map.of());

        wb.truncate();

        assertTrue(wb.isTruncate());
    }

    @Test
    void truncateWithNoMergeBySucceeds() {
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 256, null, Map.of());

        assertDoesNotThrow(wb::truncate);
        assertTrue(wb.isTruncate());
    }

    @Test
    void truncateWithEmptyMergeBySucceeds() {
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 256, new String[0], Map.of());

        assertDoesNotThrow(wb::truncate);
        assertTrue(wb.isTruncate());
    }

    @Test
    void truncateWithMergeByThrows() {
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 100, new String[] {"id"}, Map.of());

        assertThrows(RuntimeException.class, wb::truncate,
                "truncate with merge-by columns must throw");
        assertFalse(wb.isTruncate());
    }

    @Test
    void getMergeByColumnsReturnsEmptyOnTruncate() {
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 100, new String[] {"col1", "col2"}, Map.of());

        // With merge-by columns, truncate must throw (business rule).
        assertThrows(RuntimeException.class, wb::truncate);
        // Flag must remain false, mergeBy still accessible.
        assertFalse(wb.isTruncate());
        assertEquals(2, wb.getMergeByColumns().length);

        // Without merge-by columns, truncate succeeds and getMergeByColumns returns empty.
        WriteBehavior wb2 = new WriteBehavior(WriteProtocol.LITERAL_SQL, 100, new String[0], Map.of());
        wb2.truncate();
        assertEquals(0, wb2.getMergeByColumns().length);
    }

    @Test
    void getMergeByColumnsReturnsOriginalsWhenNotTruncate() {
        String[] mergeBy = {"a", "b"};
        WriteBehavior wb = new WriteBehavior(WriteProtocol.LITERAL_SQL, 100, mergeBy, Map.of());

        assertArrayEquals(mergeBy, wb.getMergeByColumns());
    }
}
