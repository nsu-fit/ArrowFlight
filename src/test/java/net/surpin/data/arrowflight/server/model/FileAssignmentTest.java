package net.surpin.data.arrowflight.server.model;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FileAssignmentTest {

    @Test
    void constructorAndAccessors() {
        FileAssignment fa = new FileAssignment(1024, Set.of("host1", "host2"));

        assertEquals(1024, fa.size());
        assertEquals(2, fa.hosts().size());
        assertTrue(fa.hosts().contains("host1"));
        assertTrue(fa.hosts().contains("host2"));
    }

    @Test
    void emptyHosts() {
        FileAssignment fa = new FileAssignment(0, Set.of());

        assertEquals(0, fa.size());
        assertTrue(fa.hosts().isEmpty());
    }

    @Test
    void equalsSameValues() {
        FileAssignment a = new FileAssignment(100, Set.of("h1"));
        FileAssignment b = new FileAssignment(100, Set.of("h1"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void notEqualsDifferentSize() {
        FileAssignment a = new FileAssignment(100, Set.of("h1"));
        FileAssignment b = new FileAssignment(200, Set.of("h1"));

        assertNotEquals(a, b);
    }
}
