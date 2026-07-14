package net.surpin.data.arrowflight.server.services;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionServiceTest {

    // ── minOf / maxOf ───────────────────────────────────────────────────────

    @Test
    void minOfReturnsSmaller() {
        assertEquals(5, ExecutionService.minOf(5, 10));
    }

    @Test
    void minOfReturnsFirstWhenEqual() {
        assertEquals(5, ExecutionService.minOf(5, 5));
    }

    @Test
    void minOfReturnsNonNullWhenOneIsNull() {
        assertEquals(5, ExecutionService.minOf(null, 5));
        assertEquals(5, ExecutionService.minOf(5, null));
    }

    @Test
    void maxOfReturnsLarger() {
        assertEquals(10, ExecutionService.maxOf(5, 10));
    }

    @Test
    void maxOfReturnsFirstWhenEqual() {
        assertEquals(5, ExecutionService.maxOf(5, 5));
    }

    @Test
    void maxOfReturnsNonNullWhenOneIsNull() {
        assertEquals(5, ExecutionService.maxOf(null, 5));
        assertEquals(5, ExecutionService.maxOf(5, null));
    }

    @Test
    void maxOfStringComparison() {
        assertEquals("z", ExecutionService.maxOf("a", "z"));
    }

    // ── addLongs / addDoubles ───────────────────────────────────────────────

    @Test
    void addLongsBothNonNull() {
        assertEquals(7L, ExecutionService.addLongs(3L, 4L));
    }

    @Test
    void addLongsFirstNull() {
        assertEquals(4L, ExecutionService.addLongs(null, 4L));
    }

    @Test
    void addLongsSecondNull() {
        assertEquals(3L, ExecutionService.addLongs(3L, null));
    }

    @Test
    void addLongsBothNull() {
        assertEquals(0L, ExecutionService.addLongs(null, null));
    }

    @Test
    void addDoublesBothNonNull() {
        assertEquals(7.5, ExecutionService.addDoubles(3.0, 4.5));
    }

    @Test
    void addDoublesFirstNull() {
        assertEquals(4.5, ExecutionService.addDoubles(null, 4.5));
    }

    @Test
    void addDoublesSecondNull() {
        assertEquals(3.0, ExecutionService.addDoubles(3.0, null));
    }

    @Test
    void addDoublesBothNull() {
        assertEquals(0.0, ExecutionService.addDoubles(null, null));
    }

    // ── partitionIntoGroups ─────────────────────────────────────────────────

    @Test
    void partitionIntoGroupsEven() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1, 2, 3, 4, 5, 6), 3);
        assertEquals(3, groups.size());
        assertEquals(List.of(1, 4), groups.get(0));
        assertEquals(List.of(2, 5), groups.get(1));
        assertEquals(List.of(3, 6), groups.get(2));
    }

    @Test
    void partitionIntoGroupsSingleGroup() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1, 2, 3), 1);
        assertEquals(1, groups.size());
        assertEquals(3, groups.get(0).size());
    }

    @Test
    void partitionIntoGroupsEmpty() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(), 3);
        assertEquals(3, groups.size());
        assertTrue(groups.get(0).isEmpty());
        assertTrue(groups.get(1).isEmpty());
        assertTrue(groups.get(2).isEmpty());
    }

    @Test
    void partitionIntoGroupsMoreGroupsThanItems() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1), 3);
        assertEquals(3, groups.size());
        assertEquals(1, groups.get(0).size());
        assertTrue(groups.get(1).isEmpty());
        assertTrue(groups.get(2).isEmpty());
    }

    // ── ducksDbPaths ────────────────────────────────────────────────────────

    @Test
    void ducksDbPathsStripsFileScheme() {
        List<String> result = ExecutionService.ducksDbPaths(
                List.of("file:/data/file.parquet"));
        assertEquals(List.of("/data/file.parquet"), result);
    }

    @Test
    void ducksDbPathsStripsFileSchemeTripleSlash() {
        List<String> result = ExecutionService.ducksDbPaths(
                List.of("file:///data/file.parquet"));
        assertEquals(List.of("/data/file.parquet"), result);
    }

    @Test
    void ducksDbPathsPreservesNonFileScheme() {
        List<String> result = ExecutionService.ducksDbPaths(
                List.of("hdfs://namenode/data/file.parquet"));
        assertEquals(List.of("hdfs://namenode/data/file.parquet"), result);
    }

    @Test
    void ducksDbPathsMixed() {
        List<String> result = ExecutionService.ducksDbPaths(
                List.of("file:/a.parquet", "hdfs://ns/b.parquet"));
        assertEquals(List.of("/a.parquet", "hdfs://ns/b.parquet"), result);
    }

    // ── duckDbPath ──────────────────────────────────────────────────────────

    @Test
    void duckDbPathFileScheme() {
        String result = ExecutionService.duckDbPath(
                new Path(URI.create("file:/data/file.parquet")));
        assertEquals("'/data/file.parquet'", result);
    }

    @Test
    void duckDbPathEscapesQuote() {
        String result = ExecutionService.duckDbPath(
                new Path(URI.create("file:/data/quote's.parquet")));
        assertEquals("'/data/quote''s.parquet'", result);
    }

    @Test
    void plainDuckDbPathFileScheme() {
        String result = ExecutionService.plainDuckDbPath(
                new Path(URI.create("file:/data/file.parquet")));
        assertEquals("/data/file.parquet", result);
    }
}
