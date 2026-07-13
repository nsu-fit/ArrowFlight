package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.model.FileAssignment;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryPlannerTest {

    // ── pickServer ──────────────────────────────────────────────────────────
    //
    // HostUtils.normalize() resolves hostnames via InetAddress.getByName().
    // Without real DNS, arbitrary names resolve to themselves. To test locality
    // matching, file hosts and server URIs must use names that normalize
    // identically — both "127.0.0.1" resolves to "127.0.0.1" regardless of form.

    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.2:32010";
    private static final String H1 = "127.0.0.1";
    private static final String H2 = "127.0.0.2";

    @Test
    void pickServerPrefersLocal() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 100L);
        load.put(S2, 50L);

        // File blocks on H1 — prefer S1 (has locality)
        String server = QueryPlanner.pickServer(Set.of(H1), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerFallsBackToGlobalWhenNoLocality() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 200L);
        load.put(S2, 50L);

        // File blocks on unknown host — fallback to least-loaded
        String server = QueryPlanner.pickServer(Set.of("unknown-host"), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerLeastLoadedAmongMultipleLocals() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 500L);
        load.put(S2, 0L);

        // File blocks on BOTH S1 and S2 — pick lower load
        String server = QueryPlanner.pickServer(Set.of(H1, H2), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerSingleServer() {
        Map<String, Long> load = Map.of(S1, 0L);
        String server = QueryPlanner.pickServer(Set.of("x"), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerThrowsOnEmptyLoad() {
        assertThrows(Exception.class, () ->
                QueryPlanner.pickServer(Set.of(H1), Map.of()));
    }

    // ── groupFilesByServer ──────────────────────────────────────────────────

    @Test
    void groupFilesByServerGroupsAllToSingleServer() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H1)));

        Map<String, Long> load = Map.of(S1, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.size());
        assertEquals(2, result.get(S1).size());
    }

    @Test
    void groupFilesByServerDistributesByLocality() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H2)));

        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 0L);
        load.put(S2, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.get(S1).size());
        assertEquals(1, result.get(S2).size());
    }

    @Test
    void groupFilesByServerEmptyInput() {
        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(
                Map.of(), Map.of(S1, 0L));

        assertTrue(result.isEmpty());
    }

    // ── extractTableFromPath ────────────────────────────────────────────────

    @Test
    void extractTableFromUnixPath() {
        assertEquals(".data.db.table", QueryPlanner.extractTableFromPath("/data/db/table/file.parquet"));
    }

    @Test
    void extractTableFromPathSameSchemaSameTable() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/t1/file.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s1/t1/other.parquet");

        assertEquals(t1, t2);
    }

    @Test
    void extractTableFromPathDifferentTables() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/table_a/f.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s2/table_b/f.parquet");

        assertNotEquals(t1, t2);
    }
}
