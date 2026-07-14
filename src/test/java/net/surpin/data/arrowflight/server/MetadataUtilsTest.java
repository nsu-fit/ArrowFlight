package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.adapters.SchemaConverter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.MetadataService;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class MetadataUtilsTest {

    static class TestableParquetAdapter extends ParquetAdapter {
        TestableParquetAdapter() throws Exception {
            super(new AppConfig(
                4096, 4, 65536, 2, 2, 2, "", false, "", "", "", "", 67108864, 30000L,
                "/nonexistent-data-dir", null, 31001, 5701, 120, 3, 500, 3
            ), new RawLocalFileSystem() {{
                initialize(URI.create("file:///"), new Configuration());
            }});
        }
    }

    private MetadataService metadataService;

    private MetadataService metadataService() {
        if (metadataService == null) {
            try {
                metadataService = new MetadataService(new TestableParquetAdapter());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return metadataService;
    }

    // ─── createLikePredicate ───────────────────────────────────────────────

    @Test
    void nullPatternMatchesEverything() {
        Predicate<String> p = MetadataService.createLikePredicate(null);
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
        assertTrue(p.test("  spaces  "));
    }

    @Test
    void exactPatternMatchesOnlyItself() {
        Predicate<String> p = MetadataService.createLikePredicate("test_schema");
        assertTrue(p.test("test_schema"));
        assertFalse(p.test("test_schema2"));
        assertFalse(p.test("TEST_SCHEMA"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnySuffix() {
        Predicate<String> p = MetadataService.createLikePredicate("test%");
        assertTrue(p.test("test"));
        assertTrue(p.test("test_schema"));
        assertTrue(p.test("testing123"));
        assertFalse(p.test("my_test"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnyPrefix() {
        Predicate<String> p = MetadataService.createLikePredicate("%_table");
        assertTrue(p.test("my_table"));
        assertTrue(p.test("some_other_table"));
        assertFalse(p.test("table"));
    }

    @Test
    void percentOnlyMatchesAll() {
        Predicate<String> p = MetadataService.createLikePredicate("%");
        assertTrue(p.test("any_string"));
        assertTrue(p.test(""));
    }

    @Test
    void underscoreMatchesSingleCharacter() {
        Predicate<String> p = MetadataService.createLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("ts"));
        assertFalse(p.test("teest"));
    }

    @Test
    void mixedWildcards() {
        Predicate<String> p = MetadataService.createLikePredicate("t%_col");
        assertTrue(p.test("tinyint_col"));
        assertTrue(p.test("t_col"));
        assertFalse(p.test("int_col"));
    }

    @Test
    void regexSpecialCharsInPatternAreEscaped() {
        Predicate<String> p = MetadataService.createLikePredicate("a.b");
        assertTrue(p.test("a.b"));
        assertFalse(p.test("axb"));
        assertFalse(p.test("a.bc"));
    }

    // ─── getCatalogsRoot ──────────────────────────────────────────────────

    @Test
    void getCatalogsRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getCatalogsRoot(allocator)) {
            assertEquals(1, root.getRowCount());
        }
    }

    // ─── getSchemasRoot ───────────────────────────────────────────────────

    @Test
    void getSchemasRootReturnsOneRowPerSchema() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getSchemasRoot(
                     List.of("schema_a", "schema_b"), allocator)) {
            assertEquals(2, root.getRowCount());
        }
    }

    @Test
    void getSchemasRootHandlesEmptyCollection() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getSchemasRoot(List.of(), allocator)) {
            assertEquals(0, root.getRowCount());
        }
    }

    // ─── getTableTypesRoot ────────────────────────────────────────────────

    @Test
    void getTableTypesRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getTableTypesRoot(allocator)) {
            assertEquals(1, root.getRowCount());
        }
    }

    // ─── buildTypeInfo ────────────────────────────────────────────────────

    @Test
    void buildTypeInfoContainsExpectedTypes() {
        var types = SchemaConverter.buildTypeInfo();
        assertFalse(types.isEmpty());

        List<String> typeNames = types.stream()
                .map(SchemaConverter.TypeInfo::typeName)
                .toList();
        assertTrue(typeNames.contains("BIGINT"));
        assertTrue(typeNames.contains("VARCHAR"));
        assertTrue(typeNames.contains("BOOLEAN"));
        assertTrue(typeNames.contains("TIMESTAMP"));
        assertTrue(typeNames.contains("DATE"));
        assertTrue(typeNames.contains("TINYINT"));
        assertTrue(typeNames.contains("SMALLINT"));
        assertTrue(typeNames.contains("INTEGER"));
        assertTrue(typeNames.contains("DOUBLE"));
        assertTrue(typeNames.contains("REAL"));
    }

    @Test
    void buildTypeInfoIsSortedByDataType() {
        var types = SchemaConverter.buildTypeInfo();
        for (int i = 1; i < types.size(); i++) {
            int prev = types.get(i - 1).dataType();
            int curr = types.get(i).dataType();
            assertTrue(prev <= curr,
                    "Types should be sorted by dataType: " + types.get(i - 1).typeName()
                            + "(" + prev + ") > " + types.get(i).typeName() + "(" + curr + ")");
        }
    }
}
