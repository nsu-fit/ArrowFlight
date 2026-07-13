package net.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbAdapterTest {

    @Test
    void sqlStringLiteralNormal() {
        assertEquals("'hello'", DuckDbAdapter.sqlStringLiteral("hello"));
    }

    @Test
    void sqlStringLiteralEscapesSingleQuote() {
        assertEquals("'it''s'", DuckDbAdapter.sqlStringLiteral("it's"));
    }

    @Test
    void sqlStringLiteralEmpty() {
        assertEquals("''", DuckDbAdapter.sqlStringLiteral(""));
    }

    @Test
    void quoteIdentifierNormal() {
        assertEquals("\"table_name\"", DuckDbAdapter.quoteIdentifier("table_name"));
    }

    @Test
    void quoteIdentifierEscapesDoubleQuote() {
        assertEquals("\"col\"\"name\"", DuckDbAdapter.quoteIdentifier("col\"name"));
    }

    @Test
    void readParquetFromClauseSingleFile() {
        String result = DuckDbAdapter.readParquetFromClause(List.of("/data/file.parquet"));
        assertEquals("read_parquet(['/data/file.parquet'])", result);
    }

    @Test
    void readParquetFromClauseMultipleFiles() {
        String result = DuckDbAdapter.readParquetFromClause(
                List.of("/data/a.parquet", "/data/b.parquet"));
        assertEquals("read_parquet(['/data/a.parquet', '/data/b.parquet'])", result);
    }

    @Test
    void readParquetFromClauseEmpty() {
        String result = DuckDbAdapter.readParquetFromClause(List.of());
        assertEquals("read_parquet([])", result);
    }
}
