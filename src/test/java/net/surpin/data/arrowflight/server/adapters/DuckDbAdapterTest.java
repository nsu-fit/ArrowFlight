package net.surpin.data.arrowflight.server.adapters;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    void ignoresHdfsOptionsWhenExtensionIsNotConfigured() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                4096, 1, 131072, 1, 1, 1,
                null, false, null, null,
                "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
                1048576, 60000L, "/data/parquet", null, 32010, 5701, 60,
                3, 1000, 30000);

        try {
            DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool);
            try (Connection connection = adapter.connection()) {
                assertFalse(connection.isClosed());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    // ── buildDuckSql / buildDuckSqlWithFilter ─────────────────────────────

    @Test
    void buildDuckSqlWithColumnSelect() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildDuckSql(pq, "my_from");
        assertTrue(sql.contains("SELECT \"id\", \"name\" FROM my_from"));
        assertTrue(sql.contains("\"id\" > 0"));
    }

    @Test
    void buildDuckSqlWithCountStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(*)"));
        assertTrue(sql.contains("FROM t0"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithGroupBy() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT bool_col, count(*) FROM s.t GROUP BY bool_col");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("GROUP BY \"bool_col\""), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilterAppliedSkipsWhere() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSqlWithFilter(pq, "t0", true);
        assertFalse(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithSumMinMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount), min(id), max(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    // ── buildGroupedDuckSql ───────────────────────────────────────────────

    @Test
    void buildGroupedDuckSqlSingleFile() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 1, false);
        assertTrue(sql.contains("FROM \"t0\""), "Got: " + sql);
        assertFalse(sql.contains("UNION ALL"), "Got: " + sql);
    }

    @Test
    void buildGroupedDuckSqlMultipleFiles() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 3, false);
        assertTrue(sql.contains("UNION ALL"), "Got: " + sql);
        assertTrue(sql.contains("\"t0\""), "Got: " + sql);
        assertTrue(sql.contains("\"t1\""), "Got: " + sql);
        assertTrue(sql.contains("\"t2\""), "Got: " + sql);
        assertTrue(sql.startsWith("SELECT count(*) FROM ("), "Got: " + sql);
    }

    // ── buildSelectSql ────────────────────────────────────────────────────

    @Test
    void buildSelectSqlSelectStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq,
                DuckDbAdapter.readParquetFromClause(List.of("/data/f.parquet")));
        assertTrue(sql.contains("SELECT *"), "Got: " + sql);
        assertTrue(sql.contains("read_parquet"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithColumns() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("\"id\", \"name\""), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t WHERE id = 1");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithAggregation() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(*)"), "Got: " + sql);
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithSum() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMin() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT min(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT max(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    // ── appendSelectExpr ──────────────────────────────────────────────────

    @Test
    void appendSelectExprCountStar() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(*) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().startsWith("count(*)"),
                "Got: " + sb);
    }

    @Test
    void appendSelectExprCountColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(id) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("count(\"id\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprSum() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT sum(amount) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("sum(\"amount\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMin() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT min(price) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("min(\"price\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMax() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT max(score) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("max(\"score\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT col_name FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("\"col_name\""), "Got: " + sb);
    }

    private static ParquetQueryParser.SelectExpr parseSingleExpr(String sql) {
        return ParquetQueryParser.parse(sql).selectExprs.get(0);
    }
}
