package net.surpin.data.arrowflight.server.services;

import org.jooq.*;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParquetQueryParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetQueryParser.class);

    public record JoinTable(String schema, String table, String alias) {}

    public static final class SelectExpr {
        public enum AggFunc { COLUMN, COUNT_STAR, COUNT, SUM, MIN, MAX }

        public final AggFunc func;
        public final String inputColumn;
        public final String outputName;

        SelectExpr(AggFunc func, String inputColumn, String outputName) {
            this.func = func;
            this.inputColumn = inputColumn;
            this.outputName = outputName;
        }
    }

    public final String schema;
    public final String table;
    public final List<String> columns;
    public final String filter;

    public final boolean hasAggregation;
    public final List<String> groupByColumnNames;
    public final List<SelectExpr> selectExprs;

    public final boolean isJoin;
    public final List<JoinTable> joinTables;
    public final String duckDbSql;

    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs) {
        this(schema, table, columns, filter, hasAggregation, groupByColumnNames, selectExprs,
                false, Collections.emptyList(), null);
    }

    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs,
            boolean isJoin, List<JoinTable> joinTables, String duckDbSql) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.filter = filter;
        this.hasAggregation = hasAggregation;
        this.groupByColumnNames = Collections.unmodifiableList(groupByColumnNames);
        this.selectExprs = Collections.unmodifiableList(selectExprs);
        this.isJoin = isJoin;
        this.joinTables = Collections.unmodifiableList(joinTables);
        this.duckDbSql = duckDbSql;
    }

    public static ParquetQueryParser parse(String query) {
        String flattened = flattenSubqueryWrapper(query);
        if (!flattened.equals(query)) {
            LOGGER.info("Flattened subquery wrapper; parsing: {}", flattened);
            return parse(flattened);
        }

        LOGGER.info("Parsing SQL query: {}", query);

        Settings settings = new Settings()
                .withRenderFormatted(true)
                .withParseNameCase(ParseNameCase.AS_IS);
        DSLContext ctx = DSL.using(SQLDialect.DEFAULT, settings);

        try {
            Query parsedQuery = ctx.parser().parseQuery(query);

            if (parsedQuery instanceof Select<?> select) {
                ParquetQueryParser parser = parseSelect(select, flattened);
                LOGGER.info("Parsed query: {}", parser);
                return parser;
            } else {
                throw new IllegalArgumentException("Only SELECT queries are supported: " + query);
            }

        } catch (Exception e) {
            LOGGER.error("Error parsing SQL query", e);
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
    }

    private static ParquetQueryParser parseSelect(Select<?> select, String originalSql) {
        List<? extends Table<?>> fromTables = select.$from();

        DSLContext noQuoteCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.NEVER));

        if (fromTables.size() > 1) {
            throw new IllegalArgumentException(
                    "Can only select from ONE table, but got query: " + select.getSQL());
        }

        boolean isJoin = hasJoinKeywords(originalSql);

        if (isJoin) {
            return parseJoinSelect(select, noQuoteCtx, originalSql);
        }
        return parseSingleTableSelect(select, fromTables, noQuoteCtx);
    }

    private static boolean hasJoinKeywords(String sql) {
        return sql.toLowerCase(java.util.Locale.ROOT).contains(" join ");
    }

    private static ParquetQueryParser parseJoinSelect(Select<?> select,
            DSLContext noQuoteCtx, String originalSql) {

        List<JoinTable> joinTables = extractJoinTables(originalSql);
        if (joinTables.isEmpty()) {
            throw new IllegalArgumentException("Could not extract table references from JOIN: " + originalSql);
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> f = (org.jooq.Field<?>) sfoa;
            columns.add(f.getName());
            String outputName = noQuoteCtx.renderInlined(f).trim();
            exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, outputName, outputName));
        }

        Condition where = select.$where();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        String filter = quotedCtx.renderInlined(where);

        String duckDbSql = rewriteTableRefs(originalSql, joinTables);

        JoinTable primary = joinTables.get(0);
        return new ParquetQueryParser(primary.schema, primary.table, columns, filter,
                false, Collections.emptyList(), exprs, true, joinTables, duckDbSql);
    }

    private static ParquetQueryParser parseSingleTableSelect(Select<?> select,
            List<? extends Table<?>> fromTables, DSLContext noQuoteCtx) {

        if (fromTables.size() != 1) {
            throw new IllegalArgumentException("Can only select from ONE table, but got query: " + select.getSQL());
        }

        String schema = Optional.ofNullable(fromTables.get(0).getSchema()).map(Schema::getName).orElse(null);
        String table = fromTables.get(0).getName();

        List<String> gbCols = new ArrayList<>();
        for (GroupField gf : select.$groupBy()) {
            gbCols.add(noQuoteCtx.render(gf).trim());
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        boolean hasAgg = !gbCols.isEmpty();

        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> f = (org.jooq.Field<?>) sfoa;
            columns.add(f.getName());

            String outputName = noQuoteCtx.renderInlined(f).trim();

            if (f instanceof org.jooq.impl.QOM.Count count) {
                hasAgg = true;
                org.jooq.Field<?> arg = count.$field();
                if (arg == null || arg instanceof org.jooq.Param<?>) {
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT_STAR, null, "count(*)"));
                } else {
                    String col = noQuoteCtx.renderInlined(arg).trim();
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT, col, outputName));
                }
            } else if (f instanceof org.jooq.impl.QOM.Sum sum) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(sum.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.SUM, col, outputName));
            } else if (f instanceof org.jooq.impl.QOM.Min<?> min) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(min.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.MIN, col, outputName));
            } else if (f instanceof org.jooq.impl.QOM.Max<?> max) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(max.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.MAX, col, outputName));
            } else {
                exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, outputName, outputName));
            }
        }

        Condition where = select.$where();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        String filter = quotedCtx.renderInlined(where);

        return new ParquetQueryParser(schema, table, columns, filter, hasAgg, gbCols, exprs);
    }

    // ── JOIN helper methods ───────────────────────────────────────────────

    private static final Pattern JOIN_KEYWORD = Pattern.compile(
            "\\b(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)\\s+",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern TABLE_REF = Pattern.compile(
            "^(\\w+(?:\\.\\w+)?)(?:\\s+(?:AS\\s+)?(\\w+))?",
            Pattern.CASE_INSENSITIVE);

    private static List<JoinTable> extractJoinTables(String sql) {
        List<JoinTable> tables = new ArrayList<>();
        Matcher kw = JOIN_KEYWORD.matcher(sql);
        while (kw.find()) {
            String rest = sql.substring(kw.end()).trim();
            Matcher tr = TABLE_REF.matcher(rest);
            if (tr.find()) {
                String qualified = tr.group(1);
                String alias = tr.group(2);
                int dot = qualified.indexOf('.');
                String schema = dot > 0 ? qualified.substring(0, dot) : null;
                String table = dot > 0 ? qualified.substring(dot + 1) : qualified;
                if (alias == null) {
                    alias = table;
                }
                tables.add(new JoinTable(schema, table, alias));
            }
        }
        return tables;
    }

    private static final Pattern JOIN_ANY = Pattern.compile(
            "\\b(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)\\s+",
            Pattern.CASE_INSENSITIVE);

    private static final String JOIN_ANY_RE = "(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)";

    static String rewriteTableRefs(String sql, List<JoinTable> joinTables) {
        String result = sql;
        for (JoinTable jt : joinTables) {
            String qualified = jt.schema() != null
                    ? jt.schema() + "." + jt.table() : jt.table();
            if (!jt.alias().equals(jt.table())) {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+)" + Pattern.quote(qualified)
                                + "\\s+(?:AS\\s+)?" + Pattern.quote(jt.alias()),
                        "$1" + Matcher.quoteReplacement(jt.alias()));
            } else {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+|,\\s*)" + Pattern.quote(qualified) + "\\b",
                        "$1" + Matcher.quoteReplacement(jt.table()));
            }
        }
        return result;
    }

    // ── public accessors ──────────────────────────────────────────────────

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getFilter() {
        return filter;
    }

    private static String flattenSubqueryWrapper(String query) {
        String q = query.trim();

        java.util.regex.Matcher fromParen =
                java.util.regex.Pattern.compile("(?i)\\bfrom\\s*\\(").matcher(q);
        if (!fromParen.find()) {
            return q;
        }

        int openParen = fromParen.end() - 1;
        String beforeFrom = q.substring(0, fromParen.start());

        int depth = 1;
        int i = openParen + 1;
        while (i < q.length() && depth > 0) {
            char c = q.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            i++;
        }
        if (depth != 0) {
            return q;
        }

        String innerSQL  = q.substring(openParen + 1, i - 1).trim();
        String afterParen = q.substring(i).trim();

        java.util.regex.Matcher aliasM =
                java.util.regex.Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)(.*)$",
                        java.util.regex.Pattern.DOTALL).matcher(afterParen);
        if (!aliasM.matches()) {
            return q;
        }
        String outerRest = aliasM.group(2).trim();

        if (!innerSQL.trim().toLowerCase().startsWith("select")) {
            return q;
        }

        java.util.regex.Matcher innerFrom =
                java.util.regex.Pattern.compile("(?i)\\bfrom\\s+(\\S+)").matcher(innerSQL);
        if (!innerFrom.find()) {
            return q;
        }
        String realTableRef = innerFrom.group(1).replaceAll("\\s*$", "");

        String innerWhere = "";
        java.util.regex.Matcher innerWhereM =
                java.util.regex.Pattern.compile("(?i)\\bwhere\\b(.+)$",
                        java.util.regex.Pattern.DOTALL).matcher(innerSQL);
        if (innerWhereM.find()) {
            innerWhere = innerWhereM.group(1).trim();
        }

        StringBuilder flat = new StringBuilder(beforeFrom);
        flat.append("from ").append(realTableRef);

        String outerWhere = "";
        if (outerRest.toLowerCase().startsWith("where ")) {
            outerWhere = outerRest.substring(5).trim();
        } else if (!outerRest.isEmpty()) {
            return q;
        }

        boolean hasInner = !innerWhere.isEmpty();
        boolean hasOuter = !outerWhere.isEmpty();
        if (hasInner && hasOuter) {
            flat.append(" where (").append(innerWhere).append(") and (").append(outerWhere).append(")");
        } else if (hasInner) {
            flat.append(" where ").append(innerWhere);
        } else if (hasOuter) {
            flat.append(" where ").append(outerWhere);
        }

        return flat.toString();
    }

    @Override
    public String toString() {
        if (isJoin) {
            return "ParquetQueryParser{isJoin=true, joinTables=" + joinTables
                    + ", columns=" + columns + ", filter='" + filter + "'}";
        }
        return "ParquetQueryParser{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                ", filter='" + filter + '\'' +
                ", hasAggregation=" + hasAggregation +
                ", groupByColumnNames=" + groupByColumnNames +
                '}';
    }

}
