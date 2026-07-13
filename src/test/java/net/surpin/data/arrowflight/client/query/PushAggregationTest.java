package net.surpin.data.arrowflight.client.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PushAggregationTest {

    @Test
    void columnExpressionsOnly() {
        PushAggregation agg = new PushAggregation(new String[]{"max(age)", "sum(amount)"});

        assertArrayEquals(new String[]{"max(age)", "sum(amount)"}, agg.getColumnExpressions());
        assertNull(agg.getGroupByColumns());
    }

    @Test
    void withGroupBy() {
        PushAggregation agg = new PushAggregation(
                new String[]{"count(*)"},
                new String[]{"gender", "region"});

        assertArrayEquals(new String[]{"count(*)"}, agg.getColumnExpressions());
        assertArrayEquals(new String[]{"gender", "region"}, agg.getGroupByColumns());
    }

    @Test
    void emptyGroupBy() {
        PushAggregation agg = new PushAggregation(new String[]{"sum(x)"}, new String[0]);

        assertArrayEquals(new String[]{"sum(x)"}, agg.getColumnExpressions());
        assertEquals(0, agg.getGroupByColumns().length);
    }

    @Test
    void emptyColumnExpressions() {
        PushAggregation agg = new PushAggregation(new String[0]);

        assertEquals(0, agg.getColumnExpressions().length);
        assertNull(agg.getGroupByColumns());
    }
}
