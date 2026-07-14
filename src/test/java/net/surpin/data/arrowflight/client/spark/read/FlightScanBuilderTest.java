package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import net.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FlightScanBuilderTest {

    private static final String COLUMN_QUOTE = "\"";

    private static Configuration config() {
        return new Configuration("localhost", 32010, "user", "pass", null);
    }

    private static Table tableWithSchema() {
        Table t = Table.forTable("test_table", COLUMN_QUOTE);
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("score", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("active", DataTypes.BooleanType, true, Metadata.empty())
        });
        t.setSparkSchema(schema);
        return t;
    }

    private static PartitionBehavior noPartitioning() {
        return new PartitionBehavior(null, null, 1, null, null, null);
    }

    // ── pushFilters ───────────────────────────────────────────────────────

    @Test
    void pushFiltersReturnsUnhandledForUnsupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unsupported = new Filter[]{
                new EqualTo("id", 1),
                new AlwaysTrue()
        };
        Filter[] unhandled = builder.pushFilters(unsupported);
        assertEquals(1, unhandled.length);
        assertInstanceOf(AlwaysTrue.class, unhandled[0]);
        assertEquals(1, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersAllSupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] supported = new Filter[]{
                new EqualTo("id", 1),
                new LessThan("score", 100.0f)
        };
        Filter[] unhandled = builder.pushFilters(supported);
        assertEquals(0, unhandled.length);
        assertEquals(2, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersAllUnsupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unsupported = new Filter[]{
                new AlwaysTrue(),
                new AlwaysTrue()
        };
        Filter[] unhandled = builder.pushFilters(unsupported);
        assertEquals(2, unhandled.length);
        assertEquals(0, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersEmptyArray() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unhandled = builder.pushFilters(new Filter[0]);
        assertEquals(0, unhandled.length);
        assertEquals(0, builder.pushedFilters().length);
    }

    // ── pruneColumns ──────────────────────────────────────────────────────

    @Test
    void pruneColumnsSetsRequiredColumns() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        StructType columns = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty())
        });
        builder.pruneColumns(columns);
    }

    // ── safe via pushAggregation with direct Spark API ────────────────────

    @Test
    void pushAggregationCountStar() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.CountStar()
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationCountStarWithGroupBy() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.CountStar()
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[]{
                                new org.apache.spark.sql.connector.expressions.NamedReference() {
                                    @Override
                                    public String[] fieldNames() {
                                        return new String[]{"active"};
                                    }

                                    @Override
                                    public String describe() {
                                        return "active";
                                    }
                                }
                        });
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationCountColumn() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Count(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationDistinctCountRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Count(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        true)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationMin() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Min(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        })
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationMax() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Max(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        })
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationSumDoubleAccepted() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationSumIntegerRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "SUM on Integer column must be rejected");
    }

    @Test
    void pushAggregationSumDistinctRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        },
                                        true)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "DISTINCT SUM must be rejected");
    }

    @Test
    void pushAggregationWithCustomAggregateRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc() {
                                    @Override
                                    public String toString() {
                                        return "custom_agg";
                                    }

                                    @Override
                                    public org.apache.spark.sql.connector.expressions.Expression[]
                                            children() {
                                        return new org.apache.spark.sql.connector.expressions
                                                .Expression[0];
                                    }
                                }
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "Unknown aggregate function must be rejected");
    }
}
