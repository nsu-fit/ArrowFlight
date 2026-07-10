# ADR 0003: Benchmark suite selection — TPC-H / TPC-DS via BenchBase

Status: Accepted
Date: 2026-07-10

## Decision Record

- Timestamp: 2026-07-10T14:00:00+07:00
- User: s.samokhin
- Question: Which benchmark suite should the project use to measure query
  performance, validate correctness, and track regressions across Parquet query
  engines?
- Decision: Adopt the TPC-H and TPC-DS benchmark suites, executed through the
  BenchBase framework. TPC-H covers decision-support queries with broad column
  access; TPC-DS adds skewed data, nested subqueries, and window functions that
  reflect real analytical workloads.
- Rationale: TPC-H and TPC-DS are the industry-standard decision-support
  benchmarks. BenchBase handles query loading, parameter binding, metric
  collection, and result validation out of the box. The combination covers
  both simple scan-heavy queries (TPC-H) and complex multi-stage queries
  (TPC-DS), matching the project's target analytical workload.

## Context

The project serves Flight SQL queries over Parquet files using three execution
paths: DuckDB (filtered scans, aggregations), Acero (full scans, column
projections), and a Java Parquet-footer fast path (metadata-only aggregates).
Performance and correctness must be verified across all three.

A benchmark suite is needed for:

- tracking query latency and throughput across releases;
- detecting regressions after engine or planner changes;
- validating result correctness against known reference outputs;
- comparing engine performance on real analytical query shapes.

TPC-H and TPC-DS are widely recognised for this purpose. BenchBase provides a
unified driver that loads the schemas, generates data at the desired scale
factor, executes queries, and collects metrics. It also supports running
against JDBC-accessible engines — DuckDB qualifies — and can be extended for
Arrow-native engines like Acero.

## Decision

- Use **TPC-H** as the primary benchmark for scan-heavy and aggregation-heavy
  workloads. Its 22 queries exercise star-join schemas, grouped aggregations,
  and sequential scans.
- Use **TPC-DS** as the secondary benchmark for complex analytical queries. Its
  99 (or 103) queries include nested subqueries, window functions, skewed data
  distributions, and multi-table joins that stress planner and engine behaviour.
- Execute both suites through **BenchBase** to avoid writing custom benchmark
  harnesses, workload generators, or metric collectors.
- Store BenchBase configuration alongside the project, enabling repeatable
  runs with consistent parameters (scale factor, number of streams, iteration
  count, warm-up rounds).
- Validate DuckDB results against TPC-H/TPC-DS reference outputs. For Acero,
  validate against known DuckDB outputs for the same query.
- Add BenchBase-driven benchmarks to the CI pipeline so regressions are
  detected automatically on each commit.

BenchBase is chosen over alternatives because:

- it supports both TPC-H and TPC-DS out of the box;
- it natively supports DuckDB via JDBC, which is one of the project's engines;
- it handles data generation, schema creation, query execution, and metric
  export in a single tool;
- it has active community maintenance and is used by comparable database
  projects.

## Alternatives Considered

### Option A: Custom JMH microbenchmarks

Write individual JMH benchmarks for each query shape and each engine.

- **Pros**: Fine-grained control over what is measured. No external benchmark
  dependencies. Easy to run during development.
- **Cons**: No standard query set — must design queries from scratch. No data
  generator. No result validation against reference outputs. Harder to
  reproduce across environments. Does not measure end-to-end query latency
  with realistic data skew.

### Option B: TPC-H / TPC-DS via custom harness

Write a custom harness that generates TPC data using `dbgen`/`dsdgen` and
executes the queries via the project's own query path.

- **Pros**: Full control over query loading, parameter injection, and metric
  collection. Deeply integrated with the project.
- **Cons**: Significant engineering effort. Must replicate BenchBase's query
  parameter logic, warm-up handling, and metrics aggregation. More surface
  area for bugs in the harness itself.

### Option C: BenchBase + TPC-H / TPC-DS (selected)

Use BenchBase as the off-the-shelf benchmark runner for both TPC suites.

- **Pros**: Proven tool with built-in workload generation, parameter binding,
  and metric collection. DuckDB JDBC support is direct. Extensible for Acero
  via a custom engine adapter if needed. Active community.
- **Cons**: BenchBase is a Java project — adds a dependency and may need
  configuration to match the project's specific deployment. Acero support
  requires a custom adapter (not a JDBC engine).

## Consequences

- **Positive**: Standardised query sets make results comparable with other
  systems and across project versions.
- **Positive**: BenchBase eliminates the need to write and maintain a custom
  benchmark harness, data generator, and metric collector.
- **Positive**: TPC-DS covers query shapes (nested subqueries, window
  functions, skewed joins) that TPC-H does not, revealing engine weaknesses
  that simple scan benchmarks would miss.
- **Positive**: BenchBase's JDBC support gives immediate DuckDB benchmark
  coverage. CI integration is straightforward.
- **Negative**: TPC-DS generates a large schema (24 tables) and complex
  query plans, which may slow down initial setup and iterate cycles.
- **Negative**: BenchBase adds a Java dependency to the benchmark toolchain.
  Version updates must be tracked.
- **Negative**: Acero is not a JDBC engine — a custom BenchBase adapter is
  needed to benchmark it through the same framework. Until the adapter is
  written, Acero benchmarks must rely on separate JMH tests or manual runs.
- **Future**: If BenchBase's adapter model proves limiting, the project
  could migrate to a custom harness while keeping the TPC-H/TPC-DS query
  sets — the investment in query parameter bindings and data generation is
  preserved.
