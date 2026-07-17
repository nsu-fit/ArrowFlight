# SQL Query Execution Flow

This document describes how an SQL query flows through the Arrow Flight server: from receiving the query to reading Parquet files, executing operations, and sending the result back to the client.

## Main Components

`HadoopArrowFlightServer` starts the server and configures Hadoop FileSystem, Hazelcast, and Arrow Flight.

`FlightSqlProducer` implements the Flight SQL layer. It receives queries, creates `FlightInfo`, builds `Ticket` objects, creates endpoints, and restores query state during `DoGet`.

`QueryPlanner` handles file distribution across nodes: it receives parsed queries and file inventories, runs `pickServer` for locality-aware assignment, and builds per-node endpoints.

`ExecutionService`, `MetadataService`, and `ParquetAdapter` handle Parquet-related work. `ParquetAdapter` owns file discovery and locality detection. `MetadataService` handles schema construction and Java footer fast paths. `ExecutionService` orchestrates Arrow Dataset / Acero scans and DuckDB execution.

`ParquetQueryParser` parses SQL and extracts schema, table, selected columns, filters, aggregations, and group by columns.

`AppConfig` / `ConfigAdapter` loads runtime tuning values from `arrowflight.properties`, with JVM system properties and selected environment variables as overrides.

## High-Level Flow

A query goes through two main phases.

The first phase is planning. The client calls `GetFlightInfo`; the server determines the result schema, distributes files across Flight nodes, and returns endpoints with tickets.

The second phase is reading. The client calls `DoGet` using the tickets, and the corresponding Flight nodes read their assigned files and stream the result in Arrow format.

Data is not read during `GetFlightInfo`. Actual reading starts only during `DoGet`.

## Receiving the SQL Query

The SQL query enters the server through `FlightSqlProducer.getFlightInfoStatement`.

At this stage, the server:

1. Creates a unique handle for the query.
2. Builds the Arrow result schema.
3. Stores the SQL query in Hazelcast `statementCache`.
4. Starts endpoint construction.

The handle connects the future ticket with the query state.

## Hazelcast Cache and Ticket

`statementCache` is a distributed Hazelcast `IMap` available to all Flight nodes in the cluster.

A ticket does not contain the files themselves and does not contain the full execution plan. A ticket contains a handle. Using this handle, the server can look up the SQL query and the file list assigned to a specific endpoint in `statementCache`.

This is necessary because `GetFlightInfo` and `DoGet` are separate network calls. The server must preserve query state between them.

Hazelcast is important in cluster mode: a ticket may be created by one node while reading may be performed by another node. The shared cache lets all nodes restore state by handle.

Entries in `statementCache` have a limited lifetime. The current implementation uses a TTL of 10 minutes.

## Building the Result Schema

Before returning `FlightInfo`, the server must know the result schema. For that, `FlightSqlProducer` calls `MetadataService.getQuerySchema`.

The result schema is built before reading data. For regular queries, it is derived from the Parquet table schema and the selected columns. For aggregations, it is derived from aggregation expressions and group by columns.

This schema is returned to the client as part of `FlightInfo`.

## File Distribution

File distribution starts in `FlightSqlProducer.determineEndpoints`, which delegates to `QueryPlanner.determineEndpoints`.

First, the server reads the SQL query from `statementCache`. Then `ParquetAdapter.locationsForQuery` determines which Parquet files belong to the query table and which Hadoop hosts store their blocks.

Next, the server retrieves all registered Flight servers from `serverRegistry`. Each Flight node registers itself in `serverRegistry` during startup.

For each file, the server calls `QueryPlanner.pickServer`. This method chooses the Flight server that will read the file.

If a Flight server runs on a host that stores the file blocks, that node is preferred. If no useful locality is available, the file is assigned using round-robin distribution across all Flight servers.

The result of this phase is a grouping of files by server. A separate endpoint is created for each group.

## Endpoint and Location

`Location` is the network address of a Flight server. It tells the client which node should be contacted to read a specific part of the result.

`FlightEndpoint` contains a `Location` and a `Ticket`. Conceptually, an endpoint tells the client to use this ticket with this node.

For every endpoint, the server creates a separate handle. The SQL query and the list of files assigned to that endpoint are stored in `statementCache` under that handle.

## Reading by Ticket

When the client calls `DoGet`, the server enters `FlightSqlProducer.getStreamStatement`.

The server extracts the handle from the ticket and uses it to load the SQL query and assigned file list from `statementCache`.

The server then calls `ExecutionService.readParquet`, passing the allocator, SQL query, file list, listener, and stream-start flag.

Actual query execution starts at this point.

## Query Engine Selection

The project uses three execution paths for Parquet reads. The route is selected in `ExecutionService.readParquet` after `ParquetQueryParser` has extracted projection, filter, aggregation, and group-by information.

Routing rules:

1. Metadata-only aggregates use Java and Parquet footers.
2. Full scans and projection-only scans use Arrow Dataset / Acero.
3. Filtered scans, general aggregations, and joins use DuckDB.

This follows ADR 0001: Acero is kept for the query shapes where it is fastest, DuckDB is used when SQL execution or predicate pushdown is needed, and Java handles the cases that can be answered without reading data pages.

## Java Footer Path

The Java footer path is used for simple aggregate queries without `WHERE` and without `GROUP BY`.

Supported fast-path expressions are:

- `COUNT(*)`
- `COUNT(col)`, when Parquet null-count statistics are complete
- `MIN(col)`, when Parquet min statistics are complete
- `MAX(col)`, when Parquet max statistics are complete

For `COUNT(*)`, Java sums row counts from Parquet row-group metadata. For `COUNT(col)`, Java subtracts null counts from row counts. For `MIN` and `MAX`, Java merges per-row-group statistics.

This path does not read column data pages and does not start Acero or DuckDB for the query. If required statistics are missing or incomplete, execution falls back to DuckDB.

## Acero Path

Full scans and projection-only scans are executed by Arrow Dataset / Acero.

Examples:

- `SELECT * FROM schema.table`
- `SELECT col1, col2 FROM schema.table`

The method resolves assigned relative file paths into absolute URIs and creates one Arrow Dataset scanner for the files assigned to the current ticket. Projection passes the required column list to the scanner. If no projection is specified, all columns are scanned.

Reading is performed by Arrow Dataset / Acero. It opens Parquet files, reads the required columns, and returns Arrow batches.

The Java code does not manually convert Parquet values into Arrow vectors. This conversion happens inside Arrow Dataset / Acero. On the Java side, the result is exposed as `ArrowReader` and `VectorSchemaRoot`.

Each loaded batch is sent to the client through the Flight listener. Before reading and sending, the server checks backpressure so it does not buffer unnecessary data when the client is not ready for the next batch.

## DuckDB Path

DuckDB is used for query shapes that need SQL execution beyond a simple scan:

- filtered scans (`WHERE ...`)
- filtered projections
- `GROUP BY`
- `SUM`
- aggregates that cannot use footer statistics
- joins

For single-table queries, `ExecutionService` builds SQL over DuckDB's `read_parquet([...])` table function. For joins, it creates temporary DuckDB views for each table alias, each view backed by `read_parquet([...])`, and then executes the rewritten join SQL against those views.

DuckDB returns results through `DuckDBResultSet.arrowExportStream`. The server copies rows into a separate Flight-owned root before `putNext()`, so reuse of DuckDB's buffers cannot corrupt an in-flight batch.

When DuckDB reads local files, no extension is required. For HDFS, `ExecutionService` preserves the qualified `hdfs://authority/path` URI and DuckDB opens it through the configured HDFS extension. The Docker image installs and verifies `duckdb-hdfs` automatically.

Relevant settings include:

- `duckDbHdfsExtension` or `DUCKDB_HDFS_EXTENSION`
- `duckDbAllowUnsignedExtensions` or `DUCKDB_ALLOW_UNSIGNED_EXTENSIONS`
- `duckDbHdfsDefaultNamenode` or `HDFS_DEFAULT_NAMENODE`
- `duckDbHdfsHaNamenodes` or `HDFS_HA_NAMENODES`
- `duckDbHdfsShortcircuit` or `HDFS_SHORTCIRCUIT`
- `duckDbHdfsDomainSocketPath` or `HDFS_DOMAIN_SOCKET_PATH`

## Runtime Tuning

The default runtime configuration lives in `src/main/resources/arrowflight.properties`.

The most important shared value is `batchSize`. It controls both Acero scan batch size and DuckDB Arrow export batch size, so changing it affects the size of batches sent through Flight.

I/O parallelism is also configurable. If `ioParallelism` is set, that exact thread count is used. Otherwise the value is derived from available CPU cores:

`max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)`

Use `ioParallelismMaxCores=0` to mean "no core cap". JVM system properties such as `-Darrowflight.io.parallelism=64` can override the file configuration.

## Sending the Result to the Client

A regular `SELECT` usually sends multiple Arrow batches. Aggregations often send a single final batch, although the result size depends on the query and the number of groups.

Sending is performed through `ServerStreamListener`. `VectorSchemaRoot` contains the current Arrow batch, and `putNext` sends it to the client.

After reading is complete, the server completes the stream.

## Current Execution Limitations

The current implementation distributes work at the whole-file level. A single large file is not split between multiple Flight nodes.

Round-robin distribution balances the number of files but does not account for data volume. For more accurate benchmark/production scenarios, file sizes or row groups should be considered.

Ticket is a reference to query state in Hazelcast, not a standalone execution plan. If the cache entry expires or is unavailable, reading by ticket is impossible.

Actual query execution starts only during `DoGet`. `GetFlightInfo` is responsible for planning, schema construction, endpoints, and tickets.
