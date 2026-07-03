# SQL Query Execution Flow

This document describes how an SQL query flows through the Arrow Flight server: from receiving the query to reading Parquet files, executing operations, and sending the result back to the client.

## Main Components

`HadoopArrowFlightServer` starts the server and configures Hadoop FileSystem, Hazelcast, and Arrow Flight.

`HadoopFlightSqlService` implements the Flight SQL layer. It receives queries, creates `FlightInfo`, builds `Ticket` objects, creates endpoints, and restores query state during `DoGet`.

`ParquetManager` handles Parquet-related work: file discovery, locality detection, reading through Arrow Dataset / Acero, fast-path aggregations, and delegating complex aggregations to DuckDB.

`ParquetQueryParser` parses SQL and extracts schema, table, selected columns, filters, aggregations, and group by columns.

`SubstraitFilterConverter` converts query filters into a Substrait representation that can be passed to Acero.

## High-Level Flow

A query goes through two main phases.

The first phase is planning. The client calls `GetFlightInfo`; the server determines the result schema, distributes files across Flight nodes, and returns endpoints with tickets.

The second phase is reading. The client calls `DoGet` using the tickets, and the corresponding Flight nodes read their assigned files and stream the result in Arrow format.

Data is not read during `GetFlightInfo`. Actual reading starts only during `DoGet`.

## Receiving the SQL Query

The SQL query enters the server through `HadoopFlightSqlService.getFlightInfoStatement`.

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

Before returning `FlightInfo`, the server must know the result schema. For that, `HadoopFlightSqlService` calls `ParquetManager.getQuerySchema`.

The result schema is built before reading data. For regular queries, it is derived from the Parquet table schema and the selected columns. For aggregations, it is derived from aggregation expressions and group by columns.

This schema is returned to the client as part of `FlightInfo`.

## File Distribution

File distribution is performed in `HadoopFlightSqlService.determineEndpoints`.

First, the server reads the SQL query from `statementCache`. Then `ParquetManager.locationsForQuery` determines which Parquet files belong to the query table and which Hadoop hosts store their blocks.

Next, the server retrieves all registered Flight servers from `serverRegistry`. Each Flight node registers itself in `serverRegistry` during startup.

For each file, the server calls `pickServer`. This method chooses the Flight server that will read the file.

If a Flight server runs on a host that stores the file blocks, that node is preferred. If no useful locality is available, the file is assigned using round-robin distribution across all Flight servers.

The result of this phase is a grouping of files by server. A separate endpoint is created for each group.

## Endpoint and Location

`Location` is the network address of a Flight server. It tells the client which node should be contacted to read a specific part of the result.

`FlightEndpoint` contains a `Location` and a `Ticket`. Conceptually, an endpoint tells the client to use this ticket with this node.

For every endpoint, the server creates a separate handle. The SQL query and the list of files assigned to that endpoint are stored in `statementCache` under that handle.

## Reading by Ticket

When the client calls `DoGet`, the server enters `HadoopFlightSqlService.getStreamStatement`.

The server extracts the handle from the ticket and uses it to load the SQL query and assigned file list from `statementCache`.

The server then calls `ParquetManager.readParquet`, passing the allocator, SQL query, file list, listener, and stream-start flag.

Actual query execution starts at this point.

## Regular SELECT

A regular query without aggregations is executed in `ParquetManager.readParquet`.

The method parses SQL and determines schema, table, projection, and filter. It then resolves assigned relative file paths into absolute URIs and creates an Arrow Dataset scanner.

Projection passes the required column list to the scanner. If a filter exists, it is converted to Substrait and also passed to the scanner.

Reading is performed by Arrow Dataset / Acero. It opens Parquet files, reads the required columns, applies the filter, and returns Arrow batches.

The Java code does not manually convert Parquet values into Arrow vectors. This conversion happens inside Arrow Dataset / Acero. On the Java side, the result is exposed as `ArrowReader` and `VectorSchemaRoot`.

Each loaded batch is sent to the client through the Flight listener. Before reading and sending, the server checks backpressure so it does not buffer unnecessary data when the client is not ready for the next batch.

## Aggregations

If the parser detects an aggregation, `readParquet` delegates execution to `executeAggregation`.

`executeAggregation` chooses the cheapest available execution path.

For a simple total row count without filtering or grouping, the server uses the Parquet footer. This allows it to produce the result without reading column data.

For some minimum, maximum, and non-null count operations, the server attempts to use Parquet statistics. If the statistics are available, column data is not read either.

If no fast path is available, execution falls back to `parallelAggregate`.

## Parallel Aggregate

`parallelAggregate` is used for more complex aggregations.

For row counts with a filter, if the filter can be expressed through Substrait, the Acero-only path is used. In this path, each file is scanned through Acero, the filter is applied by the scanner, and Java sums the row counts from the produced batches.

For more complex aggregations, DuckDB is used. In this path, Acero reads Parquet files and exports them as Arrow streams. DuckDB registers those streams as input tables and executes SQL aggregation on top of them.

DuckDB returns the result as an Arrow stream. The project then collects it into a `VectorSchemaRoot` and sends it to the client.

In this design, Acero is responsible for Parquet reading, projection, filtering, and Arrow conversion. DuckDB is used as the execution engine for complex SQL aggregation.

## Sending the Result to the Client

A regular `SELECT` usually sends multiple Arrow batches. Aggregations often send a single final batch, although the result size depends on the query and the number of groups.

Sending is performed through `ServerStreamListener`. `VectorSchemaRoot` contains the current Arrow batch, and `putNext` sends it to the client.

After reading is complete, the server completes the stream.

## Current Execution Limitations

The current implementation distributes work at the whole-file level. A single large file is not split between multiple Flight nodes.

Round-robin distribution balances the number of files but does not account for data volume. For more accurate benchmark/production scenarios, file sizes or row groups should be considered.

Ticket is a reference to query state in Hazelcast, not a standalone execution plan. If the cache entry expires or is unavailable, reading by ticket is impossible.

Actual query execution starts only during `DoGet`. `GetFlightInfo` is responsible for planning, schema construction, endpoints, and tickets.

