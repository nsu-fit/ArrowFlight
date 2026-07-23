# Parquet File Storage and Locality

This document describes how the project works with files through Hadoop FileSystem, how block location information is used, and what limitations exist in the current work distribution model.

## Storage Model

Data is not stored inside the Arrow Flight server. Flight nodes act as the compute layer: they receive assigned Parquet file paths and read them through Hadoop `FileSystem`.

The data root is configured by the `dataDirectory` parameter. Tables are expected under a `schema/table` hierarchy. For each table, the server recursively searches for Parquet files.

`ParquetAdapter` owns the main file and path handling logic.

## Hadoop FileSystem

Hadoop FileSystem is used for discovery, metadata, locality, and data-page reads. Java decodes Parquet into bounded `VectorSchemaRoot` batches and exposes them to DuckDB through Arrow C Data.

A Flight node does not need a local Parquet copy. Hadoop opens assigned files directly; neither HDFS data nor complete files are materialized in `/tmp`.

When the server starts, it creates a `FileSystem` instance associated with `dataDirectory`. All file operations then go through that object: listing files, reading metadata, resolving absolute paths, and opening files.

## HDFS and Block Locality

In HDFS, a file is split into blocks. Blocks may be placed on different DataNodes and are usually replicated. As a result, Hadoop can return a list of hosts that store blocks for a given Parquet file.

The project uses this information for data locality. If a Flight server runs on the same host where file blocks are stored, reading that file from this server is preferred. This reduces network traffic and usually improves scan performance.

`ParquetAdapter.fileLocality` is responsible for extracting locality information. It collects hosts from the block locations returned by Hadoop.

## File Discovery for a Query

When the server needs to determine which files belong to a query, it uses `ParquetAdapter.locationsForQuery`.

This method:

1. Parses SQL and determines schema/table.
2. Builds the table path under `dataDirectory`.
3. Recursively lists Parquet files.
4. Collects block hosts for each file.
5. Returns a mapping between a relative file path and the set of hosts that store its blocks.

This information does not mean that a file is already assigned to a specific Flight node. It only shows which nodes are potentially closer to the data.

## Parquet as a Storage Format

Parquet is a columnar format. This matters for analytical queries because the server can read only the required columns instead of reading full rows.

A Parquet file also contains a footer with metadata. The footer may include schema information, row counts, row group statistics, and other metadata. The project uses this metadata for several fast-path optimizations.

The current implementation distributes work at the whole-file level. Row groups are part of the internal Parquet structure, but they are not currently used as the distribution unit between Flight nodes.

## Projection

Projection means selecting only the columns required by a query. For a regular `SELECT`, the query parser extracts the selected columns and includes them in the DuckDB query.

The Java reader pushes safe top-level column projections into Parquet and decodes selected columns directly into Arrow vectors. DuckDB applies the final SQL projection to the registered stream.

## Filter Pushdown

Spark V2 predicates and SQL filters are translated into the SQL sent to DuckDB. DuckDB applies them to the registered Arrow stream. Parquet predicate pushdown is not yet implemented in the Java reader.

## Footer Fast Path

Some aggregations can be executed without reading column data.

For total row counts, the project can use row count information from the Parquet footer. For some minimum, maximum, and non-null count operations, the project attempts to use Parquet statistics.

If the required statistics are available and the query does not require filtering or grouping, the server reads only metadata. If the statistics are not sufficient, execution falls back to the regular aggregation path.

The regular aggregation path is executed by DuckDB and partial results from Flight nodes are merged by the client when required.

## File Distribution Across Flight Nodes

File distribution is performed in `FlightSqlProducer.determineEndpoints`. For each Parquet file, the server chooses the Flight server that will read it.

Server selection is handled by `pickServer`.

Selection logic:

1. If any Flight servers match the hosts that store the file blocks, only those local servers are considered (data locality priority).
2. Among candidate servers, the one with the smallest cumulative assigned data volume (sum of file sizes) is selected.

After a server is selected, the file is added to the list of files assigned to that node and the server's cumulative load is incremented by the file size. Then a separate endpoint and ticket are created for each node.

## Current Distribution Limitations

The greedy load-aware strategy distributes data volume (file size). However, it operates at the whole-file level and does not split files across nodes. Extremely large single files still pin all their data to one node.
