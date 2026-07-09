# Hadoop Arrow Flight SQL Server Overview

[![Java tests and JaCoCo coverage](https://github.com/nsu-fit/ArrowFlight/actions/workflows/coverage.yml/badge.svg)](https://github.com/nsu-fit/ArrowFlight/actions/workflows/coverage.yml)
[![JaCoCo Coverage Report](https://img.shields.io/badge/JaCoCo-Coverage_Report-brightgreen)](https://nsu-fit.github.io/ArrowFlight/jacoco/)

Provides high-performance Parquet access via **Arrow Flight SQL**. Enables analytical queries with distributed processing, data locality, and optimizations (fast-path, C++ filtering, DuckDB for aggregations and in-cluster joins). Runs in cluster mode coordinated by Hazelcast.

---

## Helper Scripts

- **`run.sh`** — Safely fetches, fast-forwards, builds, and runs selected tests. Destructive reset requires `--force-reset` and dirty-worktree confirmation.
- **`docker/bin/entrypoint.sh`** — Docker container entry point. Supports 6 modes: `server`, `spark-master`, `spark-worker`, `generate`, `flight-count`, and custom pass-through.
- **`docker/spark/generate_and_distribute.py`** — PySpark script that generates test Parquet datasets with configurable row count, partitions, schema, and table, then distributes files across server data directories.
- **`docker/spark/query_flight.py`** — PySpark script that executes Flight SQL queries via the `flight` DataSource V2 connector.

---

## Server Parameters

When launching the server (via `java -jar`), use the following arguments:

| Parameter | Description | Example |
| :--- | :--- | :--- |
| `--data-dir` | Directory containing Parquet files. | `/mnt/data/parquet` |
| `--port` | Flight SQL server port. | `32010` |
| `--hosts` | Comma-separated list of Hazelcast cluster node IPs. | `192.168.1.10,192.168.1.11` |
| `--localhost` | Local IP address to bind the server to. | `192.168.1.10` |
| `--hazelcast-port` | Port for Hazelcast cluster communication. | `5701` |

*(Note: The server waits up to `hazelcastClusterJoinTimeoutSec` seconds (default 60) for all `--hosts` nodes to join the cluster, then fails with a clear error if not all are connected. Single-node mode (`--hosts` with one host) skips the wait entirely.)*

---

## Configuration Properties

The server reads configuration from a three-tier system: **JVM system property** → **`arrowflight.properties`** → **built-in default**. DuckDB HDFS properties additionally support environment variables.

| Property (system key) | Alias | Default | Description |
| :--- | :--- | :--- | :--- |
| **DuckDB** | | | |
| `batchSize` | `arrowflight.duckdb.batchSize` | `4096` | Arrow batch size for Acero and DuckDB |
| `duckDbThreads` | `arrowflight.duckdb.threads` | `1` | Threads per DuckDB connection |
| `duckDbGroups` | `arrowflight.duckdb.groups` | `min(8, ioParallelism)` | DuckDB file groups |
| `duckDbWarmConnections` | `arrowflight.duckdb.warmConnections` | `min(8, ioParallelism)` | Pre-warmed DuckDB connections |
| **DuckDB HDFS** | | | |
| `duckDbHdfsExtension` | `arrowflight.duckdb.hdfs.extension` | — | Path to HDFS extension (also `DUCKDB_HDFS_EXTENSION` env) |
| `duckDbAllowUnsignedExtensions` | `arrowflight.duckdb.allowUnsignedExtensions` | `false` | Allow unsigned DuckDB extensions |
| `duckDbHdfsDefaultNamenode` | `arrowflight.duckdb.hdfs.defaultNamenode` | — | HDFS namenode (also `HDFS_DEFAULT_NAMENODE` env) |
| `duckDbHdfsHaNamenodes` | `arrowflight.duckdb.hdfs.haNamenodes` | — | HA namenodes (also `HDFS_HA_NAMENODES` env) |
| `duckDbHdfsShortcircuit` | `arrowflight.duckdb.hdfs.shortcircuit` | — | HDFS short-circuit read (also `HDFS_SHORTCIRCUIT` env) |
| `duckDbHdfsDomainSocketPath` | `arrowflight.duckdb.hdfs.domainSocketPath` | — | Domain socket path (also `HDFS_DOMAIN_SOCKET_PATH` env) |
| **I/O** | | | |
| `ioParallelism` | `arrowflight.io.parallelism` | auto | Explicit thread count. Default: `max(32, min(cores, maxCores) * 8)` |
| `ioParallelismMinThreads` | `arrowflight.io.minParallelism` | `32` | Minimum I/O threads |
| `ioParallelismMaxCores` | `arrowflight.io.maxCores` | `0` | Maximum cores (0 = all available) |
| `ioParallelismMultiplier` | `arrowflight.io.parallelismMultiplier` | `8` | Threads per core |
| `ioFileBufferSize` | — | `131072` | Hadoop client I/O buffer size |
| **gRPC / Flight** | | | |
| `grpcMaxInboundMessageSize` | `arrowflight.grpc.maxInboundMessageSize` | `Integer.MAX_VALUE` | Max gRPC inbound message size |
| `flightListenerReadyTimeoutMs` | `arrowflight.flight.listenerReadyTimeoutMs` | `60000` | Timeout for Flight listener ready |
| **Client** | | | |
| `client.maxRetries` | — | `3` | Client retry count |
| `client.retryBackoffMs` | — | `1000` | Backoff between retries (ms) |
| `client.connectTimeoutMs` | — | `30000` | Connection timeout (ms) |
| **Hazelcast** | | | |
| `hazelcastClusterJoinTimeoutSec` | `arrowflight.hazelcast.clusterJoinTimeoutSec` | `60` | Cluster join timeout (seconds) |

---

## Docker

The project includes a full Docker Compose orchestration (`docker-compose.yml`) with 8 services:

| Service | Description |
| :--- | :--- |
| `spark-master` | Spark master node (ports 7077, 8080) |
| `spark-worker-1` | Spark worker 1 (port 8081, cores=2, mem=2g) |
| `spark-worker-2` | Spark worker 2 (port 8082, cores=2, mem=2g) |
| `data-generator` | Generates Parquet test data and distributes across 3 server volumes |
| `flight-server-1` | Flight SQL node 1 (port 32010, healthcheck) |
| `flight-server-2` | Flight SQL node 2 (port 32011 → 32010) |
| `flight-server-3` | Flight SQL node 3 (port 32012 → 32010) |
| `spark-client` | Profiled service (`--profile test`), runs `query_flight.py` |

**Dockerfile**: Multi-stage build — `maven:3.9.9-eclipse-temurin-21` for compilation, `eclipse-temurin:21-jre-jammy` for runtime with Spark 3.5.1 bundled.

```bash
# Start the full cluster
docker compose up -d

# Run a test query via Spark
docker compose --profile test up spark-client
```

---

## Spark Integration

The project includes a full **Spark DataSource V2 connector** registered as the `flight` format:

```scala
val df = spark.read
  .format("flight")
  .option("host", "localhost")
  .option("port", "32010")
  .option("user", "test-user")
  .option("bearerToken", "test-token")
  .option("table", "SELECT * FROM test_schema.test_table")
  .load()
```

**Reader options**:

| Option | Description |
| :--- | :--- |
| `host`, `port` | Flight server address |
| `tls.enabled`, `tls.verifyServer`, `tls.truststore.jksFile`, `tls.truststore.pass` | TLS configuration |
| `user`, `password` | Basic authentication |
| `bearerToken` | Bearer token authentication |
| `table` | Table name or SQL query to execute |
| `column.quote` | Column name quoting character (default `"`) |
| `default.schema` | Default schema |
| `routing.tag`, `routing.queue` | Workload management routing |
| `partition.size`, `partition.hashFunc`, `partition.byColumn`, `partition.lowerBound` | Parallel read partitioning |

Key implementation classes: `FlightSource` (entry point), `FlightTable` (table representation), `FlightScan` / `FlightScanBuilder` (scan planning), `FlightPartitionReader` (per-partition reader).

---

## Client API

The low-level Java client (`Client.java`) provides:

- **`getQueryEndpoints(query)`** — Get Flight endpoints for a SQL query
- **`fetch(query)`** — Execute and materialize results into `RowSet`
- **`fetchStreaming(query, callback)`** — Stream results via `BatchCallback`
- **`execute(sql)`** / **`executeUpdate(sql)`** — Arbitrary statement execution
- **`getPreparedStatement(sql)`** — Prepared statement support

Features:
- Exponential backoff retry (configurable `maxRetries` / `retryBackoffMs`)
- Connection pooling (singleton per connection string)
- Default 2 GB Arrow memory allocation (`allocationLimit`)

**`Configuration.java`** supports:
- TLS with server certificate verification and truststore (JKS)
- Authentication: BasicAuth (`user` + `password`) or Bearer token (`bearerToken`)
- Workload management: `routingTag`, `routingQueue` (sent as Flight call headers `ROUTING_TAG`, `ROUTING_QUEUE`)

---

## Build and Test

- **Java 21** required
- **Maven profiles**: `server` (builds only `server/**`), `client` (builds only `client/**`)
- **Fat JAR**: Produced via `maven-shade-plugin`, main class `HadoopArrowFlightServer`
- **Code quality**: Checkstyle (`checkstyle.xml`), SpotBugs (`spotbugs-exclude.xml`)

```bash
# Unit tests only
mvn test

# All tests including integration
mvn test -DexcludedGroups=""

# Performance tests
mvn test -Dgroups=perf

# JaCoCo coverage report
mvn verify
```

**JVM flags** (required for Java 21 + Hadoop + Netty):
```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```

---

## Component Details

- **`HadoopArrowFlightServer`**: Entry point. Parses CLI arguments, initialises Hazelcast cluster, Hadoop FS, ParquetManager, and starts the Arrow Flight SQL server.
- **Flight SQL Server Adapter** (`HadoopFlightSqlService`): Handles Arrow Flight requests (`GetFlightInfo`, `DoGet`). Parses SQL via `ParquetQueryParser`, distributes files across nodes (via Hazelcast), and generates client `Ticket`s.
- **Query Parser** (`ParquetQueryParser`): Uses jOOQ to extract schema, table, columns, WHERE, JOINs, aggregations, and GROUP BY. Rewrites join queries for DuckDB execution.
- **Join Execution**:
  - *Server-side*: When all join files reside on a single server, DuckDB executes the join via native SQL.
  - *Spark fallback*: When join files span multiple servers, the query is split into per-table `SELECT *` reads. The Spark DataSource V2 connector receives each table separately and performs the join Spark-side.
- **Coordinator (Hazelcast)**: Provides distributed query context cache (`statementCache`), node registry (`serverRegistry`), server heartbeats, and locks for atomic file acquisition.
- **Parquet Execution Adapter** (`ParquetManager`): Core execution engine. Dispatches queries:
  - *Non-aggregations*: Scans via Acero with Substrait filter (if WHERE exists).
  - *Aggregations*: Uses fast-path (footer read) or passes file groups to DuckDB via Acero (Arrow C Stream).
  - *Joins*: Delegates to DuckDB for server-side join execution.
- **Schema Converter** (`ParquetSchemaConverter`): Maps Parquet types to Arrow types, handling legacy `OriginalType` annotations (INT_8, INT_16, etc.) to prevent `ClassCastException` during Spark whole-stage codegen.
- **Acero**: C++ Arrow Dataset engine for projection, filtering, and partial aggregations (data source for DuckDB).
- **DuckDB**: Executes full aggregations (`GROUP BY`, `SUM`, `COUNT`, etc.) over file groups via Arrow C Stream.
- **Spark DataSource V2 Connector**: Implements Spark's `TableProvider` + `DataSourceRegister`. Full read and write paths with partition planning and batch conversion.
- **Hadoop FS**: Abstraction for Parquet file access (HDFS, S3, local FS).

---

## Execution Flow

1. **Client** sends SQL via `GetFlightInfo`.
2. **Flight Adapter** parses the query, extracts the schema, saves it to Hazelcast, and calls `determineEndpoints` to distribute files considering locality.
3. Returns `FlightInfo` with endpoints (each containing a `Ticket` and node address).
4. **Client** calls `DoGet` for each endpoint (passing the Ticket).
5. On each node, **Flight Adapter** restores the query and file list from the Ticket, initiating a two-phase file acquisition via Hazelcast locks.
6. **Parquet Adapter** executes via Acero or DuckDB (with fast-path if applicable) and streams results as `VectorSchemaRoot`.
7. **Client** receives and processes data.

---

## Supported Features

- `SELECT` with projection, filtering (`WHERE`), aggregations (`COUNT`, `SUM`, `MIN`, `MAX`, `COUNT(DISTINCT)`, `GROUP BY`).
- `INNER JOIN` with cross-type join conditions and automatic type coercion (e.g. INT32 = INT64, FLOAT = DOUBLE, BOOL = INT8).
- Info commands (schemas, tables, types, server properties).
- Distributed processing, data locality, parallelism.
- Optimizations: fast-path (footer read), C++ filtering (Substrait), DuckDB for aggregations and server-side joins.

---

## Limitations

- `SELECT` only (read-only).
- No `ORDER BY`, `LIMIT`, `OFFSET`, subqueries, or window functions.
- `GROUP BY` aggregations require client-side partial result merging.

---

## CI / CD

| Pipeline | Trigger | Description |
| :--- | :--- | :--- |
| **GitHub Actions `ci.yml`** | PR to `main` | Builds server and client separately (Maven profiles), runs unit tests, uploads Surefire reports |
| **GitHub Actions `coverage.yml`** | PR to `main` + `workflow_dispatch` | Runs `mvn verify` with JaCoCo, deploys coverage report to GitHub Pages |
| **GitLab CI** | MR to `main` | Two-stage pipeline (build → test), separate server/client builds, JUnit report upload |

---

## Documentation

- **[ADR](docs/adr/)** — Architecture Decision Records (query engine selection, file distribution scheduler)
- **[Architecture](docs/architecture/)** — Parquet storage model, SQL query execution flow
- **[User Guides](docs/user_guides/)** — Build and test instructions, `run.sh` usage, DuckDB HDFS extension setup
