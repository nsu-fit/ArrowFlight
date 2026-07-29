# Cluster Service

This document describes how `ClusterService` coordinates multiple Flight server nodes: registration, heartbeat, file inventory, load tracking, and statement cache lifecycle.

## Overview

`ClusterService` runs on every Flight server node. It uses Hazelcast distributed maps to share state across the cluster. Each node registers itself, publishes its file inventory, and maintains a heartbeat. The service is used during query planning to discover live nodes, locate files, and balance load.

## Components

`ClusterService` wraps a `HazelcastAdapter` instance. Hazelcast provides four distributed maps used by the service:

| Map              | Key                    | Value                | Purpose                                 |
|------------------|------------------------|----------------------|-----------------------------------------|
| `serverRegistry` | `flight-server-N:32010`| `Long` (load bytes)  | Node registration + current load        |
| `serverHeartbeats`| `flight-server-N:32010`| `Long` (timestamp)   | Heartbeat timestamps for liveness check |
| `serverFiles`    | `flight-server-N:32010`| `Map<String, Long>`  | Per-node file inventory (path -> bytes) |
| `statementCache` | handle (UUID string)   | `HandleState`        | Query state, TTL 10 minutes             |
| `node-load-snapshots-v1` | `flight-server-N:32010` | `NodeLoadSnapshot` | Live admission, CPU, memory, and throughput state |

## Server Registration and Deregistration

### Startup

When a Flight server starts, its `ClusterService` constructor:

```java
hazelcast.serverRegistry().put(serverUri, 0L);
hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
```

The node writes its URI (e.g. `flight-server-1:32010`) into both maps with an initial load of 0.

### Shutdown

`ClusterService.close()` removes the node from both maps:

```java
hazelcast.serverRegistry().remove(serverUri);
hazelcast.serverHeartbeats().remove(serverUri);
```

This prevents stale entries when a node shuts down gracefully.

## Heartbeat Mechanism

After registration, a scheduled executor sends heartbeats every 15 seconds:

```java
heartbeatExecutor.scheduleAtFixedRate(() -> {
    hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
    hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
}, 15, 15, TimeUnit.SECONDS);
```

The executor is a single daemon thread named `flight-heartbeat`. Each heartbeat writes the current timestamp into `serverHeartbeats`.

### Liveness Check

Before planning a query, `FlightSqlProducer` calls `ClusterService.filterLiveServers()`. This method reads all heartbeats, filters out nodes whose last heartbeat is older than 45 seconds, and removes dead nodes from both maps:

```java
long deadline = now - 45_000; // 45 seconds without heartbeat = dead
for (String uri : serverUris) {
    Long lastHb = heartbeats.get(uri);
    if (lastHb == null) {
        // Newly registered node, no heartbeat yet — treat as alive
        live.add(uri);
    } else if (lastHb >= deadline) {
        live.add(uri);
    } else {
        // Stale — remove from cluster
        hazelcast.serverRegistry().remove(uri);
        hazelcast.serverHeartbeats().remove(uri);
    }
}
```

A node that has no heartbeat entry yet (registered but heartbeat hasn't fired) is treated as alive. This handles the short window between registration and the first heartbeat.

Time constants:

| Parameter            | Value   | Purpose                       |
|----------------------|---------|-------------------------------|
| `HEARTBEAT_INTERVAL_SEC` | 15s | How often each node writes its heartbeat |
| `HEARTBEAT_TIMEOUT_SEC`  | 45s | How long without heartbeat before a node is considered dead |

### Server Loads

Load is tracked as a single `Long` value per server in `serverRegistry`. Initially 0, it increases when a query handle is created (the `bytes` value from `HandleState`) and decreases when a handle expires or is explicitly removed.

`ClusterService.addLoad(uri, delta)` adds a signed delta to a server's load:

```java
hazelcast.serverRegistry().compute(uri, (k, v) -> {
    if (v == null) return delta;
    long updated = v + delta;
    return updated <= 0 ? 0L : updated;
});
```

Two methods expose load data:
- `allServerLoads()` — returns loads for all registered servers
- `getLoads(serverUris)` — returns loads for a specific set of servers

## File Inventory

After startup, each Flight server scans its data directory and calls `registerLocalFiles()`:

```java
hazelcast.serverFiles().put(this.serverUri, new LinkedHashMap<>(files));
```

Files are stored as relative paths with their byte sizes. This creates a distributed inventory across all nodes.

### File Location Resolution

When planning a query, `FlightSqlProducer` calls `fileLocations()` to build a complete picture of where every Parquet file lives:

```java
public Map<String, FileAssignment> fileLocations() {
    Set<String> inventoryServers = hazelcast.serverFiles().keySet();
    Map<String, Map<String, Long>> inventories = hazelcast.serverFiles().getAll(inventoryServers);
    Map<String, FileAssignment> result = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, Long>> server : inventories.entrySet()) {
        String serverUri = server.getKey();
        for (Map.Entry<String, Long> file : server.getValue().entrySet()) {
            result.compute(file.getKey(), (path, current) -> {
                Set<String> owners = new LinkedHashSet<>();
                long size = file.getValue();
                if (current != null) {
                    if (current.size() != size) {
                        throw new IllegalStateException(
                            "Conflicting sizes for replicated file " + path);
                    }
                    owners.addAll(current.hosts());
                }
                owners.add(serverUri);
                return new FileAssignment(size, owners);
            });
        }
    }
    return result;
}
```

The result is a map of `relative_path -> FileAssignment(size, [host1, host2, ...])`. If the same file appears on multiple nodes, it is treated as a replica — both hosts are preserved. `FileAssignment` throws if replicas report different sizes.

`hasFileInventory(serverUri)` checks whether a specific node has published its inventory.

## Signed Ticket and Load-Lease Lifecycle

### Ticket Creation

During `GetFlightInfo`, the server encodes endpoint state in an HMAC-SHA256
authenticated ticket. The signing secret is shared once through `statementCache`.
Every endpoint stores a load lease with a 10-minute TTL. This preserves cleanup
when a client never calls `DoGet`.

### Ticket Resolution

During `DoGet`, the target node verifies and decodes the ticket locally. Legacy
UUID handles are resolved from a local TTL cache first and Hazelcast second.

### Load-Lease Removal

When endpoint execution completes, removing the lease and decrementing load is
idempotent.

### Load-Lease Expiry

If a lease is not removed because the client disconnected or crashed, Hazelcast
expires it after 10 minutes. The existing `EntryExpiredListener` subtracts its
bytes from the owning server. If the server has already left the registry, the
listener exits without recreating it.

## Interaction with Query Execution

During `GetFlightInfo`:

1. `FlightSqlProducer` calls `filterLiveServers()` to get active nodes.
2. `ClusterService.fileLocations()` provides the distributed file inventory.
3. `QueryPlanner` reuses or refreshes the table file-plan cache.
4. The load-aware planner scores reservations, throughput, execution slots, CPU, memory, and locality.
5. Each endpoint receives a signed self-contained ticket.
6. Every assignment creates a 10-minute load lease.

During `DoGet`:

1. The ticket is verified and decoded locally.
2. The assigned files and SQL query are restored.
3. `ExecutionService` reads and streams the result.
4. The endpoint load lease is removed on completion.

If `DoGet` never arrives for an assignment, its lease expires after 10 minutes
and the load is corrected by the expiry listener.

## Adaptive Load Snapshots and Admission

Each node publishes a `NodeLoadSnapshot` every second. The snapshot contains
the current concurrency limit, active and queued queries, process CPU load,
total host or container CPU load, JVM, Arrow, system, or cgroup memory pressure, and
observed query throughput. Unrelated work on the same node therefore affects
admission and placement. Scheduling ignores stale or non-accepting snapshots.

For shared storage such as HDFS, every live Flight node is eligible to execute
a file. Block locality contributes a configurable score penalty instead of
acting as a hard constraint. The planner combines outstanding reserved bytes
with the snapshot to estimate completion cost.

Before `ExecutionService` starts DuckDB, `AdaptiveAdmissionController` obtains
a local execution permit. It maintains a bounded queue and adjusts its
concurrency limit with low/high CPU and memory watermarks. Existing executions
are never interrupted when the limit decreases.

## Queued DoGet Redirect

Before admission, an endpoint atomically changes its load lease from `RESERVED`
to `CLAIMED`. Reusing the same ticket cannot start a second execution.

When the local node is overloaded or a request waits longer than
`admissionRedirectAfterMs`, `TaskRedirectService` looks for a node with a
meaningfully lower score. A Hazelcast transaction removes the old lease,
creates a `RESERVED` lease for a new signed ticket, and transfers reserved
bytes between `serverRegistry` entries.

The server returns the replacement URI and ticket in Flight error metadata.
The client consumes that metadata before exposing the first Arrow batch and
automatically repeats only that `DoGet` on the replacement node.
`redirectCount` is signed into the ticket and `admissionMaxRedirects` prevents
ping-pong. Running DuckDB work is never migrated.

## Time Constants Summary

| Constant                        | Value  | Scope      | Purpose                                  |
|---------------------------------|--------|------------|------------------------------------------|
| `HEARTBEAT_INTERVAL_SEC`        | 15s    | Cluster    | Interval between heartbeat writes        |
| `HEARTBEAT_TIMEOUT_SEC`         | 45s    | Cluster    | Node considered dead after this silence  |
| load lease TTL                   | 10min  | Statement  | Max lifetime of a load reservation       |
| `admissionRedirectAfterMs`       | 500ms  | Admission  | Wait before redirect evaluation          |
| `flightListenerReadyTimeoutMs`  | 60s    | DuckDB     | Max wait for Flight client readiness     |
| `hazelcastClusterJoinTimeoutSec`| 60s    | Hazelcast  | Max wait for cluster formation           |
