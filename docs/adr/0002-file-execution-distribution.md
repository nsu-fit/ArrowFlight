# ADR 0002: File execution distribution across nodes

Status: Accepted
Date: 2026-07-07

## Decision Record

- Timestamp: 2026-07-07T10:30:00+07:00
- User: s.samokhin
- Question: How should the Flight SQL cluster distribute Parquet file execution
  across server nodes?
- Decision: Use a file-size-aware scheduler as the first implementation. Each
  node receives a set of files whose total cached size is approximately
  balanced. File size metadata is cached so planning does not repeatedly hit
  the filesystem.
- Rationale: File count alone is not a good proxy for work. A single large
  Parquet file can dominate execution time while many small files may be cheap.
  Size-based assignment is simple, deterministic, and significantly fairer than
  round-robin by file count. Dynamic node-load scheduling is planned as a later
  improvement.

## Context

The server can run as multiple Arrow Flight SQL nodes over a shared Parquet
dataset. A query may touch many Parquet files, and those files need to be
assigned to the available nodes before execution starts.

The scheduler has two immediate goals:

- keep work reasonably balanced between nodes;
- avoid expensive filesystem metadata reads on every query.

At the moment, the most stable input signal is Parquet file size. It is cheap
to reason about, works before query execution starts, and is available for all
files. It is not perfect: file size does not capture row-group pruning,
predicate selectivity, CPU pressure, memory pressure, or current node load.

## Decision

- For each table, collect the Parquet files that participate in the query.
- Cache file metadata, especially file size, so repeated planning does not need
  to call the filesystem for the same paths.
- Assign files to nodes using a size-aware balancing strategy:
  1. sort files by size descending;
  2. repeatedly place the next largest file onto the node with the smallest
     currently assigned total size;
  3. execute each node's assigned file subset locally where possible.
- The assignment should be deterministic for the same file set and node set.
- The file-size cache must be refreshable or invalidated when table contents
  change.

This is a scheduling heuristic, not a final distributed execution model.

## Planned Evolution

The next scheduler version should incorporate dynamic runtime signals:

- current node load;
- active query count per node;
- bytes already assigned or currently being scanned;
- observed read throughput;
- memory pressure;
- data locality when it is available from the filesystem.

The long-term goal is a hybrid planner: use cached file size as the baseline
estimate, then adjust placement using live node load and recent execution
metrics.

## Alternatives Considered

### Option A: Round-robin by file count

Assign files to nodes one by one without looking at file size.

- **Pros**: Very simple and deterministic.
- **Cons**: Poor balance when file sizes differ. One node can receive a few
  very large files while another receives many tiny files.

### Option B: Size-aware static assignment (selected)

Use cached file sizes and greedily balance total bytes assigned per node.

- **Pros**: Simple, deterministic, cheap to compute, and much fairer than file
  count when file sizes vary.
- **Cons**: Still static. It does not know that one node may already be busy or
  that a filtered query may skip most row groups in a large file.

### Option C: Dynamic load-aware assignment

Assign work using live node metrics such as CPU load, memory pressure, active
queries, and current scan throughput.

- **Pros**: Potentially best performance under concurrent workload and skew.
- **Cons**: More moving parts. Requires reliable node metrics, periodic updates,
  failure handling, and careful testing to avoid unstable scheduling decisions.

## Consequences

- **Positive**: Query planning becomes fairer than file-count distribution.
- **Positive**: Cached file metadata reduces planning overhead for repeated
  queries.
- **Positive**: The design leaves room for dynamic load-aware scheduling later.
- **Negative**: Size is an estimate, not exact work. Filters, row-group pruning,
  compression, and column projection can change actual runtime cost.
- **Negative**: The metadata cache needs a refresh/invalidation strategy when
  files are added, removed, or rewritten.
- **Future**: Add node-load metrics and use them together with cached file size
  to make assignment adaptive instead of purely static.
