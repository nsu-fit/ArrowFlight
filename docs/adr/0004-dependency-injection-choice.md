# ADR 0003: Dependency Injection with Dagger

Status: Accepted
Date: 2026-07-13

## Decision Record

- Timestamp: 2026-07-13T12:00:00+07:00
- User: vladimir
- Question: Which dependency injection approach should the Flight SQL server use,
  given that it needs testability, explicit wiring, and minimal runtime overhead
  for Arrow-native memory?
- Decision: Use Dagger for compile-time dependency injection.
- Rationale: Dagger provides compile-time safety, lightweight runtime (~0.5 MB),
  fast startup (~5 ms), and no reflection — critical for the Arrow native memory
  layer. Spring Boot would be overkill (50 MB, classpath scanning, runtime
  proxies). Guice relies on reflection at runtime, adding startup overhead and
  potential issues with Arrow's off-heap memory.

## Context

The project originally had no DI framework. All dependencies were created with
`new` inside `HadoopArrowFlightServer.start()` and duplicated across tests.

This caused several problems:

- **Impossible to unit-test** — classes like `ParquetManager` (later split into
  `ExecutionService`, `MetadataService`, `ParquetAdapter`) were hard-coded in
  the main entry point. Tests could not mock individual components.
- **Implicit dependency graph** — understanding who depends on whom required
  reading the entire `start()` method and spotting `new` calls.
- **Wiring duplication** — every test copied 30–50 lines of manual wiring
  (`ParquetManager`, `HazelcastInstance`, `RootAllocator`, `FlightSqlProducer`,
  etc.).
- **No singleton guarantees** — nothing prevented creating two
  `BufferAllocator` instances accidentally.

During the `restructure2` refactoring Dagger was introduced to solve these
issues.

## Comparison: Dagger vs Manual DI

| Criterion | Manual (main) | Dagger |
| :--- | :--- | :--- |
| Compile-time safety | ❌ (null by oversight) | ✅ |
| Startup overhead | 0 ms | ~5 ms (code generation) |
| Library size | 0 MB | ~0.5 MB |
| Testability | ❌ (hard-coded wiring) | ⚠️ (manual `TestFlightServerHelper`) |
| Singleton guarantees | ❌ | ✅ (`@Singleton`) |
| Explicit dependency graph | ❌ | ✅ (`ServerModule`) |

**Score: 9/10** — a significant improvement over no DI. Wiring is now explicit
in `ServerModule`, singletons are guaranteed by `@Singleton`, and the dependency
graph is visible at a glance.

## Comparison: Dagger vs Spring Boot

| Criterion | Spring Boot | Dagger |
| :--- | :--- | :--- |
| Compile-time safety | ❌ (runtime only) | ✅ |
| Startup overhead | ~10 s (classpath scan) | ~5 ms |
| Library size | ~50 MB | ~0.5 MB |
| AOP / metrics | ✅ (Actuator, Micrometer) | ❌ |
| Learning curve | High | Medium |
| Fit for this project | ⚠️ (overkill) | ✅ |

**Score: 7.5/10** — Spring Boot would be overkill. This project has no web
layer, no transactions, no JPA, no scheduling. Dagger covers 100% of the DI
needs with 1% of the weight.

## Comparison: Dagger vs Guice

| Criterion | Guice | Dagger |
| :--- | :--- | :--- |
| Compile-time safety | ❌ (runtime reflection) | ✅ |
| Startup overhead | ~500 ms | ~5 ms |
| Library size | ~2 MB | ~0.5 MB |
| AOP | ⚠️ (AOP Alliance) | ❌ |
| Testability | ✅ (`Modules.override`) | ⚠️ (manual) |
| Fit for this project | ⚠️ (runtime overhead) | ✅ |

**Score: 7.5/10** — Guice is a runtime framework using reflection and dynamic
proxies. For a high-performance Arrow server with native memory, reflection is
undesirable. Dagger generates code at compile time with zero runtime proxy
overhead.

## Decision

- Use **Dagger** as the DI framework.
- All singleton services are wired in `ServerModule` using `@Provides` and
  `@Singleton`.
- The generated `DaggerServerComponent` is built in
  `HadoopArrowFlightServer.start()`.
- Tests use `TestFlightServerHelper` with manual wiring (no Dagger in tests).

Dagger is the optimal compromise: compile-time safety without the bloat of
Spring Boot or the runtime overhead of Guice.

## Consequences

- **Positive**: Dependency graph is explicit in `ServerModule`. No more hunting
  for `new` calls across the codebase.
- **Positive**: `@Singleton` guarantees single instances of `BufferAllocator`,
  `HazelcastAdapter`, `FileSystem`, and other heavy objects.
- **Positive**: Errors in the dependency graph (missing providers, circular
  dependencies) are caught at compile time.
- **Positive**: Lightweight runtime (~0.5 MB) and fast startup (~5 ms).
- **Negative**: Tests still require manual wiring in `TestFlightServerHelper`.
  No `@MockBean` or `Modules.override`.
- **Negative**: No AOP, no lifecycle callbacks, no metrics. These are not needed
  yet.
- **Future**: If the project grows HTTP endpoints, health checks, scheduled
  tasks, or metrics — Spring Boot becomes a viable alternative. Until then,
  Dagger is the correct choice.
